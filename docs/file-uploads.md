# 文件上传

前端在「DAG 编排」页选择一个本地文件并点击「上传文件」。成功后自动填写共享文件路径和 SHA256，继续配置算子参数与依赖，点击「提交任务」。每次任务仍对应一个文件，整个 DAG 仍由一个执行器完成。

前端只通过管理 API 传输文件；文件由管控节点写入共享 data-root，前端不挂载文件卷。管控和所有执行器必须使用同一持久化目录及相同绝对挂载路径。开发时先更新并重启管控服务，再刷新前端；仅更新前端会因旧后端缺少上传接口而失败。

## 配置

| 设置 | CLI | 环境变量 | Helm | 默认 |
| --- | --- | --- | --- | --- |
| 单文件最大字节数 | `--upload-max-bytes` | `DATAFLOW_UPLOAD_MAX_BYTES` | `control.uploadMaxBytes` | 268435456（256 MiB）；0 关闭上传 |
| 同时接收文件数 | `--upload-concurrency` | `DATAFLOW_UPLOAD_CONCURRENCY` | `control.uploadConcurrency` | 4；可配置 1..32 |
| 接收超时（秒） | `--upload-timeout` | `DATAFLOW_UPLOAD_TIMEOUT` | `control.uploadTimeout` | 300；可配置 1..86400 |

CLI 示例：

```bash
dataflow control --data-root /absolute/shared/data \
  --upload-max-bytes 1073741824 --upload-timeout 900
```

服务仍需要独立的管理/执行器令牌与原有数据库配置。Docker Compose 已把管控数据卷改为可写，可在 `.env` 配置以上环境变量，然后执行 `docker compose up --build -d`。宿主机共享目录需给容器 UID/GID 10001 写权限。

Kubernetes 已在启用上传时将管控 PVC 挂载为可写；设置 `control.uploadMaxBytes=0` 时恢复只读挂载。通过 `helm upgrade --install` 更新镜像和 Chart。独立 Nginx 对上传接口关闭请求体缓存，由管控检查限制；其他 JSON API 仍保留原限制。使用外部 Ingress/网关时，也需同步其上传体积和超时设置。

## API

- `GET /v1/files/limits`：管理鉴权，返回 `enabled`、`max_bytes`、`max_concurrent`、`timeout_seconds`。
- `POST /v1/files?filename=document.pdf`：管理鉴权，原始文件字节，`Content-Type: application/octet-stream`，必须有 `Content-Length` 和 `Idempotency-Key`。浏览器自动设置长度。文件名为不含路径分隔符或控制字符的 basename，最多 255 个 UTF-8 字节，保留原名和扩展名。
- `GET /v1/files`：管理鉴权，携带原 `Idempotency-Key`，查询已完成的上传；尚未完成返回 404。可用于上传响应丢失后的确认，不是文件列表接口。

```bash
curl -sS -X POST "http://127.0.0.1:8080/v1/files?filename=document.pdf" \
  -H "Authorization: Bearer $DATAFLOW_ADMIN_TOKEN" \
  -H "Idempotency-Key: upload-demo-001" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @./document.pdf
```

新文件返回 201，相同键、文件名、大小与字节的重传返回 200，并复用原文件。返回 `{id, filename, size_bytes, input_path, input_sha256, created_at}`。将 `input_path` 和 `input_sha256` 放入原 `POST /v1/tasks` 的 TaskSpec；上传不自动创建任务。每个新文件使用新键，重试原文件沿用原键。

错误：401 管理鉴权失败，403 来源未允许或上传关闭，409 同键上传仍在进行或键用于不同文件，411 缺少长度，413 超过大小限制，415 错误内容类型，429 上传槽位繁忙，408 接收超时，507 存储已满，503 存储/连接暂时不可用。

## 可靠性与性能

上传以不超过 1 MiB 的块写盘并计算 SHA256，不在管控内存中加载整个文件；同时上传默认最多 4 个，不持有任务数据库事务锁。默认最多 64 个 API 线程，上传准入之外的线程继续负责调度、心跳和观测。

输入与 metadata.json 在临时目录写完并刷盘，通过一次目录重命名发布到 `data-root/uploads/<id>/`，源文件位于 `source/<原文件名>`，设为只读。上传记录和源文件一起持久化在共享卷，没有文件与数据库双写的发布窗口；任务及其执行记录继续存 PostgreSQL。

断开、失败、接收超时会清理本次临时目录；管控重启清理遗留临时目录，保留完整上传和记录。上传从头重传，不提供断点续传。前端显示传输进度，100% 后继续等待保存确认；可取消上传，离开编排页或断开 API 也会取消。

取消与最终发布存在竞态：若服务已完成发布，文件会保留，可通过原键查询确认。前端上传响应丢失时先查询记录；失败后再次点击上传也沿用同一键。取消上传不取消已提交的任务。

选中文件但尚未完成上传时不能提交任务，避免误用上一次的文件路径。上传成功只更新路径和校验和，保留其他未提交的算子/DAG 编辑。选择新文件或切换到不同 API 时重新确认输入。导出/保存草稿只包含配置及文件引用，不包含文件字节或令牌。

完整上传不会自动删除，重试/多个任务可以继续引用。清理前确认没有排队、运行或需要重试的任务引用，输入卷与上传记录一并备份；自动保留策略和删除 API 尚未提供。上传接受任意文件字节，实际格式支持由执行器算子决定，管控不解析或执行上传文件。

## 验证

```bash
pytest -q tests/test_uploads.py tests/test_console_api.py
cd frontend
npm run test:dev
DATAFLOW_TEST_PYTHON=python npm test
```

验证包括超过 JSON 预算的二进制文件、中文文件名、越界路径拒绝、管理鉴权、长度/大小限制、并发准入、断开/超时/磁盘满清理、幂等确认、管控重启、真实 free-threaded 执行器的文件 DAG，以及浏览器上传后回填、保留未提交编辑和丢失响应确认。K8S 验收使用上传文件完成 Pod 故障恢复与管控重启，并通过生产 Nginx 上传超过 2 MiB 的文件并执行真实 DAG。
