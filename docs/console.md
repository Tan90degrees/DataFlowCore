# 独立前端管控台

前端位于 `frontend/`，是独立静态 HTML/CSS/ES modules 应用，没有 Python 页面模板和生产 npm 依赖。管控节点只提供 JSON API。前端、API、执行器各自构建和部署，前端更新不需要替换执行器镜像。

## 启动

已有 `.env`、共享 `data/` 目录时：

```bash
docker compose up --build -d
# 浏览器打开 http://127.0.0.1:8081
```

点击「配置 API 连接」，地址使用默认 `/api`，输入 `.env` 中的 `DATAFLOW_ADMIN_TOKEN`。独立 Nginx 将 `/api/*` 转发给 `control:8080`，仍由后端校验 Bearer token。令牌不写入静态文件、URL、localStorage 或 sessionStorage，刷新页面需重新输入。浏览器只保存 API 地址，以及用户主动保存的 DAG 草稿。

开发时使用 Node.js 20 或更高版本，无需 Python 静态服务器、打包或安装 npm 依赖。
先启动已有管控 API，再启动独立前端：

```bash
# 管控节点和执行器仍需 Python 3.14t、凭据及共享目录
dataflow control \
  --host 127.0.0.1 --data-root /absolute/shared/data
# 另一终端
cd frontend
npm run dev
```

本机打开 `http://127.0.0.1:8000`；前端启动在服务器上时，另一台电脑打开
`http://服务器IP:8000`。默认监听 `0.0.0.0:8000`，前端 `/api` 转发到启动机器上的
`http://127.0.0.1:8080`。页面点击「配置 API 连接」，地址填写 **`/api`**，令牌填写
`DATAFLOW_ADMIN_TOKEN`。服务器防火墙/安全组需允许你的客户端访问 8000。
代理保留 Authorization 和 Idempotency-Key，后端继续验证鉴权；同源代理无需配置 CORS。

管控 API 在其他端口或机器时显式指定：

```bash
npm run dev -- --host 0.0.0.0 --port 8000 --api-upstream http://127.0.0.1:18080
```

`--host`、`--port`、`--api-upstream` 的环境变量分别为 `DATAFLOW_DEV_HOST`、
`DATAFLOW_DEV_PORT`、`DATAFLOW_API_UPSTREAM`，命令行优先。只允许本机访问可用
`--host 127.0.0.1`。开发服务器只提供页面资产及 API 代理，不启动管控、执行器或数据库。
它仅用于开发；正式部署继续使用独立 Nginx 镜像，通过 HTTPS 暴露前端和 API。

启动后应持续占用终端并显示监听地址，按 Ctrl+C 停止。SSH 断开后仍要运行可用：

```bash
nohup npm run dev -- --api-upstream http://127.0.0.1:8080 > dataflow-console.log 2>&1 &
```

## 启动与访问排查

旧版 `npm run dev` 调用 Python 静态服务器，绑定 `127.0.0.1` 只能本机访问，
且不提供默认 `/api` 代理。更新仓库后使用新的 Node 启动命令。
旧命令立即退出时，仅 npm 打印的调用行不足以判断原因，需要查看后续 stderr 和退出码。
新版前端不依赖 `python3`，启动失败返回非零退出码和明确提示。

Linux 服务器可先在启动机器执行：

```bash
node --version
curl -I http://127.0.0.1:8000/
curl -sS http://127.0.0.1:8000/api/readyz
ss -lntp 'sport = :8000'
```

- 页面请求失败：检查进程和启动日志。端口冲突可改 `--port 8001`。
- 本机页面成功而远程失败：使用服务器 IP，核对监听地址、防火墙和安全组。
- `/api/readyz` 返回 502：前端已启动，检查管控是否启动以及 `--api-upstream` 地址。
- 连接时报 401：使用管理令牌，不能用执行器令牌；令牌不写入 URL 或 config.js。
- 直接连接其他域的 API：使用实际 API 地址，并配置后端精确 CORS 来源。

`localhost` 和 `127.0.0.1` 是不同来源，直接跨域连接时需要与配置严格匹配。
HTTPS 页面应连接 HTTPS API。

`frontend/config.js` 设置默认 API 地址及轮询间隔，可在部署时独立替换，无需改 Python 服务。默认地址 `/api`、轮询 5 秒；间隔最小 2 秒。不得把令牌写入这个公开配置文件。

## 功能与边界

| 页面 | 功能 |
| --- | --- |
| 任务中心 | 全局状态计数、状态/执行池筛选、游标分页、批量进度查询 |
| 任务详情 | DAG 节点状态、耗时、计数、尝试选择、执行器会话、日志尾部、最近事件、结果与文件清单、配置导出 |
| 任务管理 | 停止排队/运行中的任务；失败/取消任务从头重试，保留原记录；复制配置到编排器 |
| DAG 编排 | 添加/删除/重命名节点，编辑算子与参数，勾选上游依赖，图形预览，API 校验，JSON 导入/导出，显式保存/加载草稿，提交文件任务 |
| 执行器 | 在线/离线/排空状态、声明槽位/CPU/内存、心跳、按会话排空 |

一个输入文件对应一个任务；DAG 完整运行在一个执行器上。前端提交的是共享文件的绝对路径，不上传本地文件。算子使用 `module:symbol`，代码须安装在执行器镜像中。前端/管控 API 均不执行、导入用户算子。

DAG 校验采用同一个 `TaskSpec.parse` 契约，检查图结构、参数、资源和重试字段，返回规范化配置及拓扑层。此阶段允许尚未挂载的路径和尚未安装的算子；提交时检查文件位置、存在性和内容校验和，执行时检查算子是否可加载。

CPU/内存是调度声明，实际限制由 Pod 资源控制。日志是最多约 16 KiB 的末尾，事件是最近 200 条；完整日志归档、文件预览下载、算子目录/版本注册、用户权限分级、独立 DAG 模板服务未包含在此版本。草稿位于当前浏览器，JSON 导出可用于跨环境迁移。

列表每页 30 条，使用一个批量状态请求获取该页进度，不逐行请求任务详情。详情页仅轮询选中的任务。页面隐藏时暂停刷新，DAG 编辑时不轮询；网络故障保留上次数据和未提交编辑，401 停止轮询。提交和重试使用幂等键，同一页面相同配置的提交响应丢失后可再次确认，不重复创建任务；更改配置会生成新键；确认成功后的主动再次提交或重试创建新任务。刷新页面后内存中的提交键不保留，需先检查任务列表再重新提交。

图预览最多显示前 200 个节点，节点选择列表和配置仍包含全部节点；API 上限为 1000。依赖编辑是勾选上游节点，布局自动计算，不是拖拽画布。

## Kubernetes

两个镜像分别构建，推送到自己的镜像仓库，不能假设默认 GHCR 镜像已经发布：

```bash
docker build -t registry.example.com/dataflowcore:0.3.0 .
docker build -t registry.example.com/dataflowcore-console:0.1.0 frontend
docker push registry.example.com/dataflowcore:0.3.0
docker push registry.example.com/dataflowcore-console:0.1.0
helm upgrade --install dataflowcore charts/dataflowcore \
  --set image.repository=registry.example.com/dataflowcore \
  --set console.enabled=true \
  --set console.image.repository=registry.example.com/dataflowcore-console
kubectl port-forward service/dataflowcore-console 8081:8080
```

前端 Deployment/Service 是独立资源，通过内部服务地址转发到 API，默认关闭，以兼容现有无前端部署。提供现有 PostgreSQL Secret 与 RWX PVC，方法见 [deployment.md](deployment.md)。前端不挂载共享文件卷，也不接触数据库或执行器凭据。

如前端在另一个域直接连接 API，配置 Helm `corsOrigins`，例如：

```bash
helm upgrade --install dataflowcore charts/dataflowcore \
  --set 'corsOrigins[0]=https://console.example.com'
```

后端默认禁用跨域。只允许列出的确切 HTTP(S) 来源及 GET/POST、Authorization/Content-Type/Idempotency-Key 请求头；worker API 不提供浏览器预检。允许来源仍须通过管理令牌认证。

## 验收

```bash
# Python 3.14t，实际关闭 GIL
python -m pip install -e '.[dev]'
pytest -q tests/test_console_api.py
cd frontend
npm ci
npm run test:dev
npx playwright install --with-deps chromium
DATAFLOW_TEST_PYTHON=python npm test
```

浏览器测试启动独立 Node 开发服务器、真实管控节点和真实 free-threaded 执行器，覆盖实际 CORS 连接及默认 `/api` 代理。覆盖鉴权失败、筛选/分页、节点编辑与环拒绝、草稿/导出、文件 DAG 执行、六节点文档入库、响应丢失的幂等提交、运行中取消、重试、失败日志、XSS 字符串安全渲染、排空、移动端及网络异常/令牌失效。测试中只有故障注入拦截请求；成功业务响应来自真实 API。Node 开发服务器测试另行验证静态资源、鉴权及请求体转发、后端不可达、端口冲突、进程持续运行和停止。

CI 分别运行 Python/PostgreSQL 验证、浏览器验证和 Kubernetes 故障恢复验证，上传前端截图、失败 trace、浏览器报告。独立前端镜像构建与 K8S 前端 Service 的页面/API 代理验证也进入 CI。

算子提交配置与上游结果是独立通道，填写入口、优先级和运行示例见 [operator-parameters.md](operator-parameters.md)。
