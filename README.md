# DataFlowCore

一个文件一个任务。一个执行尝试的完整 DAG 始终在同一个执行器上运行。
单个管控服务管理多个执行器，执行器内部使用 Python **3.14t** 无 GIL 线程并行。
Pod 或节点故障后自动从头重试，暂不支持断点续跑。没有 Ray / KubeRay 依赖。

## 已实现

- HTTP 管控 API、Python DAG SDK 和 CLI。
- JSON DAG 校验、依赖推进、分支并行和数据汇合。
- 一个执行器可运行多个文件任务，每个任务使用独立子进程。
- PostgreSQL 持久化任务、执行尝试、事件和步骤进度。
- 幂等提交、幂等领取、幂等完成；租约、超时、重试上限和指数退避。
- 自动整文件重试、管控重启恢复、旧执行结果隔离。
- 排队取消、协作停止、超时进程组终止、Linux 父进程死亡保护。
- 运行环境、执行器池、槽位与 CPU/内存预算匹配。
- 管控与执行器分别认证、输入校验和 SHA256 固定、输出 manifest。
- Python 3.14t 容器、Docker Compose、Helm、真实子进程与 K8S 故障验收。
- 流式有界的页/块线程并行、批量状态查询、游标分页、执行器排空。
- 完整文档入库业务案例、故障注入与可复现性能基准。

输出采用每次执行独立目录，数据库中的成功结果引用才是正式结果。
重试可能重复计算；外部数据库/向量库写入需要业务使用稳定标识保证幂等。

## 最快启动：Docker Compose

需要 Docker Compose。首次构建安装 CPython 3.14.8t，需要联网；业务运行不动态安装依赖。

```bash
git clone https://github.com/Tan90degrees/DataFlowCore.git
cd DataFlowCore
python3 scripts/init-demo.py
docker compose up --build -d --scale worker=2
set -a
source .env
set +a
curl -sS http://127.0.0.1:8080/readyz
curl -sS -X POST http://127.0.0.1:8080/v1/tasks \
  -H "Authorization: Bearer $DATAFLOW_ADMIN_TOKEN" \
  -H 'Idempotency-Key: demo-file-001' \
  -H 'Content-Type: application/json' \
  --data-binary @examples/file_pipeline.json
```

返回任务 id 后查询：

```bash
curl -sS "http://127.0.0.1:8080/v1/tasks/<task-id>" \
  -H "Authorization: Bearer $DATAFLOW_ADMIN_TOKEN"
```

输出位于 `data/outputs/<task-id>/<attempt-id>/write.json`。
同一个幂等键和同一个规范化请求返回同一任务；修改定义后需要新键。
初始化脚本创建随机凭证，不覆盖已有 `.env`。生产 PVC 应由 UID/GID 10001 管理。
Compose PostgreSQL 使用持久卷；保留历史时不要执行 `docker compose down -v`。

## 本机运行与开发

```bash
uv python install 3.14t
uv venv --python 3.14t .venv
source .venv/bin/activate
uv pip install -e '.[dev,postgres]'
dataflow doctor
ruff check .
ruff format --check .
pytest -q
```

PostgreSQL 驱动使用纯 Python psycopg + 系统 libpq，Debian/Ubuntu 安装 `libpq5`。
不安装 `psycopg[binary]`，项目使用 CPython cp314t ABI。
CI 验证 psycopg 导入后 GIL 仍关闭。

本机演示可使用 SQLite（单机开发专用），两个终端分别运行：

```bash
export DATAFLOW_ADMIN_TOKEN="$(python -c 'import secrets; print(secrets.token_hex(24))')"
export DATAFLOW_WORKER_TOKEN="$(python -c 'import secrets; print(secrets.token_hex(24))')"
mkdir -p data
printf 'hello world hello\n' > data/input.txt
dataflow control --data-root "$PWD/data"
```

第二个终端设置相同 DATAFLOW_WORKER_TOKEN：

```bash
dataflow worker --data-root "$PWD/data" --slots 2 --cpu 4 --memory-mb 2048
```

把示例 JSON 的 input_path 改为本机输入绝对路径，然后：

```bash
dataflow submit examples/file_pipeline.json --key file-001
dataflow get <task-id>
dataflow wait <task-id>
dataflow events <task-id>
dataflow cancel <task-id>
dataflow retry <failed-or-cancelled-task-id> --key retry-001
dataflow workers
```

wait 成功返回0，失败/取消返回1。手动重试创建新任务，原记录保留；自动故障重试
在原任务下追加执行尝试。`--allow-gil`、`--insecure` 仅供开发。

## 业务 DAG

```python
from dataflowcore.sdk import Client, Pipeline
from dataflowcore.operators import read_text, word_count, write_json

flow = (
    Pipeline("file-processing", pool="default", cpu=2, memory_mb=512)
    .step("read", read_text)
    .step("count", word_count, depends_on=["read"])
    .step("write", write_json, depends_on=["count"])
)
task = Client("http://control:8080", "your-admin-token").submit(
    flow.spec("/dataflow/input.txt"), key="file-001"
)
```

算子预装在镜像中，通过 module:symbol 引用，签名为 operator(context, inputs)。
支持函数、协程函数、无参数构造的 callable class；实例按任务/步骤创建。
inputs 是直接上游返回值映射，算子必须视依赖结果为只读。

```python
def process(context, inputs):
    context.check_cancelled()
    context.report(completed=0, total=100, message="processing")
    # context.input_path / context.output_dir / context.parameters
    return {"ok": True}
```

单步骤 JSON 上限256KiB，大结果写 output_dir 并返回引用。页/BOX内部并行需自行
限制线程和缓冲并保证线程安全，可使用 `context.map(fn, items, max_workers=4)`，
该接口按输入顺序流式返回，并限制预取数量。PermanentError 表示不自动重试的业务错误。
业务算子属于受信代码；任务进程隔离不提供不受信代码沙箱。

## 完整业务案例与性能

[文档入库案例](docs/business-validation.md)包含“解析 → 分块/统计 → 向量 → 索引 → 回执”，
覆盖真实 HTTP 推理调用、幂等入库、失败重试、取消、超时、管控重启及 Pod 故障。
参考输入为 UTF-8 文本/Markdown；内置向量是可重复的测试模型，供验证编排使用。
[性能报告](docs/performance.md)提供环境、原始 JSON、三轮对比和一键复现命令。

SDK 的 `statuses(ids)` 每次最多查询100个任务，`wait_many(ids)` 自动分批并容忍
暂时失联；`iter_tasks()` 遍历所有任务，默认返回摘要。`dataflow drain <session-id>`
停止该会话领取新任务，已有文件继续执行，替换 Pod 后使用新会话接收任务。
进度包含步骤时长、等权完成比例，成功结果记录 CPU 时间与进程峰值 RSS。

## K8S 与验收

参见 [部署](docs/deployment.md)、[可靠性](docs/architecture.md)、
[API](docs/api.md)、[操作手册](docs/operations.md)。
CI 使用真实 PostgreSQL、3.14t及容器；K8S 验收实际删除执行器 Pod、重启管控、
取消运行任务，上传 e2e-evidence。具备 Docker/kind/kubectl/Helm 的环境可运行：

```bash
docker build -t dataflowcore:e2e .
bash e2e/run.sh
```

创建 dataflowcore 本地 kind 集群，结束后运行 `kind delete cluster --name dataflowcore`。
验收 PostgreSQL emptyDir 和共享 hostPath 仅用于临时测试集群。

## 范围

第一版支持共享文件系统单文件任务、本地 DAG 和故障后整文件重试。
对象存储、断点续跑、控制面主备、租户公平调度、自动 HPA 和历史产物 GC 是后续扩展。
当前支持手动扩容执行器，队列与容量自动匹配。CPU/内存为准入预算，Pod resources
是容器资源边界。单控制面保证可恢复管理，不保证故障期间API持续可用。

## 算子配置参数

提交时可填写任务级 `parameters` 和各节点的 `steps[].parameters`；算子从 `context.parameters` 读取配置，从 `inputs` 读取上游结果。同名配置以节点值为准，嵌套配置在节点间独立。前端选中节点即可填写参数并提交。

详见 [算子参数、提交示例与优先级](docs/operator-parameters.md)。

## 前端管控台

独立静态前端通过 API 管理任务、查看 DAG 进度/日志/事件/结果、停止与重试任务、观察和排空执行器；编排器支持编辑节点与依赖、JSON 导入/导出、API 校验和提交。前后端分别构建、部署，无生产 npm 依赖。

`docker compose up --build -d` 后打开 **http://127.0.0.1:8081**，连接地址使用 `/api`，输入 `.env` 中的管理令牌。Kubernetes 可设置 `console.enabled=true` 启用独立前端 Deployment/Service。

详见 [前端部署、功能边界与浏览器验收](docs/console.md)。
