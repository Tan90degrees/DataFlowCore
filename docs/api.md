# API

管理员接口使用Authorization Bearer admin-token，执行器接口使用独立worker-token。
GET /healthz、/readyz、/version为公开探针；/metrics使用管理员认证。

| 方法 | 路径 | 功能 |
|---|---|---|
| POST | /v1/tasks | 提交单文件TaskSpec，必须有Idempotency-Key |
| GET | /v1/tasks | 游标分页，支持状态/池过滤 |
| POST | /v1/tasks/status | 批量状态摘要，每次1..100个ID |
| GET | /v1/tasks/{id} | 定义、状态、所有Attempt、进度、错误及结果 |
| GET | /v1/tasks/{id}/events | 最近200条事件 |
| POST | /v1/tasks/{id}/cancel | 幂等停止 |
| POST | /v1/tasks/{id}/retry | 失败/取消后手动重试，必须有Idempotency-Key |
| GET | /v1/workers | 会话、容量、存活与排空状态 |
| POST | /v1/workers/{session-id}/drain | 管理员排空，已有任务继续，停止新领取 |
| POST | /v1/worker/register | 注册不可变会话与容量 |
| POST | /v1/worker/heartbeat | 存活与draining |
| POST | /v1/worker/claim | 幂等领取完整任务 |
| POST | /v1/worker/renew | 续租、进度、接收取消 |
| POST | /v1/worker/complete | 幂等结果提交 |

示例见examples/file_pipeline.json。cpu/memory_mb为准入预算，dag_workers限制DAG
线程并发。max_attempts包含首次执行，timeout为每次Attempt最长时间，retry_delay
为初始指数退避。输入SHA256由管控固定，调用者可以提前提供。
完成/续租字段为tid、aid、session_id、token。领取返回assignment包含task_id、
attempt_id、number、token、完整spec和lease_seconds。续租返回cancel及lease_seconds。
过期/身份错误/冲突返回409，未知记录404，校验400，认证401，临时数据库故障503。
成功接口统一200。单步骤JSON256KiB、完成请求1MB、进度256KiB，大结果写文件并返回引用。
运行中task.log在执行器现场，保留最后16KiB；API进度中的log_tail同时持久化。
完成后本地工作目录清理，不保留每个业务项的单独事件。

任务列表参数：`limit=1..100`（默认100）、`cursor`（使用上次的next_cursor）、
`state`、`pool`、`summary=1`。返回 `{tasks: [...], next_cursor: string|null}`。
未指定summary保持原有完整任务字段；批量轮询应使用状态接口，避免反复传输定义/结果。
游标按不可变 created_at/id 排序；翻页过程中状态可能变化，过滤列表不是事务快照。

批量状态请求 `{task_ids: ["id1", "id2"]}`，返回 `{tasks: [...], missing: [...]}`。
摘要包括state、attempt_count、current_attempt、时间、error和紧凑progress，不含
输入定义、完整产物和日志。状态接口无需幂等键；未知ID放入missing。

Progress.fraction按DAG步骤等权计算，运行中有total的步骤计入其局部比例，
不是字节/页数加权的业务SLA。计数约100ms合并刷盘，生命周期转换立即刷盘；
管控可见延迟仍取决于worker.interval。终态包含FAILED/CANCELLED/SKIPPED。
成功Result.usage包含cpu_seconds和peak_rss_bytes，仅统计该文件子进程。
极大DAG进度压缩为步骤状态，诊断日志按JSON预算裁剪，避免进度导致续租失败。

## 浏览器管控 API

- `GET /v1/overview`：管理鉴权；返回 `{task_counts: {STATE: count}, version}`，计数包括所有任务。
- `POST /v1/dags/validate`：管理鉴权，JSON body 与提交任务相同；返回 `{valid: true, spec: 规范化配置, layers: [[step_id, ...], ...]}`。无幂等键、无持久化，不检查文件存在性或导入算子代码。无效图/字段返回 400。
- `OPTIONS /v1/*`：仅配置的确切 CORS 来源可以预检 GET/POST 与 Authorization/Content-Type/Idempotency-Key；worker 专用接口拒绝预检。实际请求仍要求管理令牌。

管控节点使用 `DATAFLOW_CORS_ORIGINS` 或 `--cors-origins` 配置逗号分隔的确切 HTTP(S) 来源，默认空。浏览器来源不允许时返回 403，来源允许但令牌错误时返回 401。响应包含 `Vary: Origin`，不使用跨域 cookie。独立前端见 [console.md](console.md)。

任务配置支持顶层 `parameters`（公共配置）和 `steps[].parameters`（节点配置），均须为 JSON 对象。节点同名键覆盖公共配置，嵌套对象整体替换；算子从 `context.parameters` 读取，依赖产物仍从 `inputs` 读取。配置持久化并随重试保留，完整示例见 [operator-parameters.md](operator-parameters.md)。
