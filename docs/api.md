# API

文件上传的字节协议、响应与恢复语义见 [file-uploads.md](file-uploads.md)。

`GET /v1/files/limits` 同时返回解析后的绝对 `upload_dir`。目录由管控启动时的 `--upload-dir`/`DATAFLOW_UPLOAD_DIR` 指定（默认 data-root 下的 `uploads`），POST 上传沿用此配置。

0.8.0 API 作为宿主产品的内部接口，不要求 Bearer 凭证，不提供角色、审计或算子白名单。
响应含 X-Request-ID。429/维护 503 含 Retry-After: 5 与 code；用原幂等键稍后重试。
任务详情默认只展开当前 Attempt；历史为摘要，使用 ?attempt={attempt-id} 按需展开。
GET /healthz、/readyz、/version 为探针；/metrics 为 Prometheus 指标。

| 方法 | 路径 | 功能 |
|---|---|---|
| POST | /v1/files?filename=... | 上传原始文件字节，必须有 Content-Length 和 Idempotency-Key |
| GET | /v1/files | 携带原 Idempotency-Key 查询已完成上传 |
| GET | /v1/files/limits | 查询上传大小、并发及超时限制 |
| POST | /v1/tasks | 提交单文件TaskSpec，必须有Idempotency-Key |
| GET | /v1/tasks | 游标分页，支持状态/池过滤 |
| POST | /v1/tasks/status | 批量状态摘要，每次1..100个ID |
| GET | /v1/tasks/{id} | 定义、状态、所有Attempt、进度、错误及结果 |
| GET/POST | /v1/control | 查看生产状态；暂停/恢复新接入与领取 |
| GET | /v1/tasks/{id}/events | 最近200条事件 |
| POST | /v1/tasks/{id}/cancel | 幂等停止 |
| POST | /v1/tasks/{id}/retry | 失败/取消后手动重试，必须有Idempotency-Key |
| GET | /v1/workers | 会话、容量、存活与排空状态 |
| POST | /v1/workers/{session-id}/drain | 排空，已有任务继续，停止新领取 |
| POST | /v1/worker/register | 注册不可变会话与容量 |
| POST | /v1/worker/pulse | 批量心跳、续租、领取与执行器快照 |
| POST | /v1/worker/heartbeat | 存活与draining |
| POST | /v1/worker/claim | 幂等领取完整任务 |
| POST | /v1/worker/renew | 续租、进度、接收取消 |
| POST | /v1/worker/complete | 幂等结果提交 |

示例见examples/file_pipeline.json。0.5.0 只在节点配置
`workload=cpu/io`、`resources={cpu, memory_mb}`。公开 spec 不再包含任务 CPU、内存或
`dag_workers/io_workers`。GET 任务另有只读 `placement`（逐维最大算子需求）和
`scheduling` 摘要，提交使用 spec，无需复制这些诊断字段；成功结果的 `runtime.peak_resources` 为本次
Attempt 的预留峰值，可能大于放置门槛。算子并发自动受执行器共享预算与池上限控制，详见
[算子级资源调度](operator-scheduling.md)。max_attempts包含首次执行，timeout为每次Attempt最长时间，retry_delay
为初始指数退避。输入SHA256由管控固定，调用者可以提前提供。
完成/续租字段为tid、aid、session_id、token。领取返回assignment包含task_id、
attempt_id、number、token、完整spec和lease_seconds。续租返回cancel及lease_seconds。
过期/身份错误/冲突返回409，未知记录404，校验400，临时数据库故障503。
新文件上传成功返回201，同键重传返回200；其他成功接口返回200。单步骤JSON256KiB、完成请求1MB、进度256KiB，大结果写文件并返回引用。
运行中task.log在执行器现场，保留最后16KiB；API进度中的log_tail同时持久化。
完成后本地工作目录清理，不保留每个业务项的单独事件。

任务列表参数：`limit=1..100`（默认100）、`cursor`（使用上次的next_cursor）、
`state`、`pool`、`summary=1`。返回 `{tasks: [...], next_cursor: string|null}`。
默认 summary=1；summary=0 每页最多 10 个完整定义。批量轮询使用状态接口。
游标按不可变 created_at/id 排序；翻页过程中状态可能变化，过滤列表不是事务快照。

批量状态请求 `{task_ids: ["id1", "id2"]}`，返回 `{tasks: [...], missing: [...]}`。
摘要包括state、attempt_count、current_attempt、时间、error和紧凑progress，不含
输入定义、完整产物和日志。状态接口无需幂等键；未知ID放入missing。

Progress.fraction按DAG步骤等权计算，运行中有total的步骤计入其局部比例，
不是字节/页数加权的业务SLA。实时快照按100ms节流；首次RUNNING与新增资源等待
立即发布，随后由上报、DAG推进或约500ms兜底检查刷新。实时快照原子替换、不fsync，
终态进度fsync后与结果提交到持久数据库。Pod丢失可从头重试，进度不是续跑检查点。
管控可见延迟仍取决于worker.interval。终态包含FAILED/CANCELLED/SKIPPED。
资源等待状态为 WAITING_RESOURCES，步骤包含 workload/resources、resource_wait_seconds
和 wait_reasons。原因包括cpu、memory、io_concurrency、cpu_pool、io_pool和aging；
依赖未就绪的节点仍为PENDING，不进入资源队列。
Progress.scheduler 和活跃 worker.scheduler 为执行器共享预留快照，不是每任务独占值。
其中waiting_by_reason按固定原因统计等待数，一个请求可同时受多种限制，不能相加
当作等待总数。protected_resources是当前老请求保护需求，无保护时为null；这是需求量，
不代表实际使用或额外重复预留。/metrics的dataflow_operator_waiting{reason="cpu"}等六组
固定标签聚合在线执行器；受心跳和快照年龄影响，不包含任务ID标签。
成功Result.usage包含当前任务的cpu_seconds和运行进程生命周期peak_rss_bytes。
极大DAG进度压缩为步骤状态，诊断日志按JSON预算裁剪，避免进度导致续租失败。

## 浏览器管控 API

- `GET /v1/overview`：返回 `{task_counts: {STATE: count}, version}`，计数包括所有任务。
- `POST /v1/dags/validate`：JSON body 与提交任务相同；返回 `{valid: true, spec: 规范化配置, layers: [[step_id, ...], ...]}`。无幂等键、无持久化，不检查文件存在性或导入算子代码。无效图/字段返回 400。
- `OPTIONS /v1/*`：仅配置的确切 CORS 来源可以预检 GET/POST 与 Authorization/Content-Type/Idempotency-Key；worker 专用接口拒绝预检。

管控节点使用 `DATAFLOW_CORS_ORIGINS` 或 `--cors-origins` 配置逗号分隔的确切 HTTP(S) 来源，默认空。浏览器来源不允许时返回 403。响应包含 `Vary: Origin`，不使用跨域 cookie。独立前端见 [console.md](console.md)。

任务配置支持顶层 `parameters`（公共配置）和 `steps[].parameters`（节点配置），均须为 JSON 对象。节点同名键覆盖公共配置，嵌套对象整体替换；算子从 `context.parameters` 读取，依赖产物仍从 `inputs` 读取。配置持久化并随重试保留，完整示例见 [operator-parameters.md](operator-parameters.md)。

### 执行器批量通信

`POST /v1/worker/pulse` 接受 `session_id`、`draining`、`telemetry`、`renewals` 和
`claims`。renewals 最多 32 个 `{tid,aid,token,progress?}`；未变化的 progress 可省略，
保留数据库中的进度。claims 最多 32 个不同的领取 ID，丢失响应必须原样重试，直到
分配已处理或收到单项过期冲突。telemetry 最多 16 KiB；整批仍受 1.1 MB HTTP 限制。

响应 `{draining,renewals:[{aid,cancel,lease_seconds}|{aid,status,error}],
claims:[{claim_id,assignment}|{claim_id,status,error}]}`。一次有界事务提交心跳、有效续租
和分发；单项租约失效返回 404/409，不妨碍其他续租。非法格式在写入前拒绝整批。
大进度拆批，不持有事务等待网络、业务或本机资源。单项旧 API 继续兼容已有执行尝试。

`GET /v1/workers` 返回数据库统计的 active_tasks/free_slots，独立 scheduler 快照、
快照年龄与 runtime 采样。离线会话的资源不是当前容量；旧执行器未报告的快照为 null。
RSS 为监督进程和常驻进程的采样求和，包含共享页，不是 Pod 内存。/proc 不可用返回 null。
Attempt progress.runner.generation 为槽位唯一身份，和 worker_session 联合区分 Pod/PID 复用。

完成请求可携带最终 progress，与结果在同一事务保存，避免文件在续租期间完成而遗漏终态步骤。结果和最终进度合计最多 1 MB；超过合计额度时执行器先以有效租约保存最终进度，再提交结果。完成幂等哈希只包含结果语义，重放不会改写已保存的终态快照。
