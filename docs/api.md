# API

管理员接口使用Authorization Bearer admin-token，执行器接口使用独立worker-token。
GET /healthz、/readyz、/version为公开探针；/metrics使用管理员认证。

| 方法 | 路径 | 功能 |
|---|---|---|
| POST | /v1/tasks | 提交单文件TaskSpec，必须有Idempotency-Key |
| GET | /v1/tasks | 最近100个任务 |
| GET | /v1/tasks/{id} | 定义、状态、所有Attempt、进度、错误及结果 |
| GET | /v1/tasks/{id}/events | 最近200条事件 |
| POST | /v1/tasks/{id}/cancel | 幂等停止 |
| POST | /v1/tasks/{id}/retry | 失败/取消后手动重试，必须有Idempotency-Key |
| GET | /v1/workers | 会话、容量、存活与排空状态 |
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
