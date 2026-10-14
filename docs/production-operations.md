# 生产运维（0.8.0）

## 接入和升级

在部署文档基础上使用 charts/dataflowcore/values-production.yaml，替换仓库、Secret、
PVC 和业务版本。image.digest/console.image.digest 设置后忽略 tag。业务镜像改变时
runtimeVersion 应标识业务环境，提交 runtime_version 与之相同，重试不替换历史环境。
先停旧管控再升级。命名空间锁只保证单活，不提供主备或无停机升级。

profile 启用两执行器与 PDB，单执行器需关闭或调整 PDB；它不保证节点故障或任意
Deployment 升级无损，超过终止宽限的长任务仍整文件重试。Ingress 启用时配置实际
className/host/tlsSecret；NetworkPolicy 启用后控制面只接收 worker/console 与
controlIngressPeers，直连 API 和监控来源也要加入 peers，业务出站访问不限制。

组件 API、前端和指标不需要 Bearer 凭证。Secret 只需 database-url，算子代码由
业务镜像统一交付，没有人员/角色、轮换凭证、审计或包白名单配置。旧环境中的
admin/worker 参数不再启用鉴权，历史 audit 表保留但不继续写入。

控制面和前端一起升级。已有 Attempt 的单项续租/完成 API 保持兼容；旧队列的
runtime_version 不会替换，保留对应业务版本的执行器处理完后再排空。

## 维护与背压

前端「运维」或 POST /v1/control {"paused":true} 暂停；paused=false 恢复。
暂停阻止新提交/上传/领取，续租、完成、取消和领取重放可用，已提交同键任务仍返回
旧记录。暂停持久化并随备份保留。GET /v1/control 的 quiescent=true 是已暂停且
活跃尝试/上传均为零的当前检查结果，备份窗口仍须协调外部写卷与清理程序。

control.maxUnfinished 默认 10000，计算 QUEUED/RUNNING/STOPPING（含退避任务）。
满额 429，维护拒绝 503，附 Retry-After: 5 与 code；等待后用同一幂等键重试。
设 0 禁用限额须自行评估容量。管理并发默认 8，超额 api_concurrency，worker 不占
此额度。上传检查保留余量与在途预留，507 后先扩容/清理；不约束其他程序写共享卷。

## 响应、诊断与监控

queue_diagnostic 分辨维护、退避、离线、池、版本、排空、容量与槽位，等待领取也
可能受历史整任务预留影响。算子等待见 progress.steps 的 resource_wait_seconds。
wait_reasons 与执行器页的等待瓶颈区分 CPU、内存、执行器 IO 配额、本槽位 CPU/IO
线程池及老请求保护。一个节点可有多个原因。线程池限制先查部署宽度；CPU/内存不足
先核对声明和 Pod 实测；aging 时看 protected_resources 与最长等待，运行节点释放后
老请求优先推进。NO_FREE_SLOT 是文件槽位已满，应结合驻留缓存内存评估 slots。
0.6.0 默认 GET task 只展开当前尝试，历史 detail_loaded=0；请求
GET /v1/tasks/{id}?attempt={attempt-id} 同时取得所选历史详情，前端自动处理。
列表默认 summary=1；summary=0 每页最多 10 个定义。升级依赖历史完整 payload 的
客户端时相应调整，批量轮询继续用 statuses。

默认 JSON 日志，成功 HTTP 轮询属于 DEBUG，通过 DATAFLOW_LOG_LEVEL=DEBUG 临时
打开。重复控制连接/恢复扫描故障首次保留诊断，之后至多每 30 秒一条，恢复时一条
恢复记录。HTTP 请求量/时延、组件错误计数持续累计，不因日志限频丢失故障数量。
SDK APIError 提供 request_id/code/retry_after。任务启动日志包含 task_id、attempt_id、
worker_session 和 runner_id；详情中的 generation 区分不同 Pod/槽位和 PID 复用。
runner_replaced 日志含回收原因：task_not_succeeded、unfinished_pool_work、
unmanaged_threads、child_processes 或 task_limit。线程池按线程初始化登记所有权，
已启动但尚未执行业务的池线程可继续复用；业务遗留线程/子进程仍触发隔离回收。

空闲执行器同样报告 scheduler。free_slots 由数据库活跃 Attempt 统计，不依赖可能
滞后的本机 busy 标记。快照是采样，需同时看 online/telemetry_age_seconds；离线
快照不是当前占用。sampled_rss_mb 是监督进程和常驻进程 RSS 求和，包含共享页，
/proc 不可用为 null，Pod 实际内存以 cgroup/平台数据为准。

PG 连接池按需增长到 16，保留 1 个，空闲 60 秒收缩；只有 1 个连接维护工作线程。
连接等待有界，损坏连接替换，超额/数据库故障返回可重试错误；DB 所有权连接丢失
仍停止管控，不依赖连接池重连掩盖独占锁丢失。语句默认超时 5000ms，锁等待最多
2 秒。运维页和 /metrics 展示连接数、可用连接、等待请求与累计等待时间。

指标标签固定，不含任务 ID/文件路径；任务/尝试数量、可用槽位、声明 CPU/内存占用、
算子运行/等待为 gauge。HTTP、组件错误和池计数随进程重启重置，不能用于历史审计。
monitoring.enabled=true 生成 ServiceMonitor 和告警，需平台提供 CRD/选择标签。
healthz 检查进程，readyz 检查数据库和恢复扫描。执行器独立健康与租约看门狗，不受
批量网络等待阻塞；健康快照 2 秒刷新，不 fsync。新增文件最长领取等待受 interval
影响，已运行文件的完成通知和槽位复用不等待 interval。

调优先查看队列阻塞原因、资源等待、空闲槽位、池等待与真实 Pod CPU/RSS。只在资源
预算允许、模型缓存/基线内存有余量时增加 slots；IO 长等待场景通常需要多个文件
并行，CPU 饱和时扩大文件槽位不会增加算力。interval 控制心跳/进度底噪，不用于
提高短任务完成吞吐。业务依赖和本地缓存仍固定，不逐任务启动算子 worker。

## 失败目录清理

```bash
dataflow gc-failed-outputs --dsn "$DATAFLOW_DATABASE_URL" \
  --data-root /dataflow --days 7 --limit 200
# 核对预演后显式执行
dataflow gc-failed-outputs --dsn "$DATAFLOW_DATABASE_URL" \
  --data-root /dataflow --days 7 --limit 200 --apply
```

只处理有元数据、所属任务终态、Attempt 为 FAILED/CANCELLED/LOST 且过期的目录。
输入、成功引用、链接、非目录受保护。删除/确认不存在后记录清理标记，重复运行
跳过；删后 DB 写入失败可重跑补标记。未知孤儿目录不自动删。任务/尝试、成功产物
与输入不删除，避免破坏手动重试，另行设计归档策略。

## 备份与恢复

使用独占维护窗口，暂停并等 quiescent=true；停止执行器并等全部退出以关闭业务
缓存/嵌入式索引，暂停外部写卷与清理。备份完整 data-root、元数据、镜像版本、
受控 Secret 配置，记录时间/校验和。

```bash
umask 077
pg_dump --format=custom --dbname="$DATAFLOW_DATABASE_URL" --file=metadata.dump
tar -C /srv/dataflow -czf shared-files.tgz .
sha256sum metadata.dump shared-files.tgz > backup.sha256
```

恢复先用隔离新 DB/卷，pg_restore --exit-on-error 导入，文件恢复到相同绝对路径、
UID/GID 和权限。启动一个管控，确认暂停保留，核对记录、manifest 哈希、索引与版本，
再恢复接入并提交验收文件；核对前保留原备份与卷。
CI verify_backup_restore.py 只用于一次性 PG 容器/目录：四文件业务、原生备份恢复、
历史哈希校验、两个新文件继续执行。需要创建临时 DB 权限。真实恢复时间/RPO 由
数据量、备份频率和平台决定，在用户环境另行演练。

## 发布

Release workflow 对版本 tag 或手动指定已有 tag 先跑完整 CI，再发布镜像与包、
images.json、SHA256SUMS，含 SBOM/provenance。构建凭据用 secrets，不能放入会
公开的 build args；运行依赖由 production constraints 约束，业务依赖统一锁定。
普通 PR 不发布镜像，合入不等于发 tag、推送 GHCR 或部署；发布后用实际 digest。
来源证明不替代企业签名/准入策略。

参考：[PG dump](https://www.postgresql.org/docs/17/app-pgdump.html)、
[Docker attestations](https://docs.docker.com/build/ci/github-actions/attestations/)、
[Prometheus Operator API](https://prometheus-operator.dev/docs/api-reference/api/)。

