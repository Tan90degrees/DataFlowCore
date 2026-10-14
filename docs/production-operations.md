# 生产运维（0.6.0）

## 部署与权限

在部署文档基础上使用 charts/dataflowcore/values-production.yaml，替换仓库、Secret、
PVC 和业务版本。image.digest/console.image.digest 设置后忽略 tag。业务镜像改变时
runtimeVersion 应标识业务环境，提交 runtime_version 与之相同，重试不替换历史环境。
先停旧 0.5 管控再升级。命名空间锁只保证单活，不提供主备或无停机升级。

profile 启用两执行器与 PDB，单执行器需关闭或调整 PDB；它不保证节点故障或任意
Deployment 升级无损，超过终止宽限的长任务仍整文件重试。Ingress 启用时配置实际
className/host/tlsSecret；NetworkPolicy 启用后控制面只接收 worker/console 与
controlIngressPeers，直连 API 和监控来源也要加入 peers，业务出站访问不限制。

主要凭证 DATAFLOW_ADMIN_TOKEN、DATAFLOW_WORKER_TOKEN；额外逗号分隔值用
DATAFLOW_ADMIN_TOKENS、DATAFLOW_WORKER_TOKENS，只读用 DATAFLOW_VIEWER_TOKENS。
Secret 可选键为 admin-tokens/worker-tokens/viewer-token。值须互不重复、随机、
24..256 个可打印 ASCII 字符，总数最多 24。更新须重启 Pod，无自动重新载入。
轮换先让管控接受新旧值，再更新客户端/执行器，等旧执行器排空，最后撤销旧值；
直接撤销仍续租的旧值会造成整文件恢复。

viewer 不可上传、提交、停止、重试、排空、维护，可读部署内全部任务、运维、审计、
metrics，并可无副作用批量查询/DAG 校验。审计为凭证指纹，非人员身份；业务密钥用
执行器 Secret/环境配置，不放任务参数。control.operatorModules 限制 API 新提交/
校验/重试的包前缀，不导入/安装代码，不自动撤销既有批准任务，也不是代码沙箱。

## 维护与背压

前端「运维」或 POST /v1/control {"paused":true} 暂停；paused=false 恢复。
暂停阻止新提交/上传/领取，续租、完成、取消和领取重放可用，已提交同键任务仍返回
旧记录。暂停持久化并随备份保留。GET /v1/control 的 quiescent=true 是已暂停且
活跃尝试/上传均为零的当前检查结果，备份窗口仍须协调管理员、外部写卷与清理程序。

control.maxUnfinished 默认 10000，计算 QUEUED/RUNNING/STOPPING（含退避任务）。
满额 429，维护拒绝 503，附 Retry-After: 5 与 code；等待后用同一幂等键重试。
设 0 禁用限额须自行评估容量。管理并发默认 8，超额 api_concurrency，worker 不占
此额度。上传检查保留余量与在途预留，507 后先扩容/清理；不约束其他程序写共享卷。

## 响应、诊断与监控

queue_diagnostic 分辨维护、退避、离线、池、版本、排空、容量与槽位，等待领取也
可能受历史整任务预留影响。算子等待见 progress.steps 的 resource_wait_seconds。
0.6.0 默认 GET task 只展开当前尝试，历史 detail_loaded=0；请求
GET /v1/tasks/{id}?attempt={attempt-id} 同时取得所选历史详情，前端自动处理。
列表默认 summary=1；summary=0 每页最多 10 个定义。升级依赖历史完整 payload 的
客户端时相应调整，批量轮询继续用 statuses。

默认 JSON 日志；X-Request-ID 关联审计和日志，SDK APIError 提供 request_id/code/
retry_after。指标标签固定，不含任务 ID/文件路径。任务/尝试数量为 gauge，HTTP
counter/histogram 属于进程生命周期，重启重置。monitoring.enabled=true 生成
ServiceMonitor 和五条告警，需 CRD、monitoring.labels 与可用 viewer-token。
Pod CPU/RSS/OOM、卷、DB 与模型服务由平台监控；声明预算不等于 RSS。
healthz 检查进程，readyz 检查数据库和恢复扫描。执行器独立发布健康信息，网络等待
影响 readiness，不停止安全线程。PG 语句默认超时 5000ms，锁等待最多 2 秒。

## 失败目录清理

```bash
dataflow gc-failed-outputs --dsn "$DATAFLOW_DATABASE_URL" \
  --data-root /dataflow --days 7 --limit 200
# 核对预演后显式执行
dataflow gc-failed-outputs --dsn "$DATAFLOW_DATABASE_URL" \
  --data-root /dataflow --days 7 --limit 200 --apply
```

只处理有元数据、所属任务终态、Attempt 为 FAILED/CANCELLED/LOST 且过期的目录。
输入、成功引用、链接、非目录受保护。删除/确认不存在后记录标记与审计，重复运行
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

