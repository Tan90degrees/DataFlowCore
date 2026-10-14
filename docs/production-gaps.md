# 工业化 GAP 与生产基线（0.6.0）

产品面向可信业务代码的内部离线处理：单活管控、多执行器、每文件完整 Attempt
固定在一个常驻槽位，统一依赖，无动态算子安装与断点续跑。工业化保留这些边界。

| 原缺口 | 本轮能力 | 验收 |
| --- | --- | --- |
| 重复管控 | 初始化前取得元数据命名空间独占锁，所有权连接丢失停止服务 | SQLite/PG 争用、释放、重新取得 |
| 无限提交 | 未结束任务默认上限 10000，事务中检查，429/Retry-After | 20 个并发提交只接受配置的 3 个，同键重放可用 |
| 无维护窗口 | 持久暂停新提交/上传/领取，续租、完成、取消保持可用 | 暂停跨重建、领取重放、既有任务完成 |
| 排队原因不明 | 离线、池、版本、排空、容量、槽位、退避、维护诊断 | 八种原因回归，前端显示 |
| 单一全权令牌 | admin/viewer/worker，重叠密钥轮换 | 只读拒绝修改、批量查询可用、跨角色拒绝、撤销旧钥 |
| 无管理审计 | 提交、停止、重试、排空、维护与状态同事务记录 | 审计失败回滚，分页，不记录 Authorization/参数 |
| 历史尝试放大响应 | 历史摘要与按需详情，列表在 SQL 层省略大字段 | 大进度/结果不会全部载入 HTTP 详情 |
| 管理请求占用资源 | 管理并发默认 8，超额 429，worker 不占此额度 | 满额时 worker 注册可用 |
| 阻塞影响探针 | PG 语句/锁超时，独立健康发布与租约看门狗 | ready 失败时 health 可用，阻塞续租时安全线程继续 |
| 监控过少 | HTTP 计数/时延、尝试状态、队列年龄、在线容量、维护/恢复扫描 | 固定标签，五条告警 |
| 失败产物累积 | 默认预演，显式清理过期失败/取消/丢租目录 | 成功引用、输入、链接保护，重复运行可恢复 |
| 上传填满卷 | 空闲空间、在途容量预留与保留余量检查 | 拒绝后额度/预留不泄露，存储不足 507 |
| 生产部署缺项 | TLS Ingress、NetworkPolicy、PDB、监控 CRD、镜像 digest | 生产 profile 和全功能 Helm 渲染 |
| 无恢复演练 | 原生 pg_dump/pg_restore、共享文件恢复，再跑新任务 | CI backup-restore-evidence.json |
| 发布难追溯 | 指定 tag 的完整 CI 后发布镜像/包、digest/校验和/SBOM/provenance | Release 流水线，实际触发后产生发布物 |

使用方法见 [生产运维](production-operations.md)。新配置属于部署/运维，任务仍只填
各节点 workload/resources。

上线需平台提供持久 PostgreSQL/RWX、TLS、Secret、日志与集群资源监控。
NetworkPolicy 需要 CNI 执行；监控资源需要 Prometheus Operator CRD、实际 peers 和
选择标签。模板渲染不代表已验证用户集群的网络隔离和证书。真实算子还须做峰值 RSS、
模型异常、大文件和完整业务周期的持续负载验收，测试模型不能推算 OCR/NPU 容量。
每个部署独占数据目录和元数据 schema；不同 schema 共用上传/输出目录不受锁保护。
升级先停旧 0.5 管控，它不会遵守新独占锁。

| 独立扩展 | 当前边界 |
| --- | --- |
| 控制面主备/API 无停机 | 单活故障可恢复，升级有 API 短暂中断，没有选主/透明切换 |
| OIDC、人员审计、租户隔离 | 按服务凭证/指纹审计，非人员身份；viewer 可看该部署全部任务 |
| 对象存储/跨集群 | 当前共享文件系统与固定绝对路径 |
| 成功产物/输入/元数据归档 | 保留成功引用和输入，可清理失败目录；数据库仍需容量规划 |
| 自动 HPA、RSS 自适应 | 当前手动扩容、固定槽位、声明预算与平台硬限制 |
| 不可信算子、公网多用户 | 白名单不是代码沙箱，业务代码受信，公网入口需网关保护 |

依据：[PG locks](https://www.postgresql.org/docs/17/explicit-locking.html)、
[K8S probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-probes/)、
[Prometheus instrumentation](https://prometheus.io/docs/practices/instrumentation/)。

