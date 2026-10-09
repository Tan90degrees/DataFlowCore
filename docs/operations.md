# 操作与验收

| 现象 | 检查 |
|---|---|
| QUEUED未运行 | workers的online、pool/runtime_version和资源/槽位 |
| LOST后重排 | Attempt.error中的租约过期或deadline |
| FAILED | 每次Attempt错误，修复输入/算子后手动retry |
| STOPPING | 等待取消确认；执行器失联时等待租约到期 |
| 管控503 | PostgreSQL连接、schema与事务 |
| GIL开启 | doctor、算子C扩展兼容性 |

```bash
dataflow get <task-id>
dataflow events <task-id>
dataflow workers
kubectl logs deployment/dataflowcore-control --tail=200
kubectl logs <worker-pod> --tail=200
```

运行中stdout/stderr尾部在/work/<session>/<attempt>/task.log，可kubectl exec读取。
最后16KiB通过进度log_tail持久化，任务结束后清理本地工作目录。Pod删除后尚未上报
的日志可能丢失；完整业务日志由部署日志系统采集。历史任务/产物自动GC尚未实现。
默认租约30秒、interval2秒、停止宽限5秒、尝试3次。发现时间约为租约加reaper周期，
数据库/API故障会影响恢复时间。管控长时间不可用，旧任务停止，恢复后允许整文件重试。

## 验收

- 真实3.14t子进程完成DAG并报告gil_enabled=false。
- 并发领取没有重复有效分配；CPU/内存/槽位准入有效。
- supervisor被杀后任务子进程终止，自动新Attempt完成。
- 管控重启后既有执行继续或到期后重试。
- 强停无响应算子，取消不重试，槽位重新可用。
- 旧令牌被拒绝，完成响应丢失可幂等重放。
- SQLite和真实PostgreSQL使用同一组语义测试。
- K8S实际删除执行器Pod、重启管控Deployment、停止任务。

e2e-evidence包含K8S任务历史。仅对专用测试数据库设置DATAFLOW_TEST_DATABASE_URL，
测试每例创建/删除独立schema。
