# 内部组件的性能验证（0.7.0）

衡量资源利用、吞吐和底噪时，必须固定业务、容量和通信间隔，再核对执行正确性。
采用真实管控/执行器/常驻进程，业务是文件解析、分块、统计、HTTP 嵌入、幂等索引
和收据；逐文件核对单次成功、无 GIL、索引行数和所有输出 manifest 哈希。
HTTP 嵌入是带固定延迟的测试服务，不代表真实模型。

对照源是 0.6.0 已合入的 fe536bac72f6cd16cc1c4c347e4b8f27b6621dde，保留在独立目录。
每轮单独起进程、预热空闲槽位，顺序运行对照和当前实现。每个 PG 业务样本使用新的
随机 schema，结束后清理该 schema，不混用升级前后的表结构或累计任务。

```bash
git worktree add --detach /tmp/dataflowcore-before fe536bac72f6cd16cc1c4c347e4b8f27b6621dde
python scripts/benchmark_internal.py --baseline-source /tmp/dataflowcore-before \
  --repetitions 3 --files 24 --interval 2 --idle-seconds 8 \
  --output default.json
# 同时检查积极轮询的旧实现，防止只通过放大旧轮询间隔展示收益
python scripts/benchmark_internal.py --baseline-source /tmp/dataflowcore-before \
  --repetitions 3 --files 48 --interval 0.1 --idle-seconds 8 \
  --output aggressive.json
```

生产 PG 对比加 `--dsn "$DATAFLOW_TEST_DATABASE_URL"`，需一次性数据库及创建 schema
的权限。DSN 只通过子进程环境传递，不写报告。CI 保存独立 PostgreSQL 对比产物。
报告保留每轮配置、版本、源摘要、硬件信息、通信数和完整业务验证。使用多轮中位数，
不把一次小样本当作硬件上限。

空闲样本为 4 个预建槽位、0.2 秒通信间隔、3 秒租约，测 8 秒窗口内的 worker HTTP
请求和日志字节。排除启动日志。/proc 无法可靠读取时 CPU 标为 null，不声称 CPU
使用率为零。资源预留峰值从 scheduler 读取，RSS 包含共享页，属于不同量。

完成通知独立于心跳间隔：间隔 2 秒控制空闲通信与进度刷新，但短文件完成立即上报
并复用槽位。资源通知只唤醒本次真正获准的等待者，不向所有槽位广播；丢失通知以
0.5 秒检查兜底。PG 复用有界连接，不能用 SQLite 样本推算远程 PG 的网络收益。

实测数据见 [default.json](benchmarks/internal-efficiency/default.json) 与
[aggressive.json](benchmarks/internal-efficiency/aggressive.json)。以下为本机 SQLite、CPython 3.14.8t、4 槽位/8 CPU 声明预算的三轮中位数。
两组分别测默认通信与积极通信，所有样本均单次成功。下面的固定样本来自终态进度补全前的
优化版本（报告中保留源码摘要），用于说明消除周期等待的机制收益。最终交付代码的
PostgreSQL 结果以本 PR 最新成功 CI 的 business-performance 产物为准；不同源码的
测量不可当作同一个样本。

| 通信间隔 | 文件数 | 吞吐：0.6 → 0.7（文件/秒） | 比率 | worker HTTP / 文件 |
| --- | --- | --- | --- | --- |
| 默认 2 秒 | 24 | 1.65 → 27.99 | 16.96× | 3.42 → 2.54 |
| 积极 0.1 秒 | 48 | 19.10 → 23.02 | 1.21× | 3.40 → 2.69 |

空闲 8 秒中位数为 76 → 39 次 HTTP（降低 48.7%），日志为 15960 → 0 字节。
默认间隔的收益主要来自完成事件消除轮询等待，不能外推为算法或模型算力提升。
积极间隔组还显示通信开销降低，吞吐受共享宿主调度、IO 和模型服务影响，有波动。
/proc 采样不可用的字段保持 null；远程 PostgreSQL 的连接复用另由 CI 验证。
