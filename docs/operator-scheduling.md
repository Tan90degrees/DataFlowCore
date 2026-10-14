# 算子资源与公平调度（0.5.0）

新任务只配置算子自己的 CPU、内存和 CPU/IO 类型。任务层不再填写 cpu、memory_mb、
dag_workers、io_workers，也不选择调度模式。依赖就绪后，执行器按可用预算和固定池宽度
自动决定并发。每个文件的完整 Attempt 仍在同一执行器、同一常驻槽位完成。

## 放置门槛与运行峰值

| 数值 | 计算方式 | 用途 |
| --- | --- | --- |
| 执行器放置门槛 | 各维度取所有算子需求的最大值 | 确保能逐个执行完整 DAG，不预占整个文件的资源 |
| 当前预留 | 正在准入/执行的算子需求之和 | 跨文件、跨槽位限制 CPU、内存和 IO 并发 |
| 执行峰值 | 运行中当前预留的最大值 | 运行后观测，受依赖、时长、共享预算和并行度影响 |

A 需要 CPU 2 / 256 MiB，B 需要 CPU 4 / 512 MiB。串行的工作资源峰值为
CPU 4 / 512 MiB；同时运行可能达到 CPU 6 / 768 MiB。但放置门槛始终是
CPU 4 / 512 MiB：执行器可以让两者依次运行，无需因装不下并行峰值而把文件卡在队列。
CPU 8 / 1024 MiB 的执行器在预算允许时可以让它们并行。

实际 RSS 还包含解释器、驻留模型、缓存、上游结果和监督进程。算子声明控制工作资源
准入，不能独自推导进程实际内存峰值；Pod 需在算子预算之外预留常驻占用。
大量结果写文件、传递引用，避免累积在 DAG 内存中。

## 唯一的资源配置入口

```python
from dataflowcore.sdk import Pipeline

flow = (
    Pipeline("document")
    .step(
        "read",
        "dataflowcore.operators:read_text",
        workload="io",
        resources={"cpu": 0.1, "memory_mb": 64},
    )
    .step(
        "count",
        "dataflowcore.operators:word_count",
        depends_on=["read"],
        workload="cpu",
        resources={"cpu": 2, "memory_mb": 256},
    )
    .step(
        "write",
        "dataflowcore.operators:write_json",
        depends_on=["count"],
        workload="io",
        resources={"cpu": 0.1, "memory_mb": 32},
    )
)
spec = flow.spec("/dataflow/input.txt")
```

API JSON 的 steps[] 使用相同字段；前端在「节点配置」填写。CPU 最小精度 0.001，
CPU 型至少 0.001，IO 型可为 0；内存为至少 1 MiB 的整数。未声明节点默认
CPU 1 / 256 MiB、类型 CPU；API 规范化后每个节点都带完整资源。
业务配置仍通过 steps[].parameters / context.parameters，依赖结果通过 inputs。

SDK 输出、API 校验返回和导出 JSON 都不再包含任务级资源或并发开关。任务详情另有
只读 placement={cpu,memory_mb} 和 scheduling 诊断摘要，不能作为提交字段。
数据库内部缓存派生路由值用于索引，这些值不会出现在公开 spec。

## 在线调度策略

1. 管控按池、运行版本、空槽位和放置门槛过滤，再领取最早的可执行文件。不按整个
   DAG 并行峰值预留资源，不让不匹配的队头挡住可执行文件。
2. 执行器每轮批量登记依赖就绪的算子，一次事务进行共享准入。CPU/IO 各有固定
   DAG/页块池，授予数不超过所属池宽度；等待节点不占执行线程。
3. 等待较短时允许资源回填：跳过暂时装不下的大节点，让能装入剩余预算的节点先跑，
   避免严格 FIFO 的空闲。
4. 跨文件优先服务当前占用主导资源份额较低的文件。主导份额是 CPU、内存、IO 占用
   比例的最大值；同份额优先更早就绪的节点，避免宽 DAG 独占准入机会。
5. 最早且所属池有空位的等待节点超过默认 2 秒后获得资源保护。能装入时先授予它；
   否则后续回填只能用扣除其需求后的余量。资源逐步释放后它先获得所需预算，不能
   被持续的小请求无限插队。CPU 受保护时，CPU 0 且内存有余量的 IO 仍可推进。
   正在运行的节点不抢占。

这是未知算子耗时下兼顾利用率与不饥饿的在线策略，不承诺所有负载的全局最优完成
时间。保护阶段可能保留空闲资源，这是保证大节点获得预算的取舍。
运行节点必须能结束、超时或取消；所有适配执行器离线、声明超过硬件容量，或所有槽位
被长时间运行的文件占用时不能保证立即执行。文件固定在执行器内，不能迁移个别算子
来使用其他节点的空闲资源。

## 执行器与观测

```bash
dataflow worker --slots 4 --cpu 4 --memory-mb 2048 \
  --runner-dag-workers 4 --runner-map-workers 4 \
  --runner-io-workers 16 --runner-io-map-workers 16 --io-concurrency 32 \
  --resource-aging-seconds 2
```

这些是部署级物理预算和线程上限，只在执行器配置一次。Helm 对应
worker.resourceAgingSeconds，设为 0 可立即保护队头。增加槽位增加常驻进程、缓存与
线程总量。context.map 选择对应共享页块池，CPU 在途工作受 ceil(resources.cpu)
约束，IO 受 IO 页块池限制；均受 max_workers/max_pending 限制。必须消费或关闭
迭代器，禁止嵌套。CPU 声明不会自动把普通函数拆为多线程，原生 BLAS/OpenMP 等
线程需统一限制。声明是准入预算，硬限制仍由 Pod/cgroup 提供。

资源库在 work-root 的本地磁盘/emptyDir，不能放共享 PVC。每进程连接有明确生命周期。
停止、失租、崩溃后先终止运行进程组，再回收该代次所有请求及保护，补建槽位。
Pod 故障由管控租约从头重试，无断点续跑；不能提前释放仍在执行的旧线程预算。

Attempt.progress.scheduler / 活跃 worker.scheduler 记录预留、运行/等待数、共享会话
峰值、policy=fair_backfill_with_aging、老化阈值和最长等待。它们是整个执行器的值，
不能把不同任务中的同一快照相加。步骤有独立等待时间与时长；usage.peak_rss_bytes
仍是运行进程生命周期 RSS 峰值。成功结果 runtime.peak_resources 单独记录本次
Attempt 的算子预留峰值；它与共享会话峰值、实际 RSS 含义不同。

## 兼容与验证

0.5.0 继续使用管控数据库 schema 1。旧任务 CPU/内存只在规范化未配置算子的资源时
作兼容默认值，之后移出公开定义。算子模式旧 dag_workers/io_workers 不再限制新
执行器。历史 scheduling=task 持久任务及原版本执行器保持旧协议，自动/手动重试
保持原模式；前端复制/导入到新编排器转换为算子配置，应确认运行版本对应支持该协议
的镜像。升级镜像和 runtimeVersion 为 0.5.0。

回归覆盖四节点自动并行、峰值与放置门槛的区别、队头大内存回填、持续小请求下的老
请求保护、资源份额公平、池宽度、取消、崩溃回收、旧任务幂等与重试。
完整业务基准见 [performance.md](performance.md)。
