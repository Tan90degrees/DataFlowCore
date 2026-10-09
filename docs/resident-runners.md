# 常驻运行进程与资源复用（0.3.0）

执行器启动固定 `slots` 个运行子进程。每个槽位同时只执行一个文件的完整 DAG，成功后
继续处理下一文件，不为每次任务重新启动 Python。DAG、页/块线程池属于槽位，跨任务复用。
算子代码与统一依赖预装在镜像中，不做动态安装卸载或依赖隔离。

## 生命周期和隔离

| 对象 | 生命周期 |
| --- | --- |
| Python 进程、导入模块、DAG/页块线程池 | 槽位内复用 |
| `context.resource` 缓存的模型/连接等重资源 | 同一槽位内复用，首次使用时创建 |
| callable class 实例、Context、合并后的参数副本、上游结果 | 每个任务/步骤重新创建 |
| 进度、取消事件、输出目录、日志末尾 | 每次 Attempt 独立 |

复用只发生在同一执行器的同一槽位，不跨进程或节点共享 Python 对象。同名函数内的模块
全局变量也会保留，业务不得将上一任务的参数、Context、输入/输出放进全局状态或资源缓存。
重资源本身可以保留状态，但跨并行 DAG 节点/页块使用时必须线程安全。

失败、取消、租约失效后回收整个运行进程并补建，避免不合作的线程污染下一任务。成功后
还会核对线程池是否清空、有无额外 Python 后台线程及遗留子进程；不满足复用条件时仍
提交成功结果，但替换运行进程。算子须等待所有产物写入完成再返回，子进程必须留在
运行进程组内，不支持脱离管理的 daemon。Linux runner 使用父进程死亡信号和子进程收养。

正常运行默认每槽位处理 100 个文件后回收；`0` 禁用按文件数回收。回收只发生在任务间，
不会中断正常运行的文件。没有基于 RSS 的动态回收或自动并发调节；Pod 内存限额仍可能
导致 OOM，需要按模型占用和并发量配置资源。

## 算子重资源接口

```python
import re


def clean(context, inputs):
    pattern = context.parameters.get("pattern", r"\s+")
    # 名称按 callable 引用隔离；影响资源的配置必须包含在 key 中。
    compiled = context.resource("regex:" + pattern, lambda: re.compile(pattern))
    text = inputs["read"]["text"]
    return {"text": compiled.sub(" ", text)}
```

`context.resource(key, factory)` 在该槽位中对同一算子引用、同一字符串 key 只调用一次
factory，并发首次调用由锁保护。不同节点引用同一算子时可共享该资源。key 长度 1..256，
每槽位最多 128 个缓存条目，防止把每任务 ID 当 key 导致无界增长。模型/服务地址/配置
改变时应改变 key；不要将任务 ID 纳入 key。factory 不得保存任务 Context 或创建
未托管的后台线程/子进程。资源若有 `close()`，正常退出时调用；SIGKILL/OOM 时不保证回调。

现有算子无需更改接口；但在 class `__init__` 中加载模型仍会每任务重复。将重初始化迁入
`context.resource`，让 class 只保存当前任务的轻量状态。独立槽位各自加载一份模型；希望
多个槽位共享设备模型时，使用独立 OCR/Embedding 服务。

## 固定并行预算

```bash
dataflow worker --data-root "$PWD/data" --slots 2 --cpu 4 --memory-mb 2048 \
  --runner-dag-workers 2 --runner-map-workers 4 --runner-max-tasks 100
```

CLI 的 DAG/页块线程上限默认分别为 8；Compose 和 Helm 默认分别为 2、4。线程懒创建，
达到的线程数量可在槽位内复用，最大不超过池上限。每任务实际 DAG 并发取
`min(spec.dag_workers, runner_dag_workers)`；同一槽位所有 `context.map` 共享页块线程池。
每个 map 的在途数量不超过 `min(max_workers, runner_map_workers, max_pending)`，结果
按输入顺序返回，早关迭代器等待已开始的工作结束。必须消费或关闭迭代器。

禁止在 `context.map` 的工作函数内再次调用 `context.map`，避免池内等待造成死锁。
DAG 池和页块池分开，DAG 工作线程等待页块结果不会占用页块池。
CPU/内存声明是准入预算，不是算子硬限额；外部库的 BLAS/OpenMP 线程需单独统一限制。

Helm 对应配置为 `worker.runnerDagWorkers`、`worker.runnerMapWorkers`、
`worker.runnerMaxTasks`。更新 Chart 时必须让这些值与 Pod CPU/内存一起评估。

## 观测和升级

Attempt.progress.runner 和 Task.result.runtime.runner 包含 PID、此前完成文件数和线程上限。
管控台在「本次尝试」显示 PID 与此前完成文件数，可直观看到跨文件复用。
CPU 时间为当前任务的进程 CPU 增量；`usage.peak_rss_bytes` 是常驻进程生命周期峰值，
`rss_scope=runner_lifetime`，不能当作当前文件单独占用或多个任务可相加的值。

0.3.0 仍使用数据库 schema 1，运行环境版本更新为 0.3.0。先排空旧执行器，再更新镜像和
runtimeVersion。已有 0.2.0 排队/重试任务仍绑定旧运行环境；保留旧版本执行器直到完成，
或明确取消后用新版本重新提交。不能在故障重试中自动替换任务运行环境。

回归包含 PID/线程/资源复用、每任务轻量 class 实例、日志/参数隔离、取消一个槽位不影响
另一槽位、运行/空闲进程崩溃补建、按次数回收、遗留子进程和后台线程回收、共享 map
预算、嵌套 map 拒绝，以及原有租约/故障/业务验收。性能复现：

```bash
python scripts/benchmark_resident.py
```

完整 Git 历史包含固定的改造前版本 2f37f71。三轮交错运行同一真实 HTTP 入库负载，记录
耗时、进程数量、成功数、尝试数及源码摘要；原始记录见 `docs/benchmarks/resident`。
