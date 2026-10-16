# Python 3.11 与无 GIL 运行时（0.9.0）

框架支持 CPython 3.11+，CI 明确验证 3.11.6 和 3.14t。3.11.6 有 GIL，并非
free-threaded 版本。任务协议、算子接口、参数、资源预算及故障恢复机制相同。
Linux 执行器继续采用固定常驻进程，不逐文件或逐算子创建进程池。

| 行为 | Python 3.11.6 / 普通有 GIL 构建 | Python 3.14t，GIL 关闭 |
| --- | --- | --- |
| 多文件 CPU 并行 | 不同常驻文件进程独立执行 | 常驻进程及槽位内线程 |
| 单文件纯 Python CPU 线程 | 共享该进程的一把 GIL，不能依靠线程获得多核计算 | 可并行执行 Python 字节码 |
| IO / 释放 GIL 的原生计算 | 固定线程池并发，保留原有 context.map 闭包接口 | 固定线程池并发 |
| 默认 slots=auto | 根据执行器 --cpu 预算固定预建，最多256个 | 1个，仍可显式配置更多文件槽位 |
| 默认 runner-dag-workers=auto | 每槽位同时准入1个 CPU 分支 | 每槽位最多8个 CPU 分支，仍受资源预算约束 |
| 显式 --slots N | 固定 N 个文件进程，不自动扩张 | 同左 |
| 缓存与模型 | 同一槽位复用；不同槽位各有自己的副本 | 同左 |

GIL 版本的高吞吐依赖足够的可运行文件。单个文件、单个纯 Python CPU 算子无法
自动利用多核；把线程数调大并不能消除 GIL。需要单文件 CPU 跨进程 map 时，应单独
设计可序列化工作项、进程资源归属和取消回收。本版本保留现有闭包和进程内缓存接口。

算子资源仍按目标环境的实际需求声明。3.11 中纯 Python CPU 步骤通常声明 CPU 1；
原生多线程计算按真实用量声明。CPU 4 的预留不会把普通 Python 函数自动变成四核。
IO 算子及页块保持独立线程池，避免等待 HTTP 时挡住其他就绪 CPU 节点。
有 GIL 时，每槽位默认只准入1个 CPU 分支，把剩余预算留给其他文件；释放 GIL 的
原生算子需要多个 CPU 分支并发时，可显式设置 `--runner-dag-workers 4`。
`context.map` 的计算池仍支持原生计算并发，宽度同时受算子 CPU 声明和固定池上限约束。

## 启动与部署

```bash
uv venv --python 3.11.6 .venv
source .venv/bin/activate
uv pip install -c constraints/production.txt -e '.[postgres]'
dataflow doctor
# 无需 --allow-gil；按 4 CPU 固定预建4个槽位
dataflow worker --cpu 4 --memory-mb 2048 --slots auto
# 模型缓存较重时，显式限制文件进程数量
dataflow worker --cpu 4 --memory-mb 2048 --slots 2
```

`--cpu` 是部署声明的执行器算子预算，不是自动读取的宿主机 CPU 数。`slots=auto`
只在启动时解析一次，不随任务数量扩张，也不表示模型缓存已经包含在内存预算中。
Pod 仍需为解释器、模型副本、线程栈和中间数据预留内存；重模型部署显式选择 slots。

```bash
# 默认镜像仍为3.14.8t；同一Dockerfile可生成3.11.6版本
docker build --build-arg PYTHON_VERSION=3.11.6 -t dataflowcore:0.9.0-py311 .
# Compose 下控制面和执行器一起选择普通 Python
DATAFLOW_PYTHON_VERSION=3.11.6 docker compose up --build -d
```

Compose 示例显式配置2个槽位，仍遵守该部署值。Helm 默认 worker.slots=auto；可设置
`--set worker.slots=2`。为3.11构建业务镜像后，修改 image.tag，继续保持业务依赖统一。
不同 Python/依赖环境若不能互换，应使用不同 pool 或 runtimeVersion，提交时绑定相同值。
旧任务保持原 runtime_version，升级不会把旧队列自动改成另一套业务环境。

## 可见性与约束

`dataflow doctor`、`GET /version` 的 runtime、任务 result.runtime 和 worker.runtime
如实报告 python_version、free_threaded_build、gil_enabled 及 execution_model：

- gil_process_slots：有 GIL，依靠多个文件进程提供多核吞吐。
- free_threaded_threads：无 GIL，可在槽位内执行真正并行的 Python 线程。

前端执行器页显示同一信息、每槽位 CPU 分支并发，并标明槽位由自动选择还是部署显式指定。
默认自动选择的无 GIL 执行器在启动后保持严格检查，算子导入若重新开启 GIL 则失败，
防止运行中悄悄退化。需要部署强制无 GIL 时：

```bash
dataflow doctor --require-free-threading
dataflow worker --require-free-threading
```

3.11 下这些严格命令会拒绝启动。旧 --allow-gil 参数仍兼容，但普通 Python 默认就受支持。

## 吞吐、底噪与验证

两个版本都使用阻塞派发管道、完成事件、定向资源唤醒、批量续租/领取、常驻连接及
合并进度。不会因支持旧 Python 而增加逐任务进程池、短周期磁盘轮询或成功请求日志。
停止、失租和异常仍先终止运行进程组，再回收资源；Pod 故障从文件起点重试。

```bash
python scripts/benchmark_python_runtime.py --repetitions 2 --output runtime.json
python scripts/verify_python_runtime.py runtime.json
python scripts/business_benchmark.py --files 120 --workers 2 --slots 4 --chars 2048 --output business.json
```

CPU 样本为16个文件，每文件400万次纯 Python 整数计算，交替比较1与4个槽位，
排除进程启动/预热时间，逐文件核对结果、单次成功、固定 PID 数和缓存复用。
空闲样本为4槽位、0.2秒通信间隔、8秒窗口。报告保存实际 GIL 状态和源码摘要；
进程身份无法可靠读取时 CPU 使用量为 null，不声称使用率为零。
共享机器的速度比只作为测量记录，不作为所有硬件/模型的性能承诺。

原始样本见 [3.11.6](benchmarks/python-runtime/python311.json)、
[3.14t](benchmarks/python-runtime/python314t.json)；完整文件业务另存
[3.11.6 入库](benchmarks/python-runtime/business311.json)与
[3.14t 入库](benchmarks/python-runtime/business314t.json)。CI 对两个版本分别运行
真实 PostgreSQL、文件入库、备份恢复、浏览器及 K8S Pod 故障验收，并保存对应产物。
发布工作流同时构建默认无GIL镜像和带-py311后缀的3.11.6镜像，在images.json中记录
各自摘要；只有版本tag发布才上传镜像，合并代码不会自动部署。
