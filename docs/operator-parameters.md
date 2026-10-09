# 提交任务时配置算子参数

算子同时接收两类输入，互不覆盖：

| 来源 | 提交格式 | 算子读取方式 |
| --- | --- | --- |
| 上游执行结果 | `depends_on` 指定依赖，执行时自动收集 | `inputs["上游节点 ID"]` |
| 任务级公共配置 | TaskSpec 顶层 `parameters` | `context.parameters` |
| 当前算子配置 | 对应 `steps[]` 中的 `parameters` | `context.parameters`，同名键覆盖任务级配置 |

任务级配置与节点配置按顶层键合并；嵌套对象整体覆盖，不做递归合并。没有配置的键由算子代码自行提供默认值。每个节点获得独立的配置副本，包括嵌套字典/数组，避免并行节点互相修改配置。上游结果需按只读方式使用。

例如任务设置 `chunk_size=1024, overlap=32`，chunk 节点设置 `chunk_size=256`，则 chunk 算子获取 `chunk_size=256, overlap=32`；上游 parse 的产物仍通过 `inputs["parse"]` 获取。embed 节点也可以设置自己的 `embedding_workers=8`。

参考入库流程将实际使用的切块大小、重叠和模型配置随产物传递，索引版本依据这些实际值生成；节点覆盖配置不会被误记为公共配置。

完整可运行定义见 [configured_ingestion.json](../examples/configured_ingestion.json)，使用项目内置的 UTF-8 文档入库算子与测试向量。初始化演示文件后，可在前端导入 JSON 或直接提交：

```bash
python3 scripts/init-demo.py
docker compose up --build -d
# 管理令牌从 .env 设置到当前环境
set -a
source .env
set +a
curl -sS -X POST http://127.0.0.1:8080/v1/tasks \
  -H "Authorization: Bearer $DATAFLOW_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: configured-ingestion-001' \
  --data-binary @examples/configured_ingestion.json
```

## 前端填写

1. 进入「DAG 编排」，填写输入文件路径。
2. 点击一个节点，在「算子配置参数 JSON（提交时填写）」中填写配置，如 `{"chunk_size":256}`。
3. 公共配置在「重试策略、校验和与全局参数」中的「全局参数 JSON」填写。
4. 提交时会自动应用当前节点参数，不必额外点击「应用」。切换节点也会保存当前节点配置。

配置必须是 JSON 对象，可以包含字符串、数字、布尔值、数组及嵌套对象。框架验证 JSON 对象形态，业务算子负责参数含义和取值检查。输入上游结果与配置同名也不会冲突，因为分别使用 `inputs` 和 `context.parameters`。

## Python SDK

```python
from dataflowcore.sdk import Pipeline

flow = (
    Pipeline("configured-count", parameters={"encoding": "utf-8"})
    .step("read", "dataflowcore.operators:read_text", parameters={"encoding": "utf-8-sig"})
    .step("count", "dataflowcore.operators:word_count", depends_on=["read"])
)
spec = flow.spec("/dataflow/input.txt")
# spec 可交给 Client.submit，公共/节点配置均持久化到任务定义。
```

这里 read 使用 `utf-8-sig`，count 的 `inputs["read"]` 获得 read 的返回结果。算子接口保持 `operator(context, inputs)`；配置通过 `context.parameters.get("参数名", 默认值)` 获取。

配置随任务定义持久化，导出/复制 DAG 时保留。自动整文件重试和手动重试沿用原配置；修改配置后应提交新任务或复制到编排器编辑。已提交任务的配置不随浏览器草稿变化。
