# llm-datagen 使用指南 (Batch-First & 生产级任务)

本指南旨在帮助开发者快速上手 `llm-datagen` 框架。该框架专为**可恢复、可监控、高性能**的数据处理任务设计。

---

## 1. 核心概念

在开始编写代码前，请理解以下核心概念：

*   **算子 (Operator)**：业务逻辑的最小单元。所有算子均需继承 `BaseOperator` 并实现 `process_batch`。
*   **节点 (Node)**：算子的运行容器。框架会自动将算子包装成节点，处理 I/O、并发和检查点。
*   **流水线 (Pipeline)**：节点的编排者。`UnifiedPipeline` 是推荐的入口，支持自动路径推导和引擎选型。
*   **物理锚点 (`_i`)**：框架管理的行号/索引（从 0 开始）。它是实现“断点续传”的核心资产，**严禁修改**。

---

## 2. 算子开发 (Batch-First)

框架强制要求使用批量处理模式，以获得最佳性能（如 LLM 并行请求）。

### 2.1 基础算子模板
```python
from typing import List, Any, Optional
from llm_datagen.core.operators import BaseOperator

class MyOperator(BaseOperator):
    def process_batch(self, items: List[Any], ctx: Optional[Any] = None) -> List[Any]:
        results = []
        for item in items:
            # 业务逻辑：例如将 text 转换为大写
            item["processed_text"] = item.get("text", "").upper()
            
            # 读取父级 ID (物理锚点)
            parent_id = item.get("_i") 
            results.append(item)
        return results
```

### 2.2 1:N 爆炸 (数据裂变)
如果一个输入项产生多个输出项，请确保手动处理 ID，但**不要覆盖 `_i`**。
```python
class SegmentOperator(BaseOperator):
    def process_batch(self, items: List[Any], ctx: Optional[Any] = None) -> List[Any]:
        outputs = []
        for item in items:
            parent_id = item.get("_i")
            segments = item.get("text", "").split(".")
            for idx, seg in enumerate(segments):
                # 建议通过 parent_id + 偏移量构建子 ID，方便溯源
                outputs.append({
                    "parent_id": parent_id,
                    "sub_idx": idx,
                    "content": seg.strip()
                })
        return outputs
```

---

## 3. 流水线编排 (UnifiedPipeline)

`UnifiedPipeline` 是最推荐的使用方式，它封装了复杂的拓扑焊接逻辑。

### 3.1 快速启动示例
```python
from llm_datagen.impl.pipeline import UnifiedPipeline
from my_operators import MyOperator, SegmentOperator

# 1. 定义算子实例
ops = [
    MyOperator(),
    SegmentOperator()
]

# 2. 声明式定义 Pipeline
pipe = UnifiedPipeline(
    operators=ops,
    input_uri="jsonl:///path/to/input.jsonl",
    output_uri="jsonl:///path/to/output.jsonl",
    batch_size=10,        # 批大小
    parallel_size=5,      # 并行度 (并发线程数)
    streaming=True        # 开启流式模式：节点间并行执行，减少中间存盘开销
)

# 3. 创建并运行
pipe.create(pipeline_id="task_001")
pipe.run()
```

### 3.2 关键参数说明

| 参数 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `operators` | `List` | `[]` | 算子实例列表。 |
| `streaming` | `bool` | `False` | 是否开启流式模式。开启后，节点之间通过内存管道传递，且支持断点恢复。 |
| `batch_size` | `int` | `1` | 全局默认批大小。 |
| `parallel_size` | `int` | `1` | 全局默认并行度。 |
| `results_dir` | `str` | `"tmp/results"` | 存储 `runtime.json` 和检查点的根目录。 |
| `base_path` | `str` | `"tmp"` | 自动生成的中间路径存放位置。 |
| `default_protocol`| `str` | `"jsonl://"` | 中间路径的默认存储协议。 |

---

## 4. 断点续传与恢复

这是 `llm-datagen` 的核心优势。当任务意外中断时，只需通过 `pipeline_id` 即可复活。

### 4.1 恢复代码模板
```python
from llm_datagen.impl.pipeline import UnifiedPipeline

# 必须使用与原始任务相同的 results_dir
pipe = UnifiedPipeline(results_dir="tmp/results")

# 通过 ID 自动加载所有配置和进度
pipe.resume(pipeline_id="task_001")

# 继续运行，框架会自动跳过已完成的部分
pipe.run()
```

### 4.2 恢复原理
1.  **物理镜像**：每次 `create` 或运行过程中，框架都会在 `results_dir/{pipeline_id}/` 下生成 `runtime.json`。
2.  **物理封条**：当一个节点处理完所有输入时，会在其输出 URI 同级目录下生成一个 `.done` 文件。
3.  **位点对齐**：`resume` 时，框架会检查 `.done` 文件。已标记完成的节点会被跳过；未完成的节点会根据物理存储的记录数（物理 ID）自动执行 `Seek` 操作，从中断点继续。

---

## 5. 高级特性

### 5.1 精细化节点配置 (`node_configs`)
您可以为流水线中的特定步骤设置不同的参数：
```python
from llm_datagen.core.config import NodeConfig

# 设置 node_0 (第一个算子) 的并发为 50，其余保持默认
pipe.create(
    pipeline_id="high_perf_task",
    node_configs=[
        NodeConfig(parallel_size=50), # node_0
        NodeConfig(batch_size=1)      # node_1
    ]
)
```

### 5.2 异步写入控制 (`WriterConfig`)
为了防止内存溢出，可以配置背压策略：
```python
from llm_datagen.core.config import WriterConfig

pipe = UnifiedPipeline(
    ...,
    writer_config=WriterConfig(
        max_queue_size=1000,  # 待写入队列最大长度
        batch_size=100        # 累积多少条后执行一次物理写入
    )
)
```

### 5.3 监控钩子 (`Hooks`)
```python
from llm_datagen.core.hooks import DefaultPipelineHooks

class MyMonitor(DefaultPipelineHooks):
    def on_node_finish(self, pipeline_id, node_id):
        print(f"✅ 节点 {node_id} 处理完毕！")

pipe = UnifiedPipeline(..., hooks=MyMonitor())
```

---

## 6. 常见陷阱与注意事项

1.  **不要在 `UnifiedPipeline` 中使用 `memory://`**：
    由于 `UnifiedPipeline` 的核心目标是“可恢复”，而内存流在进程退出后会消失，这会导致恢复逻辑失效。对于生产任务，请始终使用 `jsonl://` 或 `csv://`。
2.  **不要覆盖 `_i`**：
    `_i` 是物理层的唯一指纹。如果您需要业务 ID，请另起字段（如 `id` 或 `uuid`）。
3.  **算子内无状态**：
    在流式模式或多线程下，算子实例会被多处并发调用。请确保 `process_batch` 逻辑是无状态的（或线程安全的）。
4.  **CSV 换行符问题**：
    如果您处理包含换行符的文本并使用 CSV 协议，框架已内置 Pandas 引擎来确保进度统计的准确性，但请确保环境中已安装 `pandas`。
