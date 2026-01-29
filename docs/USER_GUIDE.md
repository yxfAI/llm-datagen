# llm-datagen 详细使用手册 (Comprehensive User Guide)

本手册基于 `llm-datagen` 的最新架构实现，详细定义了从高层编排到底层物理控制的全量功能。

---

## 1. 快速上手

### 1.1 定义业务算子
继承 `BaseOperator` 并实现 `process_batch`。框架保证传入的 `items` 已剥离物理包络。

```python
from llm_datagen import BaseOperator

class MyTransform(BaseOperator):
    def process_batch(self, items, ctx=None):
        for item in items:
            item["processed"] = True
        return items
```

### 1.2 运行流水线
```python
from llm_datagen import UnifiedPipeline

pipeline = UnifiedPipeline(
    operators=[MyTransform()],
    input_uri="jsonl://input.jsonl",
    output_uri="jsonl://output.jsonl"
)
pipeline.create(pipeline_id="quick_start_v1")
pipeline.run()
```

---

## 2. 核心入口：UnifiedPipeline 全量规格

`UnifiedPipeline` 是流水线的单一配置入口，其构造函数封装了所有核心调度参数。

### 2.1 构造函数参数 (`__init__`)
| 参数 | 类型 | 默认值 | 描述 |
| :--- | :--- | :--- | :--- |
| `operators` | `List[BaseOperator]` | 必填 | 逻辑算子实例序列。 |
| `input_uri` | `str` | 必填 | 起始节点输入地址。支持 `jsonl://`, `csv://`。 |
| `output_uri` | `str` | 必填 | 终止节点输出地址。 |
| `streaming` | `bool` | `False` | 是否开启全链路流式。若为 `True`，节点间通过内存桥接并行传输。 |
| `batch_size` | `int` | `1` | 全局单批次处理量。决定 I/O 密度。 |
| `parallel_size` | `int` | `1` | 全局并行度。决定算子执行的并发线程数。 |
| `writer_config` | `WriterConfig` | `None` | 异步写入配置。详见第 4 节。 |
| `results_dir` | `str` | `"tmp/results"` | 存储 `runtime.json` 和检查点的根目录。 |
| `base_path` | `str` | `"tmp"` | 自动焊接中间路径的基础目录。 |
| `protocol_prefix`| `str` | `""` | 逻辑 URI 前缀，用于在分布式环境下注入命名空间。 |
| `hooks` | `IPipelineHooks` | `None` | 监控钩子。可监听进度、Token 消耗、错误等。 |

### 2.2 运行控制方法
*   **`create(pipeline_id, ...)`**: 初始化蓝图。
    *   支持通过 `node_configs: List[NodeConfig]` 对特定节点进行精细化覆盖（例如：全局并发 10，但针对 LLM 算子单独设置并发 50）。
*   **`resume(pipeline_id)`**: 从 `results_dir` 中加载物理镜像并复活任务。
*   **`run()`**: 启动引擎执行。如果是流式模式，会并行启动所有节点；如果是顺序模式，则逐个运行。

---

## 3. 拓扑焊接与中间路径

`UnifiedPipeline` 具备“自动焊接”能力。当你提供 `operators=[OpA, OpB]` 时，框架会自动生成物理拓扑：
`InputNode` -> `node_0 (OpA)` -> `node_1 (OpB)` -> `OutputNode`

### 3.1 路径推导公式
中间节点的路径会基于 `pipeline_id` 自动生成：
`{default_protocol}{pipeline_id}/{node_id}{extension}`
例如：`jsonl://my_task/node_0.jsonl`

---

## 4. 高性能 I/O 与背压 (WriterConfig)

异步批次写入是榨干磁盘性能的核心。

```python
from llm_datagen import WriterConfig

writer_cfg = WriterConfig(
    async_mode=True,      # 开启独立后台写入线程，业务线程不阻塞
    queue_size=5000,      # 内存背压阈值。缓冲区满时，算子线程将自动暂停读取
    flush_batch_size=100, # 攒够 100 条执行一次磁盘写入
    flush_interval=1.0    # 强制刷盘间隔（秒）
)
```

---

## 5. 物理 ID 系统 (`_i`) 与数据追溯

框架为每条数据注入 `_i` 字段（物理行号），作为断点续传的**唯一真理锚点**。

### 5.1 只读契约
*   **可以读取**：用于 1:N 场景下的父级关联。
*   **严禁修改**：篡改 `_i` 会导致 `resume` 寻址失效，产生数据丢失或重复。

---

## 6. 生产环境运维模板

### 6.1 稳健的“重跑与恢复”切换逻辑
```python
import os
from llm_datagen import UnifiedPipeline

def start_task(pid, ops, input_uri, output_uri):
    pipe = UnifiedPipeline(ops, input_uri, output_uri)
    
    # 检查物理镜像是否存在
    runtime_path = os.path.join("tmp/results", pid, "runtime.json")
    
    if os.path.exists(runtime_path):
        print(f"♻️  检测到历史记录，执行断点续传: {pid}")
        pipe.resume(pid)
    else:
        print(f"🆕 开始新任务: {pid}")
        pipe.create(pid)
        
    pipe.run()
```

### 6.2 常见陷阱
1.  **忘记实例化**：`operators=[MyOp]` 是错误的，必须是 `operators=[MyOp()]`。
2.  **ID 冲突**：不同的业务逻辑若使用同一个 `pipeline_id`，会互相覆盖 `runtime.json`。
3.  **封条机制**：在“裸机模式”下手动操作 `UnifiedNode` 时，必须显式调用 `stream.seal()` 告知下游 Reader 静态文件已读取完毕。
