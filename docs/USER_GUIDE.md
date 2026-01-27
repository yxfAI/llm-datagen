# llm-datagen 详细使用手册 (Comprehensive Specification Manual)

本手册作为 llm-datagen 的权威功能指南，详细定义了从高层管道编排到底层组件操控的全量功能、参数规格及物理行为。

---

## 1. 架构总览 (Architecture Architecture)

llm-datagen 采用“蓝图与执行分离”的三层架构：
*   **Pipeline (编排层)**：定义拓扑逻辑，决定数据的流转方式（顺序/流式/恢复）。
*   **Node (执行层)**：并发执行单元，负责处理位点锚点 (`_i`)、背压监控与算子调用。
*   **Bus (存储层)**：抽象 I/O 总线，负责 URI 寻址及物理格式解析（JSONL/CSV/Memory）。

---

## 2. 核心入口：UnifiedPipeline

`UnifiedPipeline` 是面向用户的“单一事实来源”入口，它封装了逻辑算子到物理节点的映射过程。

### 2.1 构造函数全量参数
| 参数 | 类型 | 描述 |
| :--- | :--- | :--- |
| `operators` | `List[IOperator]` | 逻辑算子序列。框架会自动根据并行度将其包装为 `OperatorNode`。 |
| `input_uri` | `str` | 起始节点输入地址。支持协议前缀：`jsonl://`, `csv://`, `memory://`。 |
| `output_uri` | `str` | 终止节点输出地址。 |
| `streaming` | `bool` | 是否开启全链路流式。若为 `True`，节点间将通过 `StreamBridge` 实时传递数据。 |
| `parallel_size` | `int` | 全局并行度。决定跨批次任务的并发线程数。 |
| `batch_size` | `int` | 单批次处理量。决定每次磁盘读取和算子处理的数据密度。 |
| `writer_config` | `WriterConfig` | 异步写入器配置。详见第 5 节。 |
| `recoverable` | `bool` | 是否开启断点续传。开启后，必须配合持久化存储协议。 |
| `base_path` | `str` | 自动路径推导的基础目录（默认 `"tmp"`）。 |
| `protocol_prefix` | `str` | 中间链路文件名的前缀（默认 `"node_"`）。 |
| `default_protocol`| `str` | 中间链路默认协议（默认 `"jsonl://"`）。 |
| `results_dir` | `str` | 存放任务镜像 (`runtime.json`) 和进度 (`checkpoint.json`) 的目录。 |

---

## 3. 运行控制 (Runtime Control)

### 3.1 `pipeline.create(...)`
创建或更新蓝图。若在构造后调用，可覆盖初始参数。
*   **`node_configs`**: `List[Dict]` 类型。用于对特定的节点进行精细化覆盖。
    *   `output_uri`: 显式指定该节点的输出路径。
    *   `batch_size`: 为特定节点设置不同于全局的批次大小。

### 3.2 `pipeline.resume(pipeline_id)`
从 `results_dir` 中加载指定的 `pipeline_id` 镜像，并自动执行以下物理恢复：
1.  **位点 Seek**：将 Reader 定位到磁盘文件的最后一行。
2.  **协议解封**：自动清理掉未完成节点的 `.done` 标记。
3.  **参数对齐**：自动加载崩溃前的 `writer_config` 及并发设置。

---

## 4. 算子开发规格 (Operator Specification)

所有算子应继承 `BaseOperator` 以获得最佳性能适配。

### 4.1 自动适配逻辑
*   **Single 模式**：实现 `process_item(item, ctx)`。若 `batch_size > 1`，框架会在批次内部开启 `ThreadPoolExecutor` 并发调用。
*   **Batch 模式**：实现 `process_batch(items, ctx)`。开发者手动处理批量逻辑（如聚合 API 请求）。
*   **1:N 爆炸分发**：若返回 `List`，框架执行“平铺输出”，并自动生成层级 ID（如父 ID `100` -> 子 ID `100_0`, `100_1`）。

### 4.2 上下文 `INodeContext` (ctx)
*   `ctx.node_id`: 当前执行节点的 ID。
*   `ctx.report_usage(usage_dict)`: 汇报 Token 消耗或其他指标。
*   `ctx.is_cancelled()`: 检查当前任务是否已被外部终止（用于长耗时算子自救）。

---

## 5. 高性能 I/O：`WriterConfig`

异步批次写入是 llm-datagen 榨干磁盘性能的核心。

| 参数 | 类型 | 描述 |
| :--- | :--- | :--- |
| `async_mode` | `bool` | 是否开启单写者背景线程模式。 |
| `queue_size` | `int` | **传输级背压上限**。缓冲区满时，计算线程将阻塞。 |
| `flush_batch_size`| `int` | 攒够多少条数据执行一次磁盘物理写入。 |
| `flush_interval` | `float` | 强制刷盘的时间间隔（秒），防御最后几条数据的滞留。 |
| `retry_interval` | `float` | 背景线程轮询队列的频率。 |

---

## 6. 底层组件操控 (Advanced Component Level)

在不使用 `UnifiedPipeline` 的情况下，你可以直接操控物理组件实现极高自由度的定制。

### 6.1 `UnifiedNode` 直接实例化
```python
node = UnifiedNode(node_id="custom_node", parallel_size=10, batch_size=20)
node.bind_io(
    input_bus=StreamFactory.create("jsonl://in.jsonl"),
    output_bus=StreamFactory.create("jsonl://out.jsonl")
)
node.set_processor(my_logic_func)
node.run()
```

### 6.2 自定义存储总线
通过继承 `FileStream` 实现自定义协议（如 `oss://`），并注册到选型分发器。

---

## 7. 物理安全与防御机制

### 7.1 早产 EOF (Premature EOF) 防御
在流式管道中，下游 Reader 会在读取无数据时执行“重试退火”：
1.  检测到上游未贴 `.done` 封条。
2.  执行 5 次重试（每次休眠 0.1s）。
3.  防止由于操作系统 I/O 延迟或信号竞态导致的链路过早中断。

### 7.2 镜像级状态快照
`runtime.json` 存储了全景拓扑和所有节点的 URI 配置。这意味着即使你修改了代码中的临时路径，只要 `pipeline_id` 匹配，恢复运行始终以镜像中的路径为准，确保数据连续性。

---

## 8. 最佳实践建议

*   **LLM 处理**：建议 `parallel_size=20+`, `batch_size=5`，开启 `async_mode=True`。
*   **本地清洗**：建议 `parallel_size=CPU核数`, `batch_size=100`。
*   **内存保护**：对于处理千万级任务，务必显式设置 `WriterConfig(queue_size=1000)` 以防止内存爆炸。

---

## 9. 高级进阶功能 (Advanced Features)

... (保持 9.1 监控钩子内容) ...

---

## 10. 裸机模式 (Bare-Metal Mode) 深度解构

当你需要完全自主控制拓扑结构，或不希望框架自动生成任何文件路径时，应使用“裸机模式”。

### 10.1 核心组件职责表
| 组件 | 职责 | 核心行为 |
| :--- | :--- | :--- |
| **`UnifiedNode`** | 物理容器 | 负责实例化引擎、管理批次内部并发、控制位点跳转。 |
| **`StreamFactory`** | I/O 桥梁 | 负责根据 URI 字符串创建物理 Bus (Reader/Writer)。 |
| **`UnifiedNodePipeline`** | 纯节点容器 | **不具备自动焊接能力**。仅负责启动、并发调度及节点级状态保存。 |

### 10.2 纯物理编排流程 (Manual Wiring)
不同于 `UnifiedPipeline` 的自动化，在裸机模式下，你需要手动完成所有物理连接：

```python
from llm_datagen import UnifiedNode, UnifiedNodePipeline, StreamFactory

# 1. 实例化 UnifiedNode (此时它只是一个空壳)
node = UnifiedNode(node_id="n1", parallel_size=5, batch_size=10)

# 2. 手动创建 I/O 总线 (Stream)
# 注意：如果是静态文件，必须手动调用 seal()，否则 Reader 会陷入等待流模式
in_stream = StreamFactory.create("jsonl://my_input.jsonl")
in_stream.seal() 
out_stream = StreamFactory.create("jsonl://my_output.jsonl")

# 3. 物理绑定 (bind_io)
# 这一步建立了 Node 与物理磁盘的联系。UnifiedNodePipeline 无法替你完成这一步。
node.bind_io(input_bus=in_stream, output_bus=out_stream)

# 4. 注入逻辑处理器 (Processor)
# 可以是 lambda 或复杂的函数，参数必须对齐 (items, ctx)
node.set_processor(lambda items, ctx: [{"res": i["data"].upper()} for i in items])

# 5. 使用 UnifiedNodePipeline 驱动
# 它接收已完成物理绑定的 node 列表。它不会自动推导 input_uri 或 output_uri。
pipeline = UnifiedNodePipeline(nodes=[node])
pipeline.create(pipeline_id="bare_metal_task")
pipeline.run()
```

### 10.3 裸机模式的关键差异
*   **无路径推导**：`UnifiedNodePipeline` 不支持全局 `input_uri` 和 `output_uri`。你必须为每一个 Node 单独 `bind_io`。
*   **无协议前缀**：它不会自动补齐 `protocol_prefix` 或 `base_path`。你传入什么 URI，它就直接操作什么路径。
*   **手动解封**：在恢复模式下，若使用底层组件，你可能需要手动控制 `stream.unseal()` 来重新激活已封口的流。

### 10.4 动态逻辑热插拔
在裸机模式下，你可以在 `pipeline.run()` 之前的任何时刻，通过 `node.set_processor()` 动态更换节点的处理逻辑，这在 A/B 测试或动态算子配置场景中非常有用。

---

## 11. 状态流转与物理封条

... (保持 11.1 和 11.2 内容) ...

---

## 12. 物理 ID 系统 (`_i`) 与数据一致性

llm-datagen 在物理层面为每一行数据注入了 `_i` 字段。这是整个框架实现断点续传的“定海神针”。

### 12.1 ID 生成规则
*   **根 ID**：输入文件的行号（从 0 开始）。
*   **分发 ID (1:N)**：若第 `10` 行数据被炸成 3 条，生成的 ID 分别为 `100000`, `100001`, `100002`。
*   **多级透传**：ID 会跨节点透传。即使经过 10 个算子，最终输出的 ID 依然能追溯到原始输入的行号。

### 12.2 恢复时的寻址逻辑
当 Pipeline 恢复时，Reader 不仅仅是看行数，更会校验最后一行数据的 `_i` 值。这确保了即使你在崩溃期间手动删改了中间文件，框架也能通过 ID 校验发现不一致并安全拦截。

---

## 13. 故障处理语义 (Error Handling)

### 13.1 算子抛错行为
*   **默认行为**：若 `process_item` 抛出未捕获异常，当前节点会立即标记为 `FAILED` 状态，并触发 Pipeline 的全局取消信号（Canceling）。
*   **优雅退出**：Pipeline 会等待其他已派发的线程池任务完成后再物理关闭，确保不留下损坏的磁盘块。

### 13.2 信号处理 (`Ctrl+C`)
llm-datagen 内置了信号监听：
*   按下一次 `Ctrl+C`：触发正常关闭逻辑，所有节点会执行 `close()`，刷新缓冲区并存盘位点。
*   连续连击：强制强杀进程（不建议，可能导致最后几行位点丢失）。

---

## 14. 调试、日志与可视化

### 14.1 平台日志 (`platform.log`)
所有核心调度、背压阻塞、流重试信息都会记录在根目录下的 `logs/platform.log` 中。
*   **关键日志点**：搜索 `[Backpressure]` 查看背压触发频率；搜索 `[EOF Retry]` 查看流等待情况。

### 14.2 运行时监控
你可以直接读取 `results_dir/runtime.json`。这个文件是实时的，你可以编写一个简单的脚本每秒轮询该文件，以构建自己的仪表盘（Dashboard）。

---

## 15. 性能调优量化建议 (Optimization Matrix)

| 任务类型 | 推荐配置 | 调优核心 |
| :--- | :--- | :--- |
| **纯文本清洗 (CPU)** | `parallel=CPU核数`, `batch=500` | 减少线程切换开销。 |
| **LLM 异步调用 (I/O)** | `parallel=20~50`, `batch=10` | 增加并发度以抵消 API 网络延迟。 |
| **向量库检索 (Memory)** | `parallel=5`, `batch=200` | 减少序列化/反序列化频率，发挥批量检索优势。 |
| **百万级写盘 (I/O)** | `async_mode=True`, `flush_batch=500` | 利用背景线程聚合写入，减少 `fsync` 调用。 |

---

## 16. 常见陷阱 (The Gotchas)

1.  **静态文件忘记 `seal()`**：在“裸机模式”下，如果输入是文件但没调用 `seal()`，程序会卡在第一步，因为 Reader 以为文件后面还会有动态流进来。
2.  **异步写入下的进度“虚高”**：由于计算和写入分离，你看到的进度是“已计算”的，而不是“已落盘”的。在大批次写入模式下，最后几十条数据的落盘会有 1~2 秒的延迟。
3.  **中间路径被占用**：如果两个 Pipeline 使用同一个 `base_path` 和 `pipeline_id`，它们会互相改写 `runtime.json`，导致恢复逻辑彻底失效。请务必保证 `pipeline_id` 的全局唯一性。
