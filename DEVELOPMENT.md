# llm-datagen 开发与架构文档

## 0. 为什么需要 llm-datagen？(The Hook)

在构建大规模 LLM 数据加工流水线时，开发者通常会面临三个“死亡陷阱”：
1.  **计算资源浪费**：昂贵的 API 调用因为网络波动或程序崩溃而重跑，导致 Token 费用翻倍。
2.  **内存撑爆 (OOM)**：上游读得太快，下游处理太慢，中间缓冲区积压百万级数据，瞬间击垮进程。
3.  **磁盘 I/O 锁死**：高并发计算线程同时竞争文件写入，导致 CPU 闲置，I/O 成为吞吐量天花板。

**llm-datagen 为此而生。** 它不仅是一个 Pipeline 库，更是一个**工业级的流式数据加工厂**。它通过“最多一次”语义保护你的钱包，通过“传输级背压”保护你的内存，通过“单写者异步总线”榨干磁盘性能。

---

## 1. 整体生态定位

llm-datagen 在整个数据处理生态中处于**最底层的算力引擎**位置。它向上支撑编排框架，向下直接驱动物理 I/O。

```text
┌─────────────────────────────────────────────────────────┐
│  Web 层 (待实现) : Web UI、可视化监控、API 服务             │
├─────────────────────────────────────────────────────────┤
│  CLI 层 (待实现) : 命令行工具、任务提交、配置管理           │
├─────────────────────────────────────────────────────────┤
│  逻辑层 (待实现) : 领域特定算子、业务清洗流水线             │
├─────────────────────────────────────────────────────────┤
│  DataTask 层 (框架层): 任务调度、DAG 管理、资产注册         │
├─────────────────────────────────────────────────────────┤
│  llm-datagen 层 (库层) ⭐ : Pipeline、Node、Bus、Operator      │
└─────────────────────────────────────────────────────────┘
```

*   **llm-datagen (本库)**：执行引擎库，负责数据处理的原子操作。它的定位是：**极简、高性能、可恢复的流式数据加工厂。**
*   **DataTask (上层)**：编排框架，负责任务调度、资产管理、DAG 管理。上层逻辑层、CLI 层、服务层基于 DataTask 构建业务应用。

---

## 2. 核心设计原则 (Design Philosophy)

### 2.1 引擎与逻辑彻底解耦 (Engine-Logic Decoupling)
*   **Node 是躯干，Operator 是灵魂**：Node 负责处理生命周期、并发调度、I/O 绑定及断点位点；Operator 仅负责纯粹的业务计算。
*   **自适应容器 (Adaptive Containers)**：`OperatorNode` 具备动态能力探测。无论算子实现的是 `process_item` 还是 `process_batch`，容器都会自动适配。对于单条算子，容器会自动在批次内部开启多线程并发，实现计算效率最大化。

### 2.2 路径自动焊接 (Path Auto-Welding)
llm-datagen 采用 **“意图驱动”** 的路径解析策略：
*   **P1 (最高)**：Node 显式指定 `input_uri` / `output_uri`（用于复杂拓扑）。
*   **P2 (次高)**：Pipeline 全局 `input_uri` / `output_uri`（自动绑定首尾节点）。
*   **P3 (最低)**：自动生成中间路径（基于 `base_path` + `protocol_prefix` + `node_id`）。
*   **协议中立**：Bus 统一按 URI 协议分发（`jsonl://`, `csv://`, `memory://`），上层无感知。

### 2.3 最多一次 (At-most-once) 语义
*   为了保护昂贵的 LLM 调用，llm-datagen 默认采用 **“派发即视为消耗”** 的策略。
*   即使系统在中途崩溃，已派发给线程池的任务不会在恢复后重跑，宁可丢掉少量当前批次数据，也要确保不重复支付 Token 费用。

---

## 3. llm-datagen 架构体系 (Library Architecture)

### 3.1 核心定义层 (llm_datagen.core) - 定义契约
| 模块 | 核心契约 | 职责 |
| :--- | :--- | :--- |
| **Pipeline** | `IPipeline` | 拓扑规划、生命周期控制、状态机管理。 |
| **Node** | `INode` | 环境激活 (Open) -> 循环处理 (Run) -> 物理结项 (Close)。 |
| **Bus** | `IPipelineBus` | URI 寻址、Reader/Writer 实例化、中间路径自愈。 |
| **Operator** | `BaseOperator` | 提供 `process_item` 和 `process_batch` 的双向自适应适配。 |
| **Context** | `INodeContext` | 运行时身份工牌（pid/nid）、进度汇报、Token 审计、取消信号。 |

### 3.2 工业实现层 (llm_datagen.impl) - 动力源泉
#### A. 统一选型引擎 (The Unified Tier)
*   **`UnifiedOperatorPipeline`**：llm-datagen 的头等舱入口。它负责将逻辑算子序列映射为物理节点，并根据 `streaming` 参数自动切换 **流式执行引擎 (`StreamingPipeline`)** 或 **顺序执行引擎 (`SequentialPipeline`)**。
*   **`UnifiedNode`**：根据 `parallel_size` 自动切换 **单线程同步引擎 (`BatchNode`)** 或 **多线程并行引擎 (`ParallelBatchNode`)**。

#### B. 高性能总线 (I/O Bus)
*   **`Asynchronous Batch Writer`**：采用单写者模式。背景线程负责聚合写入，计算线程只需将数据推入缓冲区，彻底消除了高并发下的磁盘锁竞争。
*   **`StreamBridge`**：流式管道的核心“水管”，内置了 **“零进度退火重试”** 机制，完美解决了跨进程/跨线程场景下的“早产 EOF”竞态问题。

---

## 4. 特性实现深度解析

### 4.1 传输级背压控制 (Backpressure)
llm-datagen 通过双层防线下钻解决 OOM 问题：
1.  **算子层**：通过 `Semaphore` 限制派发中的任务数为 `parallel * 2`。
2.  **传输层**：异步写入器通过 `queue_size` 硬上限阻塞生产者。当磁盘写入速度跟不上计算速度时，上游会自动降速。

### 4.2 物理封条机制 (The .done Seal)
*   每个节点处理完成后，Bus 会在目录下生成一个 `.done` 标记。
*   **流式防御**：下游节点在读取时，若发现 `.done` 标记且缓冲区已空，才视为物理流结束。这确保了最后几行数据不会因信号延迟而被截断。

### 4.3 路径自愈与协议转发
*   `UnifiedFileStream` 会自动根据后缀补全协议，或根据协议补全后缀。
*   例如：`input.jsonl` 会被自动识别并分发给 `JsonlStream` 处理。

---

## 5. 开发者选型路线 (Complexity Ladder)

llm-datagen 支持根据业务复杂度“丝滑切换”运行模式：

1.  **初级：单线程验证 (Sequential Mode)**
    *   **配置**：`parallel_size=1`, `streaming=False`
    *   **场景**：调试算子逻辑，处理小规模本地数据。
    *   **协议**：建议 `memory://` 或 `jsonl://`。
2.  **中级：并行批处理 (Parallel Batch Mode)**
    *   **配置**：`parallel_size=N`, `batch_size=M`
    *   **场景**：利用多核 CPU 加速纯本地计算逻辑。
    *   **协议**：建议 `jsonl://` 开启断点续传。
3.  **高级：全链路流式 (Streaming Mode)**
    *   **配置**：`streaming=True`, `parallel_size=N`
    *   **场景**：上下游节点协同工作，消除等待间隔。利用 `process_item` 的**批次内线程池并发**压榨 I/O。
    *   **协议**：支持 `csv://` 或 `jsonl://`，内置“零进度退火”防御早产 EOF。
4.  **旗舰：工业级生产 (Recoverable & Async Mode)**
    *   **配置**：`recoverable=True` + `WriterConfig(async_mode=True, queue_size=K)`
    *   **场景**：处理数百万级 LLM 数据。具备断点续传能力，利用**单写者异步总线**消除 I/O 瓶颈，并开启**传输级背压**彻底终结 OOM。

---

## 6. 状态流转与生命周期

| 动作阶段 | Pipeline 状态 | Node 状态 | 关键行为 |
| :--- | :--- | :--- | :--- |
| **创建 (Create)** | `PENDING` | `PENDING` | 静态蓝图映射，路径优先级解析。 |
| **恢复 (Resume)** | `RESUMING` | `RESUMING` | 加载 `runtime.json`，寻找物理位点（Seek）。 |
| **激活 (Open)** | `RUNNING` | `RUNNING` | 实例化 Reader/Writer，准备资源。 |
| **执行 (Run)** | `RUNNING` | `RUNNING` | 任务调度、背压控制、进度汇报。 |
| **结项 (Close)** | `COMPLETED` | `COMPLETED` | 刷盘（Flush）、贴上 `.done` 物理封条。 |

---

## 7. 结语

llm-datagen 的架构核心在于 **“无状态 Bus + 有状态 Node + 蓝图化 Pipeline”** 的三层解耦。开发者只需要关注 **URI (在哪里读写)** 和 **Operator (做什么计算)**，剩下的并发、恢复、背压、协议分发均由框架自动化处理。
