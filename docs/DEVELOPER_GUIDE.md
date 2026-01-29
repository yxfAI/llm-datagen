# llm-datagen 扩展开发指南 (Developer/Extension Guide)

本指南面向架构师与框架贡献者，介绍如何扩展 llm-datagen 的存储协议、执行引擎以及深度钩子系统。

---

## 1. 扩展存储总线 (Custom Bus)

DataGen 采用协议路由模式。要支持一个新存储（如 S3、Redis、SQL），需要实现三个层级的接口：

### 1.1 实现 IReader & IWriter (物理层)
这是最底层的物理操作。
```python
class MyS3Reader(IReader):
    def read(self, batch_size, timeout=None):
        # 实现从 S3 分块下载并剥壳的逻辑
        pass

    def get_current_checkpoint(self):
        # 返回不透明的位点对象（如 S3 VersionId 或 Offset）
        pass
```

### 1.2 实现 IDataStream (契约层)
你需要通过继承**协议证书 (Protocols)** 来告知 Pipeline 你的能力。
```python
from llm_datagen.core.protocols import RecoveryProtocol, StreamingProtocol

class MyS3Stream(BaseStream, RecoveryProtocol):
    """
    通过继承 RecoveryProtocol，Pipeline 在审计阶段会认为此流
    支持断点续传（即支持 Seek 寻址）。
    """
    def get_reader(self, offset=None):
        return MyS3Reader(self.uri, offset)
```

### 1.3 注册到路由器
在 `UnifiedFileStream` 或 `RouterBus` 中注册你的协议头（如 `s3://`）。

---

## 2. 自定义执行引擎 (Custom Node/Pipeline)

如果你需要特殊的调度逻辑（如：基于优先级的并行，或者分布式执行）：

### 2.1 扩展 BaseNode
Node 负责“意图”到“执行”的转换。
*   你可以重写 `_ensure_impl` 来根据参数实例化不同的底层引擎。
*   利用 `NodeContextImpl` 实现与外部的监控闭环。

### 2.2 算子容器逻辑
Node 应通过调用算子的 `process_batch` 方法来处理数据。自 1.1.0 起，框架不再支持单条处理 `process_item` 的自适应包装，所有算子均需遵循批量处理契约。

---

## 3. 深度钩子系统 (Advanced Hooks)

`IPipelineHooks` 是 DataGen 的“观测中心”。你可以通过自定义 Hooks 实现：
*   **企业审计**：将 Token 消耗实时推送至公司财务系统。
*   **实时大屏**：通过 WebSocket 将各节点进度推送到前端可视化界面。
*   **自愈逻辑**：在 `on_error` 中根据错误类型决定是重试、跳过还是报警。

---

## 4. 关键设计模式参考

在扩展 DataGen 时，请遵循以下模式：
1.  **策略工厂模式**：参考 `UnifiedPipeline`。高层组件应作为工厂，根据用户“意图”返回最匹配的底层真身。
2.  **剥壳/装箱原则**：`Reader` 负责把物理格式剥离成纯业务数据，`Writer` 负责把结果装箱回物理格式。这能保证业务算子的绝对纯净。
3.  **报错优于纠正**：如果配置冲突（如内存协议配了恢复模式），应在 `run()` 前通过审计报错，严禁在运行时进行暗箱降级。
