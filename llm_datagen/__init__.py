"""DataGen: 极简高性能流式数据加工库"""
from . import core
from . import impl
from . import operators

# 常用核心组件导出
from .impl import (
    UnifiedNodePipeline,
    UnifiedOperatorPipeline,
    UnifiedPipeline,
    UnifiedNode,
    UnifiedFileStream,
    JsonlStream,
    CsvStream,
    StreamFactory,
    RecoverableStreamFactory
)

from .core.config import (
    NodeConfig,
    WriterConfig
)

from .core.hooks import (
    JsonFileCheckpointHooks,
    DefaultPipelineHooks
)

# 常用算子导出
from .operators import (
    GenericLLMOperator,
    FunctionOperator
)

# 向后兼容导出 (别名处理)
GenericBatchNode = UnifiedNode
GenericBus = UnifiedFileStream
LocalMemoryBus = impl.MemoryStream
JsonlBus = impl.JsonlStream
FileBus = impl.FileStream
SequentialPipeline = impl.SequentialPipeline
StreamingPipeline = impl.StreamingPipeline
RecoverableSequentialPipeline = impl.SequentialPipeline
RecoverableStreamingPipeline = impl.StreamingPipeline

__version__ = "1.0.0"

__all__ = [
    "core",
    "impl",
    "operators",
    # Core Components
    "UnifiedNodePipeline",
    "UnifiedOperatorPipeline",
    "UnifiedPipeline",
    "UnifiedNode",
    "UnifiedFileStream",
    "JsonlStream",
    "CsvStream",
    "StreamFactory",
    "RecoverableStreamFactory",
    "JsonFileCheckpointHooks",
    "DefaultPipelineHooks",
    # Config
    "NodeConfig",
    "WriterConfig",
    # Operators
    "GenericLLMOperator",
    "FunctionOperator",
    # Compatibility Aliases
    "GenericBatchNode",
    "GenericBus",
    "LocalMemoryBus",
    "JsonlBus",
    "FileBus",
    "SequentialPipeline",
    "StreamingPipeline",
    "RecoverableSequentialPipeline",
    "RecoverableStreamingPipeline"
]
