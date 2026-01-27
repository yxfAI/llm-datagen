"""DataGen 实现层：提供标准参考实现"""

from .bus.bus import (
    BaseStream,
    MemoryStream,
    FileStream,
    JsonlStream,
    CsvStream,
    UnifiedFileStream,
    StreamFactory,
    RecoverableStreamFactory,
    GenericReader,
    GenericWriter
)

from .node import (
    BaseNode,
    BatchNode,
    ParallelBatchNode,
    BatchOperatorNode,
    SingleOperatorNode,
    ParallelBatchOperatorNode,
    ParallelSingleOperatorNode,
    UnifiedNode,
    InputNode,
    OutputNode
)

from .pipeline import (
    NodePipeline,
    SequentialPipeline,
    StreamingPipeline,
    UnifiedNodePipeline,
    UnifiedOperatorPipeline,
    UnifiedPipeline
)

__all__ = [
    # Bus (Stream)
    "BaseStream",
    "MemoryStream",
    "FileStream",
    "JsonlStream",
    "CsvStream",
    "UnifiedFileStream",
    "StreamFactory",
    "RecoverableStreamFactory",
    "GenericReader",
    "GenericWriter",
    
    # Node
    "BaseNode",
    "BatchNode",
    "ParallelBatchNode",
    "BatchOperatorNode",
    "SingleOperatorNode",
    "ParallelBatchOperatorNode",
    "ParallelSingleOperatorNode",
    "UnifiedNode",
    "InputNode",
    "OutputNode",
    
    # Pipeline
    "NodePipeline",
    "SequentialPipeline",
    "StreamingPipeline",
    "UnifiedNodePipeline",
    "UnifiedOperatorPipeline",
    "UnifiedPipeline"
]
