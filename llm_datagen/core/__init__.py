"""DataGen 核心接口层：定义整个库的扩展边界"""
from .protocols import (
    DataItem,
    DataBatch,
    Progress,
    PersistenceProtocol,
    RecoveryProtocol,
    StreamingProtocol,
    BatchProtocol
)
from .bus import (
    IReader,
    IWriter,
    IDataStream,
    IStandardStream,
    IRecoverableStream
)
from .node import INode, INodeContext, IBatchNode, IRecoverableNode
from .pipeline import (
    IPipeline,
    ISequentialPipeline,
    IStreamingPipeline,
    IRecoverableSequentialPipeline,
    IRecoverableStreamingPipeline
)
from .operators import IOperator
from .llm import ILLMClient, IBatchLLMClient, IModelContainer
from .exceptions import DataGenError, CancelledError, BusError, NodeError, OperatorError, LLMError
from .runtime import IRuntime, PipelineRuntimeData
from .hooks import IPipelineHooks, PipelineHooksAdapter

__all__ = [
    # Protocols & Types
    "DataItem",
    "DataBatch",
    "Progress",
    "PersistenceProtocol",
    "RecoveryProtocol",
    "StreamingProtocol",
    "BatchProtocol",
    
    # Bus (Stream)
    "IReader",
    "IWriter",
    "IDataStream",
    "IStandardStream",
    "IRecoverableStream",
    
    # Node
    "INode",
    "INodeContext",
    "IBatchNode",
    "IRecoverableNode",
    
    # Pipeline
    "IPipeline",
    "ISequentialPipeline",
    "IStreamingPipeline",
    "IRecoverableSequentialPipeline",
    "IRecoverableStreamingPipeline",
    
    # Operators
    "IOperator",
    "BaseOperator",
    
    # Others
    "ILLMClient",
    "IBatchLLMClient",
    "IModelContainer",
    "DataGenError",
    "CancelledError",
    "BusError",
    "NodeError",
    "OperatorError",
    "LLMError",
    "IRuntime",
    "PipelineRuntimeData",
    "IPipelineHooks",
    "PipelineHooksAdapter",
]
