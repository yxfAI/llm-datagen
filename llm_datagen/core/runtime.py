"""运行时接口：定义运行时数据的序列化"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
try:
    from typing_extensions import TypedDict
except ImportError:
    from typing import TypedDict


class IRuntime(ABC):
    """运行时接口：所有运行时对象的基类"""
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典，用于持久化"""
        pass
    
    @abstractmethod
    def from_dict(self, data: Dict[str, Any]):
        """从字典反序列化"""
        pass
    
    @abstractmethod
    def update(self, **kwargs):
        """更新运行时信息"""
        pass


class PipelineRuntimeData(TypedDict, total=False):
    """Pipeline 运行时数据结构（只存储 Pipeline 级别的恢复信息）"""
    pipeline_id: str
    engine_type: str  # "streaming", "sequential"
    current_node_id: Optional[str]  # 当前执行的节点 ID（仅顺序执行需要）
