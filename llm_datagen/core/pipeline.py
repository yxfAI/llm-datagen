from abc import ABC, abstractmethod
from typing import List, Any, Dict
from enum import Enum

class PipelineStatus(Enum):
    """Pipeline 状态枚举"""
    PENDING = "pending"      # 等待执行
    RESUMING = "resuming"    # 正在恢复状态
    RUNNING = "running"      # 正在执行
    CANCELING = "canceling"  # 正在取消
    CANCELED = "canceled"    # 已取消
    COMPLETED = "completed" # 已完成
    FAILED = "failed"       # 执行失败

class IPipeline(ABC):
    """流水线基础接口：仅暴露核心标识与执行入口"""
    
    @property
    @abstractmethod
    def pipeline_id(self) -> str:
        """流水线唯一标识"""
        pass

    @abstractmethod
    def run(self):
        """启动执行"""
        pass


class IRecoverablePipeline(IPipeline, ABC):
    """可恢复流水线接口：增加运行时快照与位点恢复能力"""

    @abstractmethod
    def get_runtime(self) -> Dict[str, Any]:
        """获取全景运行时快照"""
        pass

    @abstractmethod
    def save_runtime(self, file_path: str):
        """将运行时快照持久化"""
        pass

    @abstractmethod
    def resume_from_runtime(self, runtime_data: Dict[str, Any]):
        """从运行时状态恢复"""
        pass

    @abstractmethod
    def resume_from_file(self, file_path: str):
        """从文件加载快照并恢复状态"""
        pass


class ISequentialPipeline(IPipeline, ABC):
    """顺序流水线接口"""
    pass


class IStreamingPipeline(IPipeline, ABC):
    """流式流水线接口"""
    pass


class IRecoverableSequentialPipeline(ISequentialPipeline, IRecoverablePipeline, ABC):
    """可恢复顺序流水线接口"""
    pass


class IRecoverableStreamingPipeline(IStreamingPipeline, IRecoverablePipeline, ABC):
    """可恢复流式流水线接口"""
    pass
