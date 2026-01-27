"""
DataGen 核心能力接口定义。
这些类作为能力标签（Mixins），用于在接口层声明组件具备哪些特殊能力。
不再使用注解（Decorators）或注册表逻辑，回归纯粹的接口继承与类型检查。
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List

# 明确数据结构 (Type Aliases for Clarity)
DataItem = Any                # 原始数据项 (可以是 Dict, String, 等)
DataBatch = List[DataItem]     # 原生数据列表
Progress = Any              # 进度标识 (如 offset 或 anchor_id)

class PersistenceProtocol(ABC):
    """持久化能力：组件具备物理落盘与清理能力"""
    @abstractmethod
    def clear_data(self) -> None: pass


class RecoveryProtocol(ABC):
    """恢复能力：组件具备进度感知与位点跳转能力"""
    @abstractmethod
    def get_current_progress(self) -> Any:
        pass


class StreamingProtocol(ABC):
    """流式能力：组件具备信号传递、非阻塞读取与超时退火机制"""
    pass


class BatchProtocol(ABC):
    """批处理能力：组件具备成批读写与处理能力"""
    pass
