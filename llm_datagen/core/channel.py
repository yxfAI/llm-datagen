from abc import ABC, abstractmethod

class IChannel(ABC):
    """通信频道接口：负责节点间的信号同步"""
    
    @abstractmethod
    def notify(self) -> None:
        """发送“有新数据”的信号"""
        pass

    @abstractmethod
    def wait(self, timeout: float = None) -> bool:
        """等待信号，返回是否被唤醒"""
        pass

    @abstractmethod
    def set_eof(self) -> None:
        """标记流结束"""
        pass

    @abstractmethod
    def is_eof(self) -> bool:
        """检查是否已结束"""
        pass

    @abstractmethod
    def reset(self) -> None:
        """重置通道状态，用于断点恢复"""
        pass
