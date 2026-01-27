from abc import ABC, abstractmethod
from typing import Any, List, Optional

class IStorage(ABC):
    """物理存储接口：负责数据的持久化存取"""
    
    @abstractmethod
    def append(self, items: List[Any]) -> None:
        """追加数据"""
        pass

    @abstractmethod
    def read(self, offset: int, limit: int) -> List[Any]:
        """从指定位置读取数据"""
        pass

    @abstractmethod
    def size(self) -> int:
        """获取当前已存储的数据总量"""
        pass

    @abstractmethod
    def clear(self) -> None:
        """清理数据（用于重新运行）"""
        pass

    def mark_finished(self) -> None:
        """设置结束标识（可选实现）"""
        pass

    def is_finished(self) -> bool:
        """检测是否已结束（可选实现）"""
        return False

    def reset_finished(self) -> None:
        """重置结束标识（可选实现）"""
        pass
