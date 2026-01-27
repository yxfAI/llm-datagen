from abc import ABC, abstractmethod
from typing import Any, Optional

class IResumable(ABC):
    """可恢复能力：支持断点续传的组件"""
    
    @abstractmethod
    def get_checkpoint(self) -> Any:
        """获取当前状态的检查点（不透明对象）"""
        pass

    @abstractmethod
    def seek(self, checkpoint: Any):
        """恢复到指定的检查点"""
        pass

class IStreamable(ABC):
    """流式处理能力：支持持续产生或消费数据的组件"""
    
    @property
    @abstractmethod
    def is_eof(self) -> bool:
        """是否已到达流末尾"""
        pass

class ITransactional(ABC):
    """事务能力：支持提交与回滚的组件"""
    
    @abstractmethod
    def commit(self):
        """提交更改"""
        pass

    @abstractmethod
    def rollback(self):
        """撤销未提交的更改"""
        pass
