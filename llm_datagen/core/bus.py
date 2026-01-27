from abc import ABC, abstractmethod
from typing import Any, Generator, Optional, List, Dict, Tuple
from .protocols import (
    PersistenceProtocol, RecoveryProtocol, Progress
)


class IReader(ABC):
    """数据读取器接口"""
    
    @abstractmethod
    def read(self, batch_size: int = 1, timeout: Optional[float] = None) -> Generator[Tuple[List[Any], List[Any]], None, None]:
        pass

    @abstractmethod
    def close(self):
        pass


class IWriter(ABC):
    """数据写入器接口"""
    
    @abstractmethod
    def write(self, items: List[Any], anchors: List[Any] = None, commit: bool = True):
        pass

    @abstractmethod
    def close(self):
        pass


class IDataStream(ABC):
    """
    基础数据流接口：代表一个物理 IO 端点。
    """
    @property
    @abstractmethod
    def protocol(self) -> str: pass

    @property
    @abstractmethod
    def uri(self) -> str: pass

    @property
    @abstractmethod
    def protocol_prefix(self) -> str: pass

    @abstractmethod
    def open(self): pass

    @abstractmethod
    def close(self): pass

    @property
    @abstractmethod
    def is_opened(self) -> bool: pass

    @abstractmethod
    def get_reader(self, progress: Any = None) -> IReader: pass

    @abstractmethod
    def get_writer(self, options: Dict[str, Any] = None) -> IWriter: pass



class IStandardStream(IDataStream, PersistenceProtocol, RecoveryProtocol, ABC):
    """标准数据流接口：具备基础读写与位点能力"""
    pass


class IRecoverableStream(IStandardStream, ABC):
    """
    可恢复数据流接口：在标准流基础上增加运行时快照能力。
    """
   
    @abstractmethod
    def get_runtime(self) -> Dict[str, Any]: pass

    @abstractmethod
    def save_runtime(self, file_path: str) -> None: pass

    @abstractmethod
    def resume_from_runtime(self, runtime_data: Dict[str, Any]) -> None: pass
