from typing import Any, List
from llm_datagen.core.storage import IStorage

class MemoryStorage(IStorage):
    """内存存储实现"""
    
    def __init__(self):
        self._data = []

    def append(self, items: List[Any]) -> None:
        self._data.extend(items)

    def read(self, offset: int, limit: int) -> List[Any]:
        if offset >= len(self._data):
            return []
        return self._data[offset : offset + limit]

    def size(self) -> int:
        return len(self._data)

    def clear(self) -> None:
        self._data = []

    def reset_finished(self) -> None:
        pass
