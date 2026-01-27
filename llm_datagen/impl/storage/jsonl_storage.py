import os
import json
from typing import Any, List
from llm_datagen.core.storage import IStorage

class JsonlStorage(IStorage):
    """JSONL 文件存储实现，支持物理 EOF 标记"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self._done_file = f"{file_path}.done" # 物理结束标记文件
        self._ensure_dir()

    def _ensure_dir(self):
        dir_path = os.path.dirname(os.path.abspath(self.file_path))
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

    def append(self, items: List[Any]) -> None:
        with open(self.file_path, 'a', encoding='utf-8') as f:
            for item in items:
                # 修复：跳过空项，避免写入空行
                if item is None:
                    continue
                # 修复：确保 item 是有效的字典或可序列化对象
                if isinstance(item, dict) and not item:
                    continue
                f.write(json.dumps(item, ensure_ascii=False) + '\n')

    def read(self, offset: int, limit: int) -> List[Any]:
        results = []
        if not os.path.exists(self.file_path):
            return []
        
        with open(self.file_path, 'r', encoding='utf-8') as f:
            # 跳过 offset 行
            for _ in range(offset):
                if not f.readline():
                    return []
            
            # 读取 limit 行
            for _ in range(limit):
                line = f.readline()
                if not line:
                    break
                try:
                    results.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return results

    def mark_finished(self):
        """核心功能：在磁盘上创建结束标识"""
        with open(self._done_file, 'w', encoding='utf-8') as f:
            f.write("done")

    def is_finished(self) -> bool:
        """核心功能：检测磁盘上的结束标识"""
        return os.path.exists(self._done_file)

    def size(self) -> int:
        if not os.path.exists(self.file_path):
            return 0
        with open(self.file_path, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f)

    def clear(self) -> None:
        """彻底清理，包括物理标记"""
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
        if os.path.exists(self._done_file):
            os.remove(self._done_file)

    def reset_finished(self):
        """核心修复：撕掉旧封条，让流重新激活"""
        if os.path.exists(self._done_file):
            os.remove(self._done_file)
