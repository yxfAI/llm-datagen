import csv
import os
from typing import List, Any
from llm_datagen.core.storage import IStorage

class CsvStorage(IStorage):
    """
    CSV 存储实现
    注意：CSV 格式通常要求所有行的列结构保持一致。
    """
    def __init__(self, file_path: str, delimiter: str = ','):
        self.file_path = file_path
        self.delimiter = delimiter
        self._done_file = f"{file_path}.done"
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)

    def append(self, items: List[Any]) -> None:
        if not items:
            return
        
        file_exists = os.path.exists(self.file_path)
        # 确保所有 items 都是字典
        valid_items = [item if isinstance(item, dict) else {"data": item} for item in items]
        
        with open(self.file_path, 'a', newline='', encoding='utf-8') as f:
            # 以第一条数据的 key 作为表头
            fieldnames = valid_items[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=self.delimiter)
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerows(valid_items)

    def read(self, offset: int, limit: int) -> List[Any]:
        results = []
        if not os.path.exists(self.file_path):
            return []
            
        with open(self.file_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            # 跳过位点
            count = 0
            for row in reader:
                if count < offset:
                    count += 1
                    continue
                results.append(row)
                count += 1
                if len(results) >= limit:
                    break
        return results

    def size(self) -> int:
        if not os.path.exists(self.file_path):
            return 0
        with open(self.file_path, 'r', newline='', encoding='utf-8') as f:
            # 减去表头行
            return max(0, sum(1 for _ in f) - 1)

    def clear(self) -> None:
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
        if os.path.exists(self._done_file):
            os.remove(self._done_file)

    def reset_finished(self):
        """核心修复：撕掉旧封条，让流重新激活"""
        if os.path.exists(self._done_file):
            os.remove(self._done_file)

    def mark_finished(self):
        with open(self._done_file, 'w', encoding='utf-8') as f:
            f.write("done")

    def is_finished(self) -> bool:
        return os.path.exists(self._done_file)
