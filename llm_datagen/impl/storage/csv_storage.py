import os
import pandas as pd
from typing import List, Any
from llm_datagen.core.storage import IStorage

class CsvStorage(IStorage):
    """
    基于 Pandas 实现的 CSV 存储
    能够正确处理单元格内换行符，确保进度统计准确。
    """
    def __init__(self, file_path: str, delimiter: str = ','):
        self.file_path = file_path
        self.delimiter = delimiter
        self._done_file = f"{file_path}.done"
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)

    def append(self, items: List[Any]) -> None:
        if not items:
            return
        
        # 确保数据格式统一为 DataFrame
        valid_items = [item if isinstance(item, dict) else {"data": item} for item in items]
        df = pd.DataFrame(valid_items)
        
        file_exists = os.path.exists(self.file_path)
        
        # 使用 pandas 的追加模式
        # 如果文件不存在，写入表头；如果已存在，则不写表头
        df.to_csv(
            self.file_path, 
            mode='a', 
            index=False, 
            header=not file_exists, 
            sep=self.delimiter,
            encoding='utf-8'
        )

    def read(self, offset: int, limit: int) -> List[Any]:
        if not os.path.exists(self.file_path):
            return []
            
        try:
            # 使用 pandas 的分块读取功能
            # skiprows 处理：跳过前 offset 行数据（注意：pandas 的 skiprows 包含表头处理逻辑）
            # 我们通过指定 header=0 并配合 skiprows 来精确定位
            df = pd.read_csv(
                self.file_path,
                sep=self.delimiter,
                skiprows=range(1, offset + 1) if offset > 0 else None,
                nrows=limit,
                encoding='utf-8'
            )
            # 处理 NaN 值，将其转换为 None 保持与原有字典结构一致
            return df.where(pd.notnull(df), None).to_dict('records')
        except pd.errors.EmptyDataError:
            return []
        except Exception:
            return []

    def size(self) -> int:
        """
        利用 Pandas 引擎准确统计物理记录数（无视单元格内换行）
        """
        if not os.path.exists(self.file_path):
            return 0
        try:
            # 性能优化：只读取第一列，不加载全量数据
            # 这样既能识别转义换行符，又避免了内存爆炸
            df_iter = pd.read_csv(
                self.file_path, 
                sep=self.delimiter, 
                usecols=[0], 
                chunksize=10000, 
                encoding='utf-8'
            )
            return sum(len(chunk) for chunk in df_iter)
        except Exception:
            return 0

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
