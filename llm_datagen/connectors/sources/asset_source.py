from typing import Iterable, Any, List
from dataPlat.core.interfaces import IDataSource

class AssetSource(IDataSource):
    """
    资产源适配器：从平台的 AssetRepository 读取数据。
    实现了 IDataSource 接口，使任务运行器不关心具体的资产加载细节。
    """
    def __init__(self, asset_id: str, asset_svc):
        self.asset_id = asset_id
        self.asset_svc = asset_svc
        self._data = None

    def _get_data(self) -> List[Any]:
        if self._data is None:
            content = self.asset_svc.get_asset_content(self.asset_id)
            # 兼容处理包装格式和扁平格式
            if isinstance(content, dict) and "items" in content:
                self._data = content["items"]
            else:
                self._data = content if isinstance(content, list) else [content]
        return self._data

    def load(self) -> Iterable[Any]:
        """返回可迭代的数据条目"""
        return self._get_data()

    def get_total_count(self) -> int:
        """获取总任务数，供进度条使用"""
        return len(self._get_data())
