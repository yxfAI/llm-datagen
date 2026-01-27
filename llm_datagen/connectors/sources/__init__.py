"""数据源适配器"""
try:
    from .asset_source import AssetSource
    __all__ = ["AssetSource"]
except ImportError:
    __all__ = []
