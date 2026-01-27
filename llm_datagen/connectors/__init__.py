"""Connectors: 数据源和数据输出适配器"""
try:
    from .sources.asset_source import AssetSource
    from .sinks.task_jsonl_sink import TaskJsonlSink
    __all__ = [
        "AssetSource",
        "TaskJsonlSink",
    ]
except ImportError:
    # 如果 dataPlat 未安装，connectors 不可用
    __all__ = []
