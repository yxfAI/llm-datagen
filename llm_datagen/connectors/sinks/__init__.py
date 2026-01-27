"""数据输出适配器"""
try:
    from .task_jsonl_sink import TaskJsonlSink
    __all__ = ["TaskJsonlSink"]
except ImportError:
    __all__ = []
