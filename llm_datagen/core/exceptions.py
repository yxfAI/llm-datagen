"""DataGen 核心异常定义"""


class DataGenError(Exception):
    """DataGen 核心异常基类"""
    pass


class CancelledError(DataGenError):
    """任务或节点执行被取消的异常"""
    pass


class BusError(DataGenError):
    """总线相关错误"""
    pass


class NodeError(DataGenError):
    """节点执行错误"""
    pass


class OperatorError(DataGenError):
    """算子执行错误"""
    pass


class LLMError(DataGenError):
    """LLM 调用错误"""
    pass
