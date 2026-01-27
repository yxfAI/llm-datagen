"""Operator 实现：通用算子"""
from .llm_operator import GenericLLMOperator
from llm_datagen.core.operators import ISingleOperator, IBatchOperator, BaseOperator
from typing import Any, Callable, List, Dict

__all__ = [
    "GenericLLMOperator",
    "FunctionOperator",
    "FilterOperator",
]


class FunctionOperator(BaseOperator):
    """
    函数算子：将任意 Python 函数包装为算子
    
    支持 1:1, 1:N 以及过滤。
    
    使用示例:
        def my_transform(item):
            return item.upper()
        
        operator = FunctionOperator(my_transform)
    """
    
    def __init__(self, func: Callable[[Any], Any], config: Dict[str, Any] = None, ctx: Any = None):
        self.config = config or {}
        self.func = func
    
    @property
    def operator_type(self) -> str:
        """算子类型标识"""
        return "function"
    
    def process_item(self, item: Any, ctx: Any = None) -> Any:
        """
        处理单项数据
        
        Args:
            item: 单条数据
            ctx: 节点上下文（可选）
        
        Returns:
            处理结果
        """
        return self.func(item)


class FilterOperator(BaseOperator):
    """
    过滤算子：根据指定的函数或条件过滤数据
    
    使用示例:
        def is_valid(item):
            return item.get("status") == "active"
        
        operator = FilterOperator(is_valid)
    """
    
    def __init__(self, predicate: Callable[[Any], bool], config: Dict[str, Any] = None, ctx: Any = None):
        self.config = config or {}
        self.predicate = predicate
    
    @property
    def operator_type(self) -> str:
        """算子类型标识"""
        return "filter"
    
    def process_item(self, item: Any, ctx: Any = None) -> Any:
        """
        处理单项数据（过滤）
        
        Args:
            item: 单条数据
            ctx: 节点上下文（可选）
        
        Returns:
            如果 predicate 返回 True，返回原数据；否则返回 None（会被过滤掉）
        """
        return item if self.predicate(item) else None
