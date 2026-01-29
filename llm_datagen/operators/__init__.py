"""Operator 实现：通用算子"""
from .llm_operator import GenericLLMOperator
from llm_datagen.core.operators import IOperator, BaseOperator
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
    
    def process_batch(self, items: List[Any], ctx: Any = None) -> List[Any]:
        """
        处理批量数据
        
        Args:
            items: 数据列表
            ctx: 节点上下文（可选）
        
        Returns:
            处理结果列表
        """
        results = []
        for item in items:
            res = self.func(item)
            if res is None:
                continue
            if isinstance(res, list):
                results.extend(res)
            else:
                results.append(res)
        return results


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
    
    def process_batch(self, items: List[Any], ctx: Any = None) -> List[Any]:
        """
        处理批量数据（过滤）
        
        Args:
            items: 数据列表
            ctx: 节点上下文（可选）
        
        Returns:
            经过过滤后的数据列表
        """
        return [item for item in items if self.predicate(item)]
