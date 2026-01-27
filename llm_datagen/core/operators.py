from typing import Any, List, Optional, Protocol, runtime_checkable

@runtime_checkable
class IOperator(Protocol):
    """算子基础接口"""
    def open(self, config: Optional[dict] = None) -> None: ...
    def close(self) -> None: ...

@runtime_checkable
class ISingleOperator(IOperator, Protocol):
    """逐条处理算子接口"""
    def process_item(self, item: Any, ctx: Optional[Any] = None) -> Any:
        """处理单条数据，返回处理后的数据、数据列表或 None（表示过滤）"""
        ...

@runtime_checkable
class IBatchOperator(IOperator, Protocol):
    """批量处理算子接口"""
    def process_batch(self, items: List[Any], ctx: Optional[Any] = None) -> List[Any]:
        """批量处理数据"""
        ...

class BaseOperator(IBatchOperator, ISingleOperator):
    """
    算子基类：提供自动的 1:N 适配与批量/单条转换逻辑。
    开发者只需实现 process_item 或 process_batch 其中之一。
    """
    def open(self, config: Optional[dict] = None) -> None:
        pass

    def close(self) -> None:
        pass

    def process_item(self, item: Any, ctx: Optional[Any] = None) -> Any:
        """
        默认实现：将单条包装为批次执行。
        如果子类只实现了 process_batch，此方法确保单条调用也能工作。
        """
        results = self.process_batch([item], ctx)
        return results[0] if results else None

    def process_batch(self, items: List[Any], ctx: Optional[Any] = None) -> List[Any]:
        """
        默认实现：循环调用 process_item 并执行扁平化。
        如果子类只实现了 process_item，此方法提供高性能（相对）的批量封装。
        支持 1:N 爆炸分发：如果 process_item 返回 list，会自动 extend。
        """
        results = []
        for item in items:
            res = self.process_item(item, ctx)
            if res is None:
                continue
            if isinstance(res, list):
                results.extend(res)
            else:
                results.append(res)
        return results
