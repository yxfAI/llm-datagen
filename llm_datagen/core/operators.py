from typing import Any, List, Optional, Protocol, runtime_checkable

@runtime_checkable
class IOperator(Protocol):
    """
    算子核心接口。
    自 1.1.0 起，全面转向批量处理模式。
    """
    def process_batch(self, items: List[Any], ctx: Optional[Any] = None) -> List[Any]:
        """
        处理批量数据。
        Args:
            items: 输入数据批次
            ctx: 节点上下文，提供 node_id, report_usage 等能力
        Returns:
            处理后的数据批次（支持 1:1, 1:N 平铺 或 过滤）
        """
        ...

class BaseOperator(IOperator):
    """
    算子基类：Batch-First 模式。
    开发者应主要实现 process_batch 方法。
    """
    def process_batch(self, items: List[Any], ctx: Optional[Any] = None) -> List[Any]:
        """
        处理批量数据。子类必须实现此逻辑。
        默认抛出 NotImplementedError。
        """
        raise NotImplementedError(f"算子 {self.__class__.__name__} 必须实现 process_batch 方法。")
