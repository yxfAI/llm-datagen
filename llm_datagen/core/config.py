from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

@dataclass
class NodeConfig:
    """
    节点配置模型：用于在编排时精确控制单个节点的行为
    """
    batch_size: Optional[int] = None
    parallel_size: Optional[int] = None
    input_uri: Optional[str] = None
    output_uri: Optional[str] = None
    protocol_prefix: Optional[str] = None
    base_path: Optional[str] = None
    
    # 允许存储算子特定的额外配置
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典，过滤掉 None 值，以便进行合并"""
        data = {k: v for k, v in asdict(self).items() if v is not None and k != 'extra'}
        if self.extra:
            data.update(self.extra)
        return data

@dataclass
class WriterConfig:
    """
    写入器配置：控制同步/异步策略及性能参数
    """
    async_mode: bool = False
    queue_size: int = 5000
    flush_batch_size: int = 100
    flush_interval: float = 1.0      # 最大等待刷盘时间（秒）
    retry_interval: float = 0.1      # 退火时间/轮询间隔（秒）
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
