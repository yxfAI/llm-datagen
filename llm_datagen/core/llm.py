"""LLM 接口：定义大模型协议"""
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Tuple, Optional


class ILLMClient(ABC):
    """
    基础 LLM 客户端接口：定义单次调用、批量调用、Token 统计的标准
    """
    
    @abstractmethod
    def call(self, prompt: str, **kwargs) -> Tuple[str, Dict[str, int]]:
        """
        单次调用
        
        Args:
            prompt: 提示词
            **kwargs: 其他参数（如 temperature, max_tokens 等）
        
        Returns:
            (content, usage) 元组，usage 格式: {"prompt_tokens": ..., "completion_tokens": ..., "total_tokens": ...}
        """
        pass


class IBatchLLMClient(ILLMClient):
    """
    批量并发 LLM 客户端接口：自动管理线程池，保证输出顺序与输入一致
    """
    
    @abstractmethod
    def call_batch(self, prompts: List[str], max_workers: int = 10, **kwargs) -> List[Tuple[str, Dict[str, int]]]:
        """
        批量并发调用
        
        Args:
            prompts: 提示词列表
            max_workers: 最大并发数
            **kwargs: 其他参数
        
        Returns:
            [(content, usage), ...] 列表，顺序与输入一致
        """
        pass


class IModelContainer(ABC):
    """
    模型池管理器接口：支持多模型注册、负载均衡和故障转移
    """
    
    @abstractmethod
    def register(
        self,
        name: str,
        model: str,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        default_params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Any:
        """
        注册模型（万能接口）
        
        Args:
            name: 模型池名称
            model: 模型名称
            base_url: API 基础 URL（可选）
            api_key: API 密钥（可选）
            default_params: 默认调用参数（如 temperature, max_tokens 等）
            **kwargs: 其他参数（用于扩展，不知道会传入什么）
        
        Returns:
            模型池实例
        """
        pass
    
    @abstractmethod
    def get(self, name: str) -> ILLMClient:
        """
        获取模型客户端
        
        Args:
            name: 模型池名称
        
        Returns:
            ILLMClient 实例
        """
        pass
