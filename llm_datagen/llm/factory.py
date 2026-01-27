"""模型容器实现：万能接口支持"""
from typing import Dict, List, Any, Optional, Type
from llm_datagen.core.llm import IModelContainer, ILLMClient
from llm_datagen.llm.model_pool import (
    ModelConfig,
    BaseModelPool,
    OpenAIPool,
    AzurePool,
    DoubaoPool,
    QwenPool
)


class ModelContainer(IModelContainer):
    """
    模型容器：支持万能接口注册模型
    
    万能接口参数：
    - name: 模型池名称
    - model: 模型名称
    - base_url: API 基础 URL（可选）
    - api_key: API 密钥（可选）
    - default_params: 默认调用参数（可选）
    - **kwargs: 其他参数（用于扩展）
    """
    
    def __init__(self):
        self._pools: Dict[str, BaseModelPool] = {}
        # 注册模型类型与对应类的映射
        self._type_registry: Dict[str, Type[BaseModelPool]] = {
            "doubao": DoubaoPool,
            "qwen": QwenPool,
            "openai": OpenAIPool,
            "azure": AzurePool,
            "gpt": AzurePool,  # 常用别名
        }
    
    def register(
        self,
        name: str,
        model: str,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        default_params: Optional[Dict[str, Any]] = None,
        model_type: Optional[str] = None,
        timeout: Optional[float] = None,
        **kwargs
    ) -> BaseModelPool:
        """
        注册模型（万能接口）
        
        Args:
            name: 模型池名称
            model: 模型名称
            base_url: API 基础 URL（可选）
            api_key: API 密钥（可选）
            default_params: 默认调用参数（如 temperature, max_tokens 等）
            model_type: 模型类型（如 "openai", "azure", "doubao", "qwen"），如果不指定则自动推断
            timeout: 超时时间（秒）
            **kwargs: 其他参数（用于扩展，不知道会传入什么）
                - 可以传入多个配置（如多个 api_key），格式: configs=[{...}, {...}]
                - 可以传入单个配置的额外参数，会被合并到 ModelConfig 的 extra_params 中
        
        Returns:
            模型池实例
        
        示例:
            # 单个配置
            container.register(
                name="my_model",
                model="gpt-4",
                base_url="https://api.openai.com/v1",
                api_key="sk-xxx",
                default_params={"temperature": 0.7}
            )
            
            # 多个配置（轮询）
            container.register(
                name="my_model",
                model="gpt-4",
                base_url="https://api.openai.com/v1",
                configs=[
                    {"api_key": "sk-xxx1"},
                    {"api_key": "sk-xxx2"}
                ]
            )
        """
        # 处理多个配置的情况
        if "configs" in kwargs:
            configs_list = kwargs.pop("configs")
            model_configs = []
            for cfg in configs_list:
                model_configs.append(ModelConfig(
                    model=cfg.get("model", model),
                    api_key=cfg.get("api_key", api_key),
                    base_url=cfg.get("base_url", base_url),
                    call_params=cfg.get("call_params", {}),
                    timeout=cfg.get("timeout", timeout),
                    **{k: v for k, v in cfg.items() if k not in ["model", "api_key", "base_url", "call_params", "timeout"]}
                ))
        else:
            # 单个配置
            model_configs = [ModelConfig(
                model=model,
                api_key=api_key,
                base_url=base_url,
                call_params=kwargs.pop("call_params", {}),
                timeout=timeout,
                **kwargs  # 其余参数保存到 extra_params
            )]
        
        # 自动推断模型类型（如果未指定）
        if model_type is None:
            # 根据 base_url 或 model 名称推断
            if base_url and "azure" in base_url.lower():
                model_type = "azure"
            elif "doubao" in model.lower() or "doubao" in (base_url or "").lower():
                model_type = "doubao"
            elif "qwen" in model.lower() or "qwen" in (base_url or "").lower():
                model_type = "qwen"
            else:
                model_type = "openai"  # 默认使用 OpenAI 协议
        
        if model_type not in self._type_registry:
            raise ValueError(f"不支持的模型类型: {model_type}. 已注册类型: {list(self._type_registry.keys())}")
        
        pool_class = self._type_registry[model_type]
        # DoubaoPool 和 QwenPool 不接受 model_type 参数，OpenAIPool 和 AzurePool 接受
        if model_type in ["doubao", "qwen"]:
            pool_instance = pool_class(
                model_configs,
                default_params=default_params,
                timeout=timeout
            )
        else:
            pool_instance = pool_class(
                model_configs,
                model_type=model_type,
                default_params=default_params,
                timeout=timeout
            )
        self._pools[name] = pool_instance
        return pool_instance
    
    def get(self, name: str) -> ILLMClient:
        """
        获取模型客户端
        
        Args:
            name: 模型池名称
        
        Returns:
            ILLMClient 实例（实际上是 BaseModelPool）
        
        Raises:
            KeyError: 如果模型池不存在
        """
        if name not in self._pools:
            raise KeyError(f"模型池 '{name}' 未在容器中注册")
        return self._pools[name]
    
    def __getitem__(self, name: str) -> ILLMClient:
        """支持 [] 语法获取模型池"""
        return self.get(name)
    
    @property
    def pools(self) -> Dict[str, BaseModelPool]:
        """获取所有已注册的模型池"""
        return self._pools


# ====== 全局容器实例 ======
model_container = ModelContainer()
