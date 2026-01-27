"""模型池实现：支持多模型注册、负载均衡和故障转移"""
import os
import threading
import time
import logging
from typing import List, Optional, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
try:
    from openai import AzureOpenAI, OpenAI
except ImportError:
    AzureOpenAI = None
    OpenAI = None
from llm_datagen.core.llm import ILLMClient, IModelContainer
from llm_datagen.core.exceptions import LLMError

# ------------------- 内部独立 Logger 配置 -------------------
_logger = logging.getLogger("ModelPool")
_logger.setLevel(logging.INFO)
_logger.propagate = False
if not _logger.handlers:
    try:
        import datetime
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"model_pool_{datetime.datetime.now().strftime('%Y%m%d')}.log")
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(formatter)
        _logger.addHandler(file_handler)
    except Exception:
        pass


class ModelConfig:
    """模型配置"""
    def __init__(self, model: str, api_key: Optional[str] = None, base_url: Optional[str] = None, call_params: Optional[Dict] = None, timeout: Optional[float] = None, **kwargs):
        self.model = model
        self.api_key = api_key
        self.base_url = base_url
        self.call_params = call_params or {}
        self.timeout = timeout
        self.extra_params = kwargs


class BaseModelPool(ILLMClient):
    """基础模型池"""
    DEFAULT_TIMEOUT = 120.0
    
    def __init__(self, configs: List[ModelConfig], model_type: str = "Generic", default_params: Optional[Dict] = None, timeout: Optional[float] = None):
        if not configs: raise ValueError("必须提供至少一个模型配置")
        self.configs = configs
        self.model_type = model_type
        self.default_params = default_params or {}
        self.timeout = timeout or self.DEFAULT_TIMEOUT
        self.lock = threading.Lock()
        self.index = 0
    
    def _get_next_config(self) -> ModelConfig:
        with self.lock:
            cfg = self.configs[self.index]
            self.index = (self.index + 1) % len(self.configs)
            return cfg
    
    def call(self, prompt: str, retry: int = 2, timeout: Optional[float] = None, **kwargs) -> Tuple[str, Dict[str, int]]:
        raise NotImplementedError
    
    def call_batch(self, prompts: List[str], max_workers: int = 10, timeout: Optional[float] = None, **kwargs) -> List[Tuple[str, Dict[str, int]]]:
        if not prompts: return []
        results = [None] * len(prompts)
        request_timeout = timeout or self.timeout
        executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="llm_pool_")
        try:
            future_to_idx = {executor.submit(self.call, p, timeout=request_timeout, **kwargs): i for i, p in enumerate(prompts)}
            overall_timeout = request_timeout * 2 + 30 # 稍微宽松一点的整体超时
            try:
                for future in as_completed(future_to_idx, timeout=overall_timeout):
                    idx = future_to_idx[future]
                    try: results[idx] = future.result()
                    except Exception as e:
                        _logger.error(f"Batch pool request failed at index {idx}: {e}")
                        results[idx] = ("", {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
            except FutureTimeoutError:
                _logger.error(f"Batch pool overall timeout (timeout={overall_timeout}s)")
                for idx in range(len(prompts)):
                    if results[idx] is None:
                        results[idx] = ("", {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
        finally:
            executor.shutdown(wait=False)
        return results


class OpenAIPool(BaseModelPool):
    """OpenAI 协议模型池"""
    def __init__(self, configs: List[ModelConfig], model_type: str = "OpenAI", default_params: Optional[Dict] = None, timeout: Optional[float] = None):
        super().__init__(configs, model_type, default_params, timeout)
        self._clients: Dict[str, OpenAI] = {}
        self._client_lock = threading.Lock()
    
    def _get_client(self, cfg: ModelConfig) -> OpenAI:
        if OpenAI is None:
            raise LLMError("未找到 openai 库。请运行 'pip install openai' 来支持 OpenAI 协议模型（包括 Doubao/Qwen 等）。")
        cache_key = f"{cfg.base_url}_{cfg.api_key}"
        with self._client_lock:
            if cache_key not in self._clients:
                self._clients[cache_key] = OpenAI(api_key=cfg.api_key, base_url=cfg.base_url, timeout=cfg.timeout or self.timeout)
            return self._clients[cache_key]
    
    def call(self, prompt: str, retry: int = 2, timeout: Optional[float] = None, **kwargs) -> Tuple[str, Dict[str, int]]:
        last_err = None
        request_timeout = timeout or self.timeout
        for attempt in range(retry + 1):
            start_time = time.time()
            try:
                cfg = self._get_next_config()
                client = self._get_client(cfg)
                params = {**self.default_params, **cfg.call_params, **kwargs}
                response = client.chat.completions.create(model=cfg.model, messages=[{'role': 'user', 'content': prompt}], timeout=cfg.timeout or request_timeout, **params)
                content = response.choices[0].message.content
                usage = getattr(response, 'usage', None)
                usage_dict = {"prompt_tokens": usage.prompt_tokens if usage else 0, "completion_tokens": usage.completion_tokens if usage else 0, "total_tokens": usage.total_tokens if usage else 0}
                elapsed = time.time() - start_time
                _logger.info(f"✅ [{self.model_type}] 成功 | 耗时: {elapsed:.2f}s | Tokens: Prompt={usage_dict['prompt_tokens']}, Completion={usage_dict['completion_tokens']}, Total={usage_dict['total_tokens']}")
                return content, usage_dict
            except Exception as e:
                elapsed = time.time() - start_time
                last_err = e
                _logger.warning(f"⚠️ [{self.model_type}] 失败/超时 (尝试 {attempt+1}/{retry+1}): {e}")
                if attempt < retry: time.sleep(0.5 * (attempt + 1))
        raise LLMError(f"[{self.model_type}] 调用全部失败: {last_err}")


class AzurePool(BaseModelPool):
    """Azure OpenAI 模型池"""
    def __init__(self, configs: List[ModelConfig], default_params: Optional[Dict] = None, timeout: Optional[float] = None):
        defaults = {"temperature": 0.4, "top_p": 1.0}
        if default_params: defaults.update(default_params)
        super().__init__(configs, "AzureGPT", defaults, timeout)
        self._clients: Dict[str, AzureOpenAI] = {}
        self._client_lock = threading.Lock()

    def _get_client(self, cfg: ModelConfig) -> AzureOpenAI:
        if AzureOpenAI is None:
            raise LLMError("未找到 openai 库。请运行 'pip install openai' 来支持 Azure OpenAI。")
        cache_key = f"{cfg.base_url}_{cfg.api_key}"
        with self._client_lock:
            if cache_key not in self._clients:
                self._clients[cache_key] = AzureOpenAI(api_key=cfg.api_key, api_version="2025-01-01-preview", azure_endpoint=cfg.base_url, timeout=cfg.timeout or self.timeout)
            return self._clients[cache_key]
    
    def call(self, prompt: str, retry: int = 2, timeout: Optional[float] = None, **kwargs) -> Tuple[str, Dict[str, int]]:
        last_err = None
        request_timeout = timeout or self.timeout
        for attempt in range(retry + 1):
            start_time = time.time()
            try:
                cfg = self._get_next_config()
                client = self._get_client(cfg)
                params = {**self.default_params, **cfg.call_params, **kwargs}
                response = client.chat.completions.create(model=cfg.model, messages=[{'role': 'user', 'content': prompt}], timeout=cfg.timeout or request_timeout, **params)
                content = response.choices[0].message.content
                usage = getattr(response, 'usage', None)
                usage_dict = {"prompt_tokens": usage.prompt_tokens if usage else 0, "completion_tokens": usage.completion_tokens if usage else 0, "total_tokens": usage.total_tokens if usage else 0}
                elapsed = time.time() - start_time
                _logger.info(f"✅ [Azure] 成功 | 模型: {cfg.model} | 耗时: {elapsed:.2f}s | Tokens: Prompt={usage_dict['prompt_tokens']}, Completion={usage_dict['completion_tokens']}, Total={usage_dict['total_tokens']}")
                return content, usage_dict
            except Exception as e:
                elapsed = time.time() - start_time
                last_err = e
                _logger.warning(f"⚠️ [Azure] 失败/超时 (尝试 {attempt+1}/{retry+1}): {e}")
                if attempt < retry: time.sleep(1 * (attempt + 1))
        raise LLMError(f"[Azure] 调用全部失败: {last_err}")


class DoubaoPool(OpenAIPool):
    """豆包模型池"""
    def __init__(self, configs: List[ModelConfig], default_params: Optional[Dict] = None, timeout: Optional[float] = None):
        super().__init__(configs, model_type="Doubao", default_params=default_params, timeout=timeout)


class QwenPool(OpenAIPool):
    """通义千问模型池"""
    def __init__(self, configs: List[ModelConfig], default_params: Optional[Dict] = None, timeout: Optional[float] = None):
        defaults = {"temperature": 0.05, "max_tokens": 2000, "response_format": {"type": "json_object"}}
        if default_params: defaults.update(default_params)
        super().__init__(configs, model_type="QWEN", default_params=defaults, timeout=timeout)
