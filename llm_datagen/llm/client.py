"""LLM 客户端实现：单次和批量调用"""
import json
import threading
from typing import List, Tuple, Dict, Any, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from llm_datagen.core.llm import ILLMClient, IBatchLLMClient
from llm_datagen.core.exceptions import LLMError


class LLMClient(ILLMClient):
    """
    基础 LLM 客户端：负责单次序贯调用
    
    通过 ModelContainer 获取模型池，然后调用模型池的 call 方法
    """
    
    def __init__(self, model_name: str, model_container=None):
        """
        Args:
            model_name: 模型池名称（在 ModelContainer 中注册的名称）
            model_container: ModelContainer 实例（可选，默认使用全局实例）
        """
        self.model_name = model_name
        self._container = model_container
        self._usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        self._lock = threading.Lock()
    
    @property
    def container(self):
        """获取模型容器"""
        if self._container is None:
            from llm_datagen.llm.factory import model_container
            return model_container
        return self._container
    
    @property
    def pool(self):
        """获取模型池"""
        return self.container.get(self.model_name)
    
    def call(self, prompt: str, **kwargs) -> Tuple[str, Dict[str, int]]:
        """
        单次调用
        
        Args:
            prompt: 提示词
            **kwargs: 其他参数（如 temperature, max_tokens 等）
        
        Returns:
            (content, usage) 元组
        """
        try:
            res, usage = self.pool.call(prompt, **kwargs)
            with self._lock:
                self._usage["prompt_tokens"] += usage.get("prompt_tokens", 0)
                self._usage["completion_tokens"] += usage.get("completion_tokens", 0)
                self._usage["total_tokens"] += usage.get("total_tokens", 0)
            return res, usage
        except Exception as e:
            raise LLMError(f"LLM call failed ({self.model_name}): {e}") from e
    
    def pull_usage(self) -> Dict[str, int]:
        """
        拉取当前累加的用量快照并重置
        
        Returns:
            用量快照
        """
        with self._lock:
            snapshot = self._usage.copy()
            self._usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            return snapshot
    
    def parse_json(self, response: str) -> Any:
        """
        解析 LLM 返回的 JSON 字符串
        
        Args:
            response: LLM 返回的字符串
        
        Returns:
            解析后的 JSON 对象，失败返回 None
        """
        if not response:
            return None
        try:
            cleaned = response.strip()
            # 1. 尝试从 Markdown 代码块中提取
            if "```json" in cleaned:
                cleaned = cleaned.split("```json")[1].split("```")[0].strip()
            elif "```" in cleaned:
                parts = cleaned.split("```")
                if len(parts) >= 3:
                    cleaned = parts[1].strip()
            
            # 2. 尝试寻找 JSON 结构的起点（处理带分析文字的返回）
            if not (cleaned.startswith("{") or cleaned.startswith("[")):
                start_dict = cleaned.find("{")
                start_list = cleaned.find("[")
                if start_dict != -1 and (start_list == -1 or start_dict < start_list):
                    cleaned = cleaned[start_dict:]
                elif start_list != -1:
                    cleaned = cleaned[start_list:]
            
            # 3. 尝试标准解析
            try:
                return json.loads(cleaned)
            except json.JSONDecodeError:
                # 4. 容错处理：处理截断 (通用闭合法)
                if (cleaned.startswith("{") and not cleaned.endswith("}")) or \
                   (cleaned.startswith("[") and not cleaned.endswith("]")):
                    # 补齐未闭合的引号
                    temp = cleaned
                    if temp.count('"') % 2 != 0:
                        temp += '"'
                    
                    # 使用栈补齐未闭合的括号
                    stack = []
                    for char in temp:
                        if char == '{':
                            stack.append('}')
                        elif char == '[':
                            stack.append(']')
                        elif char == '}' or char == ']':
                            if stack and stack[-1] == char:
                                stack.pop()
                    
                    while stack:
                        temp += stack.pop()
                    
                    try:
                        return json.loads(temp)
                    except:
                        # 特殊兜底：如果是之前 sessions 结构的严重截断
                        if '"sessions": [' in cleaned:
                            last_brace = cleaned.rfind("}")
                            if last_brace != -1:
                                try:
                                    return json.loads(cleaned[:last_brace+1] + "\n  ]\n}")
                                except:
                                    pass
                raise
        except Exception as e:
            return None


class BatchLLMClient(LLMClient, IBatchLLMClient):
    """
    批量并发 LLM 客户端：通过多线程实现并发，并保证输出顺序
    """
    
    def call_batch(
        self, 
        prompts: List[str], 
        max_workers: int = 10, 
        is_cancelled_func: Optional[Callable[[], bool]] = None,
        **kwargs
    ) -> List[Tuple[str, Dict[str, int]]]:
        """
        批量并发调用
        
        Args:
            prompts: 提示词列表
            max_workers: 最大并发数
            is_cancelled_func: 检查是否已取消的回调（可选）
            **kwargs: 其他参数
        
        Returns:
            [(content, usage), ...] 列表，顺序与输入一致
        """
        if not prompts:
            return []
        
        results = [None] * len(prompts)
        executor = ThreadPoolExecutor(max_workers=max_workers)
        
        try:
            # 1. 提交所有任务
            future_to_idx = {
                executor.submit(self.pool.call, p, **kwargs): i
                for i, p in enumerate(prompts)
            }
            
            # 2. 收集结果（保证顺序）
            pending_futures = set(future_to_idx.keys())
            
            while pending_futures:
                # 检查是否取消
                if is_cancelled_func and is_cancelled_func():
                    # 尝试取消未开始的任务
                    for f in pending_futures:
                        f.cancel()
                    break

                # 等待已完成的任务
                done, not_done = wait(pending_futures, timeout=0.5, return_when=FIRST_COMPLETED)
                
                for f in done:
                    idx = future_to_idx[f]
                    try:
                        res, usage = f.result()
                        results[idx] = (res, usage)
                        with self._lock:
                            self._usage["prompt_tokens"] += usage.get("prompt_tokens", 0)
                            self._usage["completion_tokens"] += usage.get("completion_tokens", 0)
                            self._usage["total_tokens"] += usage.get("total_tokens", 0)
                    except Exception as e:
                        results[idx] = ("", {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
                    pending_futures.remove(f)
        except Exception as e:
            if "interpreter shutdown" not in str(e):
                raise LLMError(f"Batch LLM execution failed: {e}") from e
        finally:
            # 非阻塞关闭，防止阻塞进程退出
            # 如果已取消，则强制不等待
            wait_on_shutdown = not (is_cancelled_func and is_cancelled_func())
            executor.shutdown(wait=wait_on_shutdown)
        
        return results
