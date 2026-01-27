"""LLM 算子实现"""
import json
import logging
from typing import List, Any, Dict, Optional
from llm_datagen.core.operators import IBatchOperator, BaseOperator
from llm_datagen.core.node import INodeContext
from llm_datagen.llm.client import BatchLLMClient

logger = logging.getLogger("DataGen.Operators")

class GenericLLMOperator(BaseOperator):
    """
    通用 LLM 模板处理算子：绝对扁平化，支持全量字段渲染
    """
    
    def __init__(self, config: Dict[str, Any] = None, model_name: str = None, ctx: Any = None):
        self.config = config or {}
        self.model_name = self.config.get("model_name", model_name)
        if not self.model_name:
            raise ValueError("GenericLLMOperator requires model_name")
        self.llm = BatchLLMClient(model_name=self.model_name)
    
    @property
    def operator_type(self) -> str:
        """算子的唯一类型标识"""
        return "llm"
    
    @property
    def audit_type(self) -> str:
        """审计类型"""
        return "llm"
    
    def process_batch(self, items: List[Any], ctx: INodeContext = None) -> List[Any]:
        prompt_tmpl = self.config.get("custom_prompt")
        if not prompt_tmpl:
            return items
        
        prompts = []
        valid_indices = []
        
        for i, item in enumerate(items):
            try:
                # 现在的 item 已经是框架剥离后的纯业务字典
                if isinstance(item, dict):
                    # 自动兼容：如果模板要 {text} 但只有 {content}，自动映射
                    render_data = item.copy()
                    if "text" not in render_data and "content" in render_data:
                        render_data["text"] = render_data["content"]
                    
                    # 核心：使用 format(**render_data) 渲染所有业务字段
                    p = prompt_tmpl.format(**render_data)
                else:
                    # 非字典回退
                    p = prompt_tmpl.format(text=str(item), content=str(item))
                
                prompts.append(p)
                valid_indices.append(i)
            except KeyError as e:
                # 显式提示缺少哪个字段
                available_keys = list(item.keys()) if isinstance(item, dict) else '非字典'
                logger.warning(f"算子格式化失败: 缺少字段 {e}。当前可用字段: {available_keys}")
                # 容错：如果是因为 data 嵌套导致的（理论上框架已处理，但为了双重保险），尝试解包
                if isinstance(item, dict) and "data" in item and isinstance(item["data"], dict):
                    try:
                        p = prompt_tmpl.format(**item["data"])
                        prompts.append(p)
                        valid_indices.append(i)
                        continue
                    except: pass
                prompts.append(None)
            except Exception as e:
                logger.warning(f"Prompt 渲染异常 [Item:{i}]: {e}")
                prompts.append(None)
        
        # 过滤掉渲染失败的项
        actual_prompts = [p for p in prompts if p is not None]
        actual_indices = [i for i, p in enumerate(prompts) if p is not None]
        
        if not actual_prompts:
            return [None] * len(items)
        
        # 批量调用 LLM
        max_workers = self.config.get("max_workers", 10)
        responses = self.llm.call_batch(
            actual_prompts, 
            max_workers=max_workers,
            is_cancelled_func=ctx.is_cancelled if ctx else None
        )
        
        # 组装结果
        results = [None] * len(items)
        for i, (resp, usage) in enumerate(responses):
            idx = actual_indices[i]
            original_item = items[idx]
            
            # 创建结果副本，保持平铺
            res_item = original_item.copy() if isinstance(original_item, dict) else {"content": original_item}
            res_item["llm_output"] = resp
            
            # 尝试解析 JSON 增强结果
            try:
                parsed = self.llm.parse_json(resp)
                if isinstance(parsed, dict):
                    # 直接合并业务字段，不加任何前缀
                    res_item.update({k: v for k, v in parsed.items() if k not in ("_i", "_meta")})
            except Exception:
                pass
            
            # 上报 Token (提供商, 模型名, 用量指标)
            if ctx: 
                provider = self.llm.pool.model_type if hasattr(self.llm.pool, "model_type") else "unknown"
                # 修复：report_usage 仅接受一个 dict 参数
                usage_metrics = {
                    "provider": provider,
                    "model": self.model_name,
                    **usage
                }
                ctx.report_usage(usage_metrics)
            results[idx] = res_item
        
        return results
