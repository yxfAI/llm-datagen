import os
from typing import Dict, Optional

class PromptManager:
    """简单的提示词管理类，支持从文件加载提示词模板"""
    
    def __init__(self, search_path: str = None):
        self.search_path = search_path or os.path.dirname(__file__)
        self._cache: Dict[str, str] = {}

    def get_prompt(self, name: str) -> Optional[str]:
        """
        获取提示词模板。
        :param name: 提示词名称（文件名，不含后缀）
        """
        if name in self._cache:
            return self._cache[name]
            
        file_path = os.path.join(self.search_path, f"{name}.txt")
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                self._cache[name] = content
                return content
        return None

# 全局单例
prompt_manager = PromptManager()
