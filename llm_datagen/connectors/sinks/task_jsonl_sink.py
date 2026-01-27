import json
import os
from typing import List, Any
from dataPlat.core.interfaces import IDataSink

class TaskJsonlSink(IDataSink):
    """
    JSONL 输出适配器：实现“跑一个存一个”的工业级特性。
    完成后负责将中间暂存数据注册到资产管理库。
    """
    def __init__(self, task_id: str, task_repo, asset_svc, asset_name: str, asset_type: str, config: dict = None):
        self.tid = task_id
        self.task_repo = task_repo
        self.asset_svc = asset_svc
        self.asset_name = asset_name
        self.asset_type = asset_type
        self.config = config or {}

    def write(self, item: Any):
        """增量写入一条结果到暂存区"""
        self.task_repo.append_item_result(self.tid, item)

    def get_processed_ids(self) -> List[str]:
        """读取已完成的中间结果，用于任务恢复（断点续传）"""
        results = self.task_repo.load_incremental_results(self.tid)
        # 获取已处理过的 pid 或 session_id
        return [str(r.get("pid") or r.get("patient_id") or r.get("session_id")) for r in results]

    def finalize(self) -> str:
        """任务全部完成后，将暂存的 jsonl 数据正式转为资产库资产"""
        final_data = self.task_repo.load_incremental_results(self.tid)
        aid = self.asset_svc.repo.save_asset(
            self.asset_name, 
            self.asset_type, 
            final_data, 
            source_task=self.tid, 
            config=self.config
        )
        # 清理任务暂存文件（可选，建议保留用于追溯）
        # self.task_repo.clear_temp_result(self.tid)
        return aid
