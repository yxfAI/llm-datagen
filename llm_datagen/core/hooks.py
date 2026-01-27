"""Pipeline 钩子接口：定义可扩展的回调机制"""
import threading
import time
import os
import json
from typing import Any, Dict, List, Optional, Callable, Protocol
from abc import ABC


class IPipelineHooks(Protocol):
    """
    Pipeline 钩子接口：定义 Pipeline 执行过程中的所有回调点
    """
    
    def clear_state(self) -> None:
        """全新启动时，强制清空内存中的统计数据"""
        ...
    
    # ========== Pipeline 生命周期钩子 ==========
    
    def on_pipeline_start(self, context_id: str, config: Dict[str, Any]) -> None:
        """Pipeline 开始执行时调用"""
        ...
    
    def load_state(self, context_id: str, config: Dict[str, Any]) -> None:
        """在 Pipeline 决策前，提前加载磁盘状态"""
        ...
    
    def on_pipeline_end(self, context_id: str, success: bool, error: Optional[Exception] = None) -> None:
        """Pipeline 执行结束时调用"""
        ...
    
    # ========== Node 生命周期钩子 ==========
    
    def on_node_start(self, context_id: str, node_id: str, config: Dict[str, Any]) -> None:
        """节点开始执行时调用"""
        ...
    
    def on_node_finish(self, context_id: str, node_id: str) -> None:
        """节点执行顺利结束时调用"""
        ...

    def on_node_end(self, context_id: str, node_id: str, success: bool, error: Optional[Exception] = None) -> None:
        """节点执行结束时调用"""
        ...
    
    # ========== 进度和状态钩子 ==========
    
    def on_node_progress(self, context_id: str, node_id: str, current: int, total: Optional[int] = None, metadata: Optional[Dict] = None) -> None:
        """处理进度更新时调用"""
        ...
    
    def on_usage(self, context_id: str, node_id: str, provider: str, model: str, metrics: Dict[str, Any]) -> None:
        """资源消耗更新时调用"""
        ...
    
    # ========== 错误和失败钩子 ==========
    
    def on_node_error(self, context_id: str, node_id: str, error: Exception, items: Optional[List[Any]] = None) -> None:
        """发生错误时调用"""
        ...
    
    # ========== 检查点钩子 ==========
    
    def on_checkpoint(self, context_id: str, node_id: str, checkpoint: Any) -> None:
        """保存检查点时调用"""
        ...
    
    def on_log(self, context_id: str, node_id: str, level: str, message: str, item_id: str = "system") -> None:
        """记录日志时调用"""
        ...

    def get_state(self) -> Dict[str, Any]:
        """导出 Hook 状态"""
        ...

    def load_state_data(self, data: Dict[str, Any]) -> None:
        """加载快照数据"""
        ...

    def get_checkpoint(self, node_id: str) -> Any:
        """获取指定节点的进度"""
        ...


class CompositePipelineHooks(IPipelineHooks):
    """组合钩子分发器"""
    def __init__(self, hooks: List[IPipelineHooks]):
        self.hooks = [h for h in hooks if h is not None]

    def clear_state(self) -> None:
        for h in self.hooks: 
            if hasattr(h, 'clear_state'): h.clear_state()

    def on_pipeline_start(self, context_id: str, config: Dict[str, Any]):
        for h in self.hooks: 
            if hasattr(h, 'on_pipeline_start'): h.on_pipeline_start(context_id, config)

    def load_state(self, context_id: str, config: Dict[str, Any]):
        for h in self.hooks: 
            if hasattr(h, 'load_state'): h.load_state(context_id, config)

    def on_pipeline_end(self, context_id: str, success: bool, error: Optional[Exception] = None):
        for h in self.hooks: 
            if hasattr(h, 'on_pipeline_end'): h.on_pipeline_end(context_id, success, error)

    def on_node_start(self, context_id: str, node_id: str, config: Dict[str, Any]):
        for h in self.hooks: 
            if hasattr(h, 'on_node_start'): h.on_node_start(context_id, node_id, config)

    def on_node_finish(self, context_id: str, node_id: str):
        for h in self.hooks: 
            if hasattr(h, 'on_node_finish'): h.on_node_finish(context_id, node_id)

    def on_node_error(self, context_id: str, node_id: str, error: Exception, items: Optional[List[Any]] = None):
        for h in self.hooks: 
            if hasattr(h, 'on_node_error'): h.on_node_error(context_id, node_id, error, items)

    def on_node_progress(self, context_id: str, node_id: str, current: int, total: Optional[int] = None, metadata: Optional[Dict] = None):
        for h in self.hooks: 
            if hasattr(h, 'on_node_progress'): h.on_node_progress(context_id, node_id, current, total, metadata)

    def on_usage(self, context_id, node_id, provider, model, metrics):
        for h in self.hooks:
            if hasattr(h, 'on_usage'): h.on_usage(context_id, node_id, provider, model, metrics)

    def on_checkpoint(self, context_id: str, node_id: str, checkpoint: Any):
        for h in self.hooks: 
            if hasattr(h, 'on_checkpoint'): h.on_checkpoint(context_id, node_id, checkpoint)

    def on_log(self, context_id, node_id, level, message, item_id="system"):
        for h in self.hooks:
            if hasattr(h, 'on_log'): h.on_log(context_id, node_id, level, message, item_id)

    def get_state(self) -> Dict[str, Any]:
        state = {}
        for h in self.hooks:
            if hasattr(h, 'get_state'): state[h.__class__.__name__] = h.get_state()
        return state

    def load_state_data(self, data: Dict[str, Any]) -> None:
        if not data: return
        for h in self.hooks:
            name = h.__class__.__name__
            if name in data and hasattr(h, 'load_state_data'): h.load_state_data(data[name])

    def get_checkpoint(self, nid: str) -> Any:
        for h in self.hooks:
            if hasattr(h, "get_checkpoint"):
                cp = h.get_checkpoint(nid)
                if cp is not None: return cp
        return None


class PipelineHooksAdapter:
    """适配器：隔离内部实现与回调分发"""
    def __init__(self, hooks: Optional[IPipelineHooks] = None):
        self._hooks = hooks
    
    def on_pipeline_start(self, cid, cfg):
        if self._hooks: self._hooks.on_pipeline_start(cid, cfg)
    
    def load_state(self, cid, cfg):
        if self._hooks: self._hooks.load_state(cid, cfg)
    
    def on_pipeline_end(self, cid, ok, err=None):
        if self._hooks: self._hooks.on_pipeline_end(cid, ok, err)
    
    def on_node_start(self, cid, nid, cfg):
        if self._hooks: self._hooks.on_node_start(cid, nid, cfg)
    
    def on_node_finish(self, cid, nid):
        if self._hooks: self._hooks.on_node_finish(cid, nid)

    def on_node_progress(self, cid, nid, curr, total=None, meta=None):
        if self._hooks: self._hooks.on_node_progress(cid, nid, curr, total, meta)
    
    def on_usage(self, cid, nid, prov, model, metrics):
        if self._hooks: self._hooks.on_usage(cid, nid, prov, model, metrics)
    
    def on_node_error(self, cid, nid, err, items=None):
        if self._hooks: self._hooks.on_node_error(cid, nid, err, items)
    
    def on_checkpoint(self, cid, nid, cp):
        if self._hooks: self._hooks.on_checkpoint(cid, nid, cp)

    def on_node_log(self, cid, nid, msg, lv):
        if self._hooks and hasattr(self._hooks, 'on_log'): 
            self._hooks.on_log(cid, nid, lv, msg)


class DefaultPipelineHooks:
    """默认业务统计钩子"""
    def __init__(self, base_dir: str = "tmp"):
        self.base_dir = base_dir
        self.start_time = 0
        self.node_usages = {} 
        self.node_progress = {} 
        self._all_nodes = []
        self._lock = threading.Lock()
        self._last_printed_prog = {}
    
    def on_pipeline_start(self, context_id, config):
        self.start_time = time.time()
        print(f"\n🚀 [Context:{context_id}] 启动...")

    def _update_node_status(self, node_id, status):
        with self._lock:
            info = self.node_progress.setdefault(node_id, {"current": 0, "total": 0})
            info["status"] = status

    def on_node_start(self, cid, nid, cfg):
        with self._lock:
            if nid not in self._all_nodes: self._all_nodes.append(nid)
            self.node_usages.setdefault(nid, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
        self._update_node_status(nid, "running")

    def on_node_finish(self, cid, nid):
        with self._lock:
            info = self.node_progress.setdefault(nid, {"current": 0, "total": 0})
            info["status"] = "completed"
            if info["total"] < info["current"]:
                info["total"] = info["current"]

    def on_node_error(self, cid, nid, err, items):
        self._update_node_status(nid, "failed")

    def on_node_progress(self, cid, nid, curr, total=None, meta=None):
        with self._lock:
            info = self.node_progress.setdefault(nid, {"current": 0, "total": 0, "status": "running"})
            if curr > info.get("current", 0): 
                info["current"] = curr
            
            # 核心修复：更新总数，并确保总数不小于当前进度
            if total is not None and total > 0: 
                info["total"] = total
            if info["current"] > info["total"]:
                info["total"] = info["current"]
            
            # 自动维护 status
            if info.get("status") != "completed":
                info["status"] = "running"
            
            # 打印逻辑优化：避免日志轰炸
            if self._last_printed_prog.get(nid) == curr: return
            total_val = info.get("total", 0)
            total_str = f"/{total_val}" if total_val > 0 else ""
            
            # 智能阈值判定
            should_print = False
            if total_val > 0:
                # 场景 A: 有明确总数。按 1% 步长打印
                step = max(1, total_val // 100)
                if curr % step == 0 or curr >= total_val:
                    should_print = True
            else:
                # 场景 B: 流式。固定每 50 条打印
                if curr % 50 == 0:
                    should_print = True
            
            if should_print:
                print(f"  ↳ [{nid}] 进度: {curr}{total_str}")
                self._last_printed_prog[nid] = curr

    def on_usage(self, cid, nid, prov, model, metrics):
        with self._lock:
            usage = self.node_usages.setdefault(nid, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
            p = metrics.get("prompt_tokens", 0)
            c = metrics.get("completion_tokens", 0)
            t = metrics.get("total_tokens", 0) or (p + c)
            usage["prompt_tokens"] += p
            usage["completion_tokens"] += c
            usage["total_tokens"] += t

    def on_checkpoint(self, cid, nid, cp):
        with self._lock:
            info = self.node_progress.setdefault(nid, {"current": 0, "total": 0})
            if isinstance(cp, dict):
                new_curr = cp.get("current", 0)
                if new_curr > info.get("current", 0): info["current"] = new_curr
                if "total" in cp: info["total"] = cp["total"]
                if "status" in cp: info["status"] = cp["status"]
                
                # 核心修复：从检查点恢复 Token 消耗数据
                if "usage" in cp:
                    self.node_usages[nid] = cp["usage"]

    def get_state(self):
        with self._lock:
            return {"node_usages": self.node_usages, "node_progress": self.node_progress, "all_nodes": self._all_nodes, "start_time": self.start_time}

    def load_state_data(self, data):
        if not data: return
        with self._lock:
            # 1. 恢复 Token 消耗和进度数据
            self.node_usages = data.get("node_usages", {})
            self.node_progress = data.get("node_progress", {})
            
            # 2. 核心修复：彻底恢复节点 ID 列表，确保报告中包含历史节点
            saved_nodes = data.get("all_nodes", [])
            for nid in saved_nodes:
                if nid not in self._all_nodes:
                    self._all_nodes.append(nid)
            
            # 补漏：如果 progress 里有但 all_nodes 没存，也要补上
            for nid in self.node_progress.keys():
                if nid not in self._all_nodes:
                    self._all_nodes.append(nid)
                    
            self.start_time = data.get("start_time", self.start_time)
            print(f"📈 [Hooks] 成功从快照同步了 {len(self._all_nodes)} 个节点的历史统计")

    def on_pipeline_end(self, cid, ok, err=None):
        duration = time.time() - self.start_time
        self._print_summary(cid, ok, err, duration)
        
        # 保存 report.json
        report_path = os.path.join(self.base_dir, cid, "report.json")
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with self._lock:
            report = {"pipeline_id": cid, "status": "completed" if ok else "failed", "error": str(err) if err else None, "duration": f"{duration:.2f}s", "nodes": self.node_progress, "usages": self.node_usages}
            with open(report_path, "w", encoding="utf-8") as f: json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"📊 运行报告已保存至: {report_path}")

    def _print_summary(self, cid, ok, err, dur):
        status_str = "✅ 顺利执行完成" if ok else f"❌ 执行失败: {err}"
        print(f"\n{'='*60}\n🏁 [Context:{cid}] {status_str}！\n⏱️  总耗时: {dur:.2f} 秒\n\n💰 资源统计:")
        print(f"{'Node ID':<20} | {'Progress':<15} | {'Tokens (P/C/Total)'}")
        print("-" * 60)
        for nid in self._all_nodes:
            prog = self.node_progress.get(nid, {"current": 0, "total": 0})
            usage = self.node_usages.get(nid, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
            p, c, t = usage['prompt_tokens'], usage['completion_tokens'], usage['total_tokens']
            print(f"{nid:<20} | {prog['current']}/{prog['total']:<15} | {p}/{c}/{t}")
        print("=" * 60 + "\n")


class JsonFileCheckpointHooks(DefaultPipelineHooks):
    """磁盘检查点持久化钩子"""
    def __init__(self, base_dir: str = "tmp/results"):
        super().__init__(base_dir=base_dir)

    def _save_checkpoint(self, cid):
        path = os.path.join(self.base_dir, cid, "checkpoint.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with self._lock:
            data = {"nodes": self.node_progress, "updated_at": time.time(), "pipeline_id": cid}
            with open(path, "w", encoding="utf-8") as f: json.dump(data, f, indent=2, ensure_ascii=False)

    def on_node_start(self, cid, nid, cfg):
        super().on_node_start(cid, nid, cfg)
        self._save_checkpoint(cid)

    def on_node_finish(self, cid, nid):
        super().on_node_finish(cid, nid)
        self._save_checkpoint(cid)

    def on_node_error(self, cid, nid, err, items):
        super().on_node_error(cid, nid, err, items)
        self._save_checkpoint(cid)

    def on_node_progress(self, cid, nid, curr, total=None, meta=None):
        super().on_node_progress(cid, nid, curr, total, meta)
        self._save_checkpoint(cid)

    def load_state(self, cid, cfg):
        path = os.path.join(self.base_dir, cid, "checkpoint.json")
        if not os.path.exists(path): return
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            with self._lock: 
                # 恢复进度
                self.node_progress = data.get("nodes", {})
                # 核心修复：将历史节点的 ID 全部注册到统计名单中
                for nid in self.node_progress.keys():
                    if nid not in self._all_nodes:
                        self._all_nodes.append(nid)
        print(f"💾 [Hooks] 从磁盘成功恢复了 {len(self.node_progress)} 个节点的历史位点与身份")

    def get_checkpoint(self, nid: str) -> Any:
        with self._lock: 
            return self.node_progress.get(nid)
