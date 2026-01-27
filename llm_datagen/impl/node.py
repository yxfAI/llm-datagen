"""节点实现：提供核心批处理容器与统一选型策略入口"""

import threading
import time
import logging
import os
from typing import Any, List, Optional, Dict, Callable
from concurrent.futures import ThreadPoolExecutor

from llm_datagen.core.node import INode, IBatchNode, IRecoverableNode, NodeStatus
from llm_datagen.core.operators import IOperator, ISingleOperator, IBatchOperator
from llm_datagen.core.config import WriterConfig

# ------------------- 节点级 Logger 配置 -------------------
_node_logger = logging.getLogger("DataGen.Node")

class NodeContextImpl:
    """节点执行上下文实现"""
    def __init__(
        self,
        node_id: str,
        context_id: str,
        on_progress: Callable[[int, Optional[int], Optional[Dict]], None],
        on_usage: Callable[[Dict[str, Any]], None],
        on_log: Callable[[str, str], None],
        on_error: Callable[[Exception, List[Any]], None],
        is_cancelled_func: Callable[[], bool],
        save_checkpoint_func: Optional[Callable[[], None]] = None
    ):
        self._node_id = node_id
        self._context_id = context_id
        self._on_progress = on_progress
        self._on_usage = on_usage
        self._on_log = on_log
        self._on_error = on_error
        self._is_cancelled_func = is_cancelled_func
        self._save_checkpoint_func = save_checkpoint_func
        
        self.metrics = {"cost": 0.0, "tokens": 0}
        self.current_progress = 0
        self.total_progress: Optional[int] = 0

    @property
    def node_id(self) -> str: return self._node_id
    @property
    def context_id(self) -> str: return self._context_id
    def is_cancelled(self) -> bool: return self._is_cancelled_func()
    def report_progress(self, current: int, total: Optional[int], metadata: Optional[Dict] = None):
        self.current_progress = current; self.total_progress = total
        self._on_progress(current, total, metadata)
    def save_checkpoint(self):
        if self._save_checkpoint_func: self._save_checkpoint_func()
    def report_usage(self, metrics: Dict[str, Any]):
        for k, v in metrics.items():
            if isinstance(v, (int, float)): self.metrics[k] = self.metrics.get(k, 0) + v
        # 核心修复：确保转发给外部监听器 (Hooks)
        self._on_usage(metrics)
    def log(self, message: str, level: str = "info"): self._on_log(message, level)
    def report_failed_items(self, items: List[Any], error: Exception): self._on_error(error, items)


# ==============================================================================
# 1. BaseNode: 只管拓扑和生命周期
# ==============================================================================

class BaseNode(IRecoverableNode):
    """
    节点执行基类：
    1. 持有拓扑配置 (node_id, input_uri, output_uri)
    2. 管理生命周期与 I/O 绑定
    """
    def __init__(self, 
                 node_id: str = None,
                 input_uri: Optional[str] = None,
                 output_uri: Optional[str] = None,
                 protocol_prefix: str = "",
                 base_path: str = ""):
        # 拓扑属性
        self._node_id = node_id
        self._input_uri = input_uri
        self._output_uri = output_uri
        self._protocol_prefix = protocol_prefix
        self._base_path = base_path
        
        # 状态属性
        self._status = NodeStatus.PENDING
        self._ctx: Optional[NodeContextImpl] = None
        self._input_stream = None
        self._output_stream = None
        self._current_progress = 0
        self._total_progress = 0
        self._resume_progress = None
        self._shutdown_event = threading.Event()
        self._start_time = 0
        self._end_time = 0
        self._writer_config: Optional[WriterConfig] = None

    @property
    def node_id(self) -> str: return self._node_id
    @property
    def input_uri(self) -> Optional[str]: return self._input_uri
    @input_uri.setter
    def input_uri(self, v): self._input_uri = v
    @property
    def output_uri(self) -> Optional[str]: return self._output_uri
    @output_uri.setter
    def output_uri(self, v): self._output_uri = v
    
    @property
    def log_id(self) -> str:
        cid = self._ctx.context_id if self._ctx else "init"
        return f"[{cid}:{self._node_id}]"
    @property
    def status(self) -> NodeStatus: return self._status
    @status.setter
    def status(self, value: NodeStatus): self._status = value
    @property
    def progress(self) -> Dict[str, int]: return {"current": self._current_progress, "total": self._total_progress}

    @property
    def input_stream(self): 
        return self._impl.input_stream if (self._impl and hasattr(self._impl, 'input_stream')) else self._input_stream
    @property
    def output_stream(self): 
        return self._impl.output_stream if (self._impl and hasattr(self._impl, 'output_stream')) else self._output_stream

    def bind_io(self, input_stream: Any, output_stream: Any):
        self._input_stream = input_stream
        self._output_stream = output_stream

    def set_context(self, ctx: NodeContextImpl):
        self._ctx = ctx

    def set_writer_config(self, config: WriterConfig):
        self._writer_config = config

    def open(self, ctx: Optional[NodeContextImpl] = None, progress: Optional[Any] = None):
        if ctx: self.set_context(ctx)
        _node_logger.info(f"{self.log_id} 🎬 节点开启 (Status={self._status.value})")
        
        # 核心修复：尊重恢复状态，不要盲目覆盖为 RUNNING
        if self._status != NodeStatus.COMPLETED:
            self._status = NodeStatus.RUNNING
            
        self._start_time = time.time()
        
        target_progress = progress if progress is not None else self._resume_progress
        
        if self._input_stream: self._input_stream.open()
        if self._output_stream: 
            # 核心修复：如果是恢复模式且节点尚未完成，必须撤销之前的物理封条
            # 否则下游节点会误以为该流已结束
            if self._status != NodeStatus.COMPLETED:
                if hasattr(self._output_stream, "unseal"):
                    self._output_stream.unseal()
            self._output_stream.open()
        
        self._reader = self._input_stream.get_reader(progress=target_progress) if self._input_stream else None
        self._writer = self._output_stream.get_writer(options=self._writer_config) if self._output_stream else None
        
        if self._reader:
            # 核心改进：初始总量对齐物理存储，但进度保持由快照注入的真相位点
            self._total_progress = self._reader.total_count
            
            _node_logger.info(f"{self.log_id} 📊 物理位点对齐: 进度={self._current_progress}, 总量={self._total_progress}")
            
            # 核心改进：开启时立即上报一次初始位点（确保 total 被 Hook 捕获）
            if self._ctx:
                self._ctx.report_progress(self._current_progress, self._total_progress)

    def close(self):
        if hasattr(self, '_reader') and self._reader: self._reader.close(); self._reader = None
        if hasattr(self, '_writer') and self._writer: self._writer.close(); self._writer = None
        if self._input_stream: self._input_stream.close()
        if self._output_stream: self._output_stream.close()
        
        old_status = self._status
        if self._shutdown_event.is_set(): self._status = NodeStatus.CANCELED
        elif self._status == NodeStatus.RUNNING: self._status = NodeStatus.COMPLETED
        self._end_time = time.time()

        # 核心修复：关闭前强制同步一次最终进度，确保 total 不为 0
        if self._ctx:
            if self._total_progress < self._current_progress:
                self._total_progress = self._current_progress
            self._ctx.report_progress(self._current_progress, self._total_progress)
        
        _node_logger.info(f"{self.log_id} 🏁 节点关闭 (Status: {old_status.value} -> {self._status.value}, Duration: {self.get_duration():.2f}s)")

    def cancel(self):
        if self._status == NodeStatus.RUNNING:
            self._status = NodeStatus.CANCELING
            self._shutdown_event.set()

    def _check_cancelled(self):
        if self._shutdown_event.is_set() or (self._ctx and self._ctx.is_cancelled()):
            raise InterruptedError()

    def get_duration(self) -> float:
        return (self._end_time or time.time()) - self._start_time if self._start_time > 0 else 0

    def get_progress(self) -> Any:
        return self._current_progress

    def _get_config_data(self) -> Dict[str, Any]:
        """获取节点的配置/身份参数（不含运行状态）"""
        return {
            "node_id": self._node_id,
            "input_uri": self._input_uri,
            "output_uri": self._output_uri,
            "protocol_prefix": self._protocol_prefix,
            "base_path": self._base_path
        }

    def get_runtime(self) -> Dict[str, Any]:
        """获取完整的运行时快照：配置参数 + 物理状态"""
        data = self._get_config_data()
        data.update({
            "status": self.status.value if hasattr(self.status, 'value') else self.status,
            "progress": self.progress,
            "duration": self.get_duration()
        })
        return data

    def resume_from_runtime(self, runtime_data: Dict[str, Any]) -> None:
        self._node_id = runtime_data.get("node_id", self._node_id)
        self._input_uri = runtime_data.get("input_uri", self._input_uri)
        self._output_uri = runtime_data.get("output_uri", self._output_uri)
        self._protocol_prefix = runtime_data.get("protocol_prefix", self._protocol_prefix)
        self._base_path = runtime_data.get("base_path", self._base_path)
        
        # 核心修复：优先从快照恢复状态
        if "status" in runtime_data:
            st = runtime_data["status"]
            if isinstance(st, str):
                # 尝试从字符串转换回 Enum
                for member in NodeStatus:
                    if member.value == st:
                        self._status = member
                        break
            elif isinstance(st, NodeStatus):
                self._status = st
        
        # 兜底：如果没标记完成，则标记为恢复中
        if self._status != NodeStatus.COMPLETED:
            self._status = NodeStatus.RESUMING
            
        prog = runtime_data.get("progress", {})
        self._current_progress = prog.get("current", 0) if isinstance(prog, dict) else (prog or 0)
        self._total_progress = prog.get("total", 0) if isinstance(prog, dict) else 0
        
        # 核心修复：必须记录恢复位点，以便 open() 时传给 Reader
        self._resume_progress = self._current_progress
        
        self._shutdown_event.clear()


# ==============================================================================
# 2. 独立引擎实现：BatchNode & ParallelBatchNode
# ==============================================================================

class BatchNode(BaseNode, IBatchNode):
    """原子引擎：顺序批处理"""
    def __init__(self, 
                 node_id: str = None, 
                 input_uri: Optional[str] = None,
                 output_uri: Optional[str] = None,
                 protocol_prefix: str = "",
                 base_path: str = "",
                 batch_size: int = 1):
        super().__init__(node_id=node_id, input_uri=input_uri, output_uri=output_uri, 
                         protocol_prefix=protocol_prefix, base_path=base_path)
        self._batch_size = batch_size
        self._processor: Optional[Callable[[List[Any], Any], List[Any]]] = None

    def set_processor(self, func: Callable[[List[Any], Any], List[Any]]):
        """设置业务处理函数"""
        self._processor = func

    def process_batch(self, data: List[Any]) -> List[Any]:
        if self._processor:
            return self._processor(data, self._ctx)
        return data

    def run(self):
        # 核心修复：只要不是完成或取消，都可以尝试运行 (包含 RESUMING, FAILED)
        if self._status not in [NodeStatus.COMPLETED, NodeStatus.CANCELED]:
            if not getattr(self, '_reader', None):
                self.open(ctx=self._ctx)
        
        reader = getattr(self, '_reader', None)
        writer = getattr(self, '_writer', None)
        if not reader:
            _node_logger.warning(f"{self.log_id} ⚠️ 引擎未就绪 (无 Reader)，跳过执行")
            return

        # 核心修复：同步总量（流式模式下总量会随上游写入而增长）
        self._total_progress = reader.total_count

        try:
            for data, ids in reader.read(batch_size=self._batch_size):
                self._check_cancelled()
                
                # At-most-once: 读后即存。优点：绝不重复执行（省 Token）；缺点：崩溃会导致本批次丢数。
                self._current_progress = reader.completed_count
                if self._total_progress <= 0 or reader.total_count > self._total_progress:
                    self._total_progress = reader.total_count

                if self._ctx: 
                    self._ctx.report_progress(self._current_progress, self._total_progress)
                    if hasattr(self._ctx, "save_checkpoint"):
                        self._ctx.save_checkpoint()

                # 丰富日志格式
                idx_range = f"{ids[0]}~{ids[-1]}" if len(ids) > 1 else f"{ids[0]}"
                _node_logger.info(f"{self.log_id} 📥 读取批次: 数量={len(data)}, 索引={idx_range}")
                
                processed = self.process_batch(data)
                if writer and processed:
                    writer.write(processed, anchors=ids)
                    _node_logger.info(f"{self.log_id} 📤 写入完成: 成功={len(processed)}条")
            
            if self._ctx: self._ctx.report_progress(self._total_progress, self._total_progress)
            
        except InterruptedError:
            self.cancel(); raise
        except Exception as e:
            _node_logger.error(f"{self.log_id} 🚨 运行崩溃: {e}", exc_info=True)
            self._status = NodeStatus.FAILED; raise
        finally:
            self.close()

    def _get_config_data(self) -> Dict[str, Any]:
        data = super()._get_config_data()
        data.update({"batch_size": self._batch_size})
        return data

class ParallelBatchNode(BatchNode):
    """原子引擎：并行批处理"""
    def __init__(self, 
                 node_id: str = None, 
                 input_uri: Optional[str] = None,
                 output_uri: Optional[str] = None,
                 protocol_prefix: str = "",
                 base_path: str = "",
                 batch_size: int = 1, 
                 parallel_size: int = 1):
        super().__init__(node_id=node_id, input_uri=input_uri, output_uri=output_uri, 
                         protocol_prefix=protocol_prefix, base_path=base_path, batch_size=batch_size)
        self._parallel_size = parallel_size

    def run(self):
        # 核心修复：只要不是完成或取消，都可以尝试运行 (包含 RESUMING, FAILED)
        if self._status not in [NodeStatus.COMPLETED, NodeStatus.CANCELED]:
            if not getattr(self, '_reader', None):
                self.open(ctx=self._ctx)
        
        reader = getattr(self, '_reader', None)
        writer = getattr(self, '_writer', None)
        if not reader:
            _node_logger.warning(f"{self.log_id} ⚠️ 引擎未就绪 (无 Reader)，跳过执行")
            return

        # 核心修复：只同步总量
        self._total_progress = reader.total_count

        # 极致背压控制
        semaphore = threading.BoundedSemaphore(self._parallel_size)
        futures = set()

        try:
            with ThreadPoolExecutor(max_workers=self._parallel_size) as executor:
                for data, ids in reader.read(batch_size=self._batch_size):
                    self._check_cancelled()
                    
                    # 1. 阻塞点：如果线程池太忙，此处会阻塞
                    semaphore.acquire()
                    
                    # 2. At-most-once: 派发前即存盘。确保任务一旦进入队列即视为已处理，即便子线程崩溃也不重跑。
                    self._current_progress = reader.completed_count
                    if self._total_progress <= 0 or reader.total_count > self._total_progress:
                        self._total_progress = reader.total_count
                    
                    if self._ctx: 
                        self._ctx.report_progress(self._current_progress, self._total_progress)
                        if hasattr(self._ctx, "save_checkpoint"):
                            self._ctx.save_checkpoint()

                    # 丰富日志格式
                    idx_range = f"{ids[0]}~{ids[-1]}" if len(ids) > 1 else f"{ids[0]}"
                    _node_logger.info(f"{self.log_id} 📥 读取批次(并行): 数量={len(data)}, 索引={idx_range}")
                    
                    def _safe_task(d, i):
                        try:
                            res = self.process_batch(d)
                            if writer and res:
                                writer.write(res, anchors=i)
                                _node_logger.info(f"{self.log_id} ✅ 并行批次完成: {len(res)} 条")
                        except Exception as e:
                            _node_logger.error(f"{self.log_id} ❌ 并行处理失败: {e}", exc_info=True)
                            raise
                        finally:
                            # 任务结束，释放信号量
                            semaphore.release()

                    future = executor.submit(_safe_task, data, ids)
                    futures.add(future)
                    
                    # 5. 及时清理已完成的 future 并检查异常
                    done_fs = {f for f in futures if f.done()}
                    for f in done_fs:
                        f.result() # 如果子线程崩溃，此处会抛出异常
                        futures.remove(f)
                
                # 6. 等待所有剩余任务完成
                for f in futures:
                    f.result() 
            
            # 7. 最终进度对齐
            if self._ctx: self._ctx.report_progress(self._total_progress, self._total_progress)
                
        except InterruptedError:
            self.cancel(); raise
        except Exception as e:
            self._status = NodeStatus.FAILED; raise
        finally:
            self.close()

    def _get_config_data(self) -> Dict[str, Any]:
        data = super()._get_config_data()
        data.update({"parallel_size": self._parallel_size})
        return data

# ==============================================================================
# 3. OperatorNode 实现：组合容器 (支持内部并发适配)
# ==============================================================================

class OperatorNode(BatchNode):
    """通用算子容器：自动识别并适配 Batch/Single 算子，支持批次内并发"""
    def __init__(self, 
                 node_id: str, 
                 operator: IOperator, 
                 input_uri: Optional[str] = None,
                 output_uri: Optional[str] = None,
                 protocol_prefix: str = "",
                 base_path: str = "",
                 batch_size: int = 1):
        super().__init__(node_id=node_id, input_uri=input_uri, output_uri=output_uri, 
                         protocol_prefix=protocol_prefix, base_path=base_path, batch_size=batch_size)
        self._operator = operator

    def process_batch(self, data: List[Any]) -> List[Any]:
        # 1. 场景 A：算子原生支持批量 (IBatchOperator) -> 直接交给算子执行
        if hasattr(self._operator, "process_batch"):
            return self._operator.process_batch(data, ctx=self._ctx)
        
        # 2. 场景 B：算子只有单条处理能力 (ISingleOperator) -> 容器层根据 batch_size 执行批次内并发
        if hasattr(self._operator, "process_item"):
            current_batch_size = len(data)
            
            # 如果批次大于 1，则开启批次内并发以压榨性能
            if current_batch_size > 1:
                with ThreadPoolExecutor(max_workers=current_batch_size) as executor:
                    futures = [executor.submit(self._operator.process_item, item, self._ctx) for item in data]
                    results = []
                    for f in futures:
                        res = f.result()
                        # 兼容 1:N 爆炸分发与过滤
                        if isinstance(res, list): results.extend(res)
                        elif res is not None: results.append(res)
                    return results
            else:
                # 只有 1 条数据时，保持串行调用
                res = self._operator.process_item(data[0], ctx=self._ctx)
                if isinstance(res, list): return res
                return [res] if res is not None else []
        
        raise TypeError(f"算子 {self._operator.__class__.__name__} 未实现 process_batch 或 process_item")

class ParallelOperatorNode(ParallelBatchNode):
    """并行算子容器：手动实现自适应 process_batch 以规避 MRO 冲突"""
    def __init__(self, 
                 node_id: str, 
                 operator: IOperator, 
                 input_uri: Optional[str] = None,
                 output_uri: Optional[str] = None,
                 protocol_prefix: str = "",
                 base_path: str = "",
                 batch_size: int = 1, 
                 parallel_size: int = 1):
        super().__init__(node_id=node_id, input_uri=input_uri, output_uri=output_uri, 
                         protocol_prefix=protocol_prefix, base_path=base_path, 
                         batch_size=batch_size, parallel_size=parallel_size)
        self._operator = operator

    def process_batch(self, data: List[Any]) -> List[Any]:
        # 1. 场景 A：算子原生支持批量 (IBatchOperator) -> 直接交给算子执行
        if hasattr(self._operator, "process_batch"):
            return self._operator.process_batch(data, ctx=self._ctx)
        
        # 2. 场景 B：算子只有单条处理能力 (ISingleOperator) -> 容器层根据 batch_size 执行批次内并发
        if hasattr(self._operator, "process_item"):
            current_batch_size = len(data)
            
            # 如果批次大于 1，则开启批次内并发以压榨性能
            if current_batch_size > 1:
                with ThreadPoolExecutor(max_workers=current_batch_size) as executor:
                    futures = [executor.submit(self._operator.process_item, item, self._ctx) for item in data]
                    results = []
                    for f in futures:
                        res = f.result()
                        # 兼容 1:N 爆炸分发与过滤
                        if isinstance(res, list): results.extend(res)
                        elif res is not None: results.append(res)
                    return results
            else:
                # 只有 1 条数据时，保持串行调用
                res = self._operator.process_item(data[0], ctx=self._ctx)
                if isinstance(res, list): return res
                return [res] if res is not None else []
        
        raise TypeError(f"算子 {self._operator.__class__.__name__} 未实现 process_batch 或 process_item")

# 向后兼容别名：现在所有算子节点都具备自适应能力
BatchOperatorNode = OperatorNode
SingleOperatorNode = OperatorNode
ParallelBatchOperatorNode = ParallelOperatorNode
ParallelSingleOperatorNode = ParallelOperatorNode


# ==============================================================================
# 4. 统一代理：作为蓝图参数的终点持有者
# ==============================================================================

class UnifiedNode(BaseNode):
    """代理节点：持有所有意图参数并在运行时实例化引擎"""
    def __init__(self, 
                 node_id: str = None,
                 input_uri: Optional[str] = None,
                 output_uri: Optional[str] = None,
                 protocol_prefix: str = "",
                 base_path: str = "",
                 batch_size: int = 1, 
                 parallel_size: int = 1):
        super().__init__(node_id=node_id, input_uri=input_uri, output_uri=output_uri, 
                         protocol_prefix=protocol_prefix, base_path=base_path)
        self._batch_size = batch_size
        self._parallel_size = parallel_size
        self._processor = None
        self._impl: Optional[BatchNode] = None

    def set_processor(self, func):
        """设置业务处理函数，并同步给内部引擎"""
        self._processor = func
        if self._impl:
            self._impl.set_processor(func)

    def _ensure_impl(self):
        if self._impl: return
        
        # 核心：完全显式初始化引擎
        if self._parallel_size > 1:
            self._impl = ParallelBatchNode(
                node_id=self._node_id,
                input_uri=self._input_uri,
                output_uri=self._output_uri,
                protocol_prefix=self._protocol_prefix,
                base_path=self._base_path,
                batch_size=self._batch_size,
                parallel_size=self._parallel_size
            )
        else:
            self._impl = BatchNode(
                node_id=self._node_id,
                input_uri=self._input_uri,
                output_uri=self._output_uri,
                protocol_prefix=self._protocol_prefix,
                base_path=self._base_path,
                batch_size=self._batch_size
            )
            
        # 同步处理器
        if self._processor:
            self._impl.set_processor(self._processor)
            
        # 核心修复：将门面暂存的“真相位点”同步给内部引擎
        facade_rt = {
            "node_id": self._node_id,
            "status": self._status,
            "progress": {"current": self._current_progress, "total": self._total_progress},
            "batch_size": self._batch_size,
            "parallel_size": self._parallel_size,
            "input_uri": self._input_uri,
            "output_uri": self._output_uri,
            "protocol_prefix": self._protocol_prefix,
            "base_path": self._base_path
        }
        self._impl.resume_from_runtime(facade_rt)

        if self._ctx: self._impl.set_context(self._ctx)
        if self._input_stream: self._impl.bind_io(self._input_stream, self._output_stream)

    @property
    def status(self) -> NodeStatus: return self._impl.status if self._impl else self._status
    
    @status.setter
    def status(self, value: NodeStatus):
        self._status = value
        if self._impl: self._impl.status = value

    @property
    def progress(self) -> Dict[str, int]: return self._impl.progress if self._impl else super().progress

    def bind_io(self, in_s, out_s):
        super().bind_io(in_s, out_s)
        if self._impl: self._impl.bind_io(in_s, out_s)
    
    def set_context(self, ctx: NodeContextImpl):
        super().set_context(ctx)
        if self._impl: self._impl.set_context(ctx)

    def open(self, ctx=None, progress=None):
        if ctx: self.set_context(ctx)
        self._ensure_impl()
        # 彻底委托给内部引擎，同步位点
        self._impl.open(ctx=self._ctx, progress=progress)
        self._status = self._impl.status
        self._current_progress = self._impl._current_progress
        self._total_progress = self._impl._total_progress

    def run(self): 
        self._ensure_impl()
        # 核心修复：增加幂等保护，只有在未准备好 Reader 时才调用 open
        # 这防止了 StreamingPipeline 的“预热阶段”与“执行阶段”重复打开 IO
        if self._status not in [NodeStatus.COMPLETED, NodeStatus.CANCELED]:
            if self._impl and not getattr(self._impl, '_reader', None):
                self.open(ctx=self._ctx)
            
        # 内部引擎执行
        self._impl.run()
        # 同步状态和进度给门面
        self._status = self._impl.status
        self._current_progress = self._impl._current_progress
        self._total_progress = self._impl._total_progress

    def close(self): 
        if self._impl: 
            self._impl.close()
            self._status = self._impl.status
        super().close()
    def cancel(self): 
        if self._impl: self._impl.cancel()
    def get_duration(self) -> float: 
        return self._impl.get_duration() if self._impl else super().get_duration()
    def get_progress(self) -> Any: 
        return self._impl.get_progress() if self._impl else self._current_progress
    def get_runtime(self) -> Dict[str, Any]: 
        rt = self._impl.get_runtime() if self._impl else super().get_runtime()
        rt.update({"batch_size": self._batch_size, "parallel_size": self._parallel_size})
        return rt

    def resume_from_runtime(self, runtime_data: Dict[str, Any]) -> None: 
        super().resume_from_runtime(runtime_data)
        self._batch_size = runtime_data.get("batch_size", self._batch_size)
        self._parallel_size = runtime_data.get("parallel_size", self._parallel_size)
        if self._impl: self._impl.resume_from_runtime(runtime_data)

class UnifiedOperatorNode(UnifiedNode):
    def __init__(self, 
                 operator: IOperator,
                 node_id: str = None,
                 input_uri: Optional[str] = None,
                 output_uri: Optional[str] = None,
                 protocol_prefix: str = "",
                 base_path: str = "",
                 batch_size: int = 1, 
                 parallel_size: int = 1):
        super().__init__(node_id=node_id, input_uri=input_uri, output_uri=output_uri, 
                         protocol_prefix=protocol_prefix, base_path=base_path, 
                         batch_size=batch_size, parallel_size=parallel_size)
        self._operator = operator

    def _ensure_impl(self):
        if self._impl: return
        
        # 核心：根据并发规模选择容器，算子内部的多态由 OperatorNode 自行处理
        if self._parallel_size > 1:
            self._impl = ParallelOperatorNode(
                node_id=self._node_id,
                operator=self._operator,
                input_uri=self._input_uri,
                output_uri=self._output_uri,
                protocol_prefix=self._protocol_prefix,
                base_path=self._base_path,
                batch_size=self._batch_size,
                parallel_size=self._parallel_size
            )
        else:
            self._impl = OperatorNode(
                node_id=self._node_id,
                operator=self._operator,
                input_uri=self._input_uri,
                output_uri=self._output_uri,
                protocol_prefix=self._protocol_prefix,
                base_path=self._base_path,
                batch_size=self._batch_size
            )
        
        # 核心修复：将门面暂存的“真相位点”同步给内部引擎
        facade_rt = {
            "node_id": self._node_id,
            "status": self._status,
            "progress": {"current": self._current_progress, "total": self._total_progress},
            "batch_size": self._batch_size,
            "parallel_size": self._parallel_size,
            "input_uri": self._input_uri,
            "output_uri": self._output_uri,
            "protocol_prefix": self._protocol_prefix,
            "base_path": self._base_path
        }
        self._impl.resume_from_runtime(facade_rt)

        if self._ctx: self._impl.set_context(self._ctx)
        if self._input_stream: self._impl.bind_io(self._input_stream, self._output_stream)

    def get_runtime(self) -> Dict[str, Any]:
        rt = super().get_runtime()
        rt["operator_type"] = self._operator.__class__.__name__ if self._operator else None
        return rt

class InputNode(UnifiedNode):
    def __init__(self, 
                 input_uri: Optional[str] = None,
                 output_uri: Optional[str] = None,
                 protocol_prefix: str = "",
                 base_path: str = "",
                 batch_size: int = 1, 
                 parallel_size: int = 1):
        super().__init__(node_id="input", input_uri=input_uri, output_uri=output_uri, 
                         protocol_prefix=protocol_prefix, base_path=base_path, 
                         batch_size=batch_size, parallel_size=parallel_size)

    def open(self, ctx=None, progress=None):
        super().open(ctx, progress)
        # 核心修复：InputNode 的输入源必然是静态的（如初始 JSONL 文件）
        # 我们必须给它的 Reader Channel 贴上 EOF 标签，否则 StreamBridge 会死等封条
        if self._impl and hasattr(self._impl, "_reader") and self._impl._reader:
            if hasattr(self._impl._reader, "channel"):
                self._impl._reader.channel.set_eof()
                _node_logger.info(f"{self.log_id} 已将输入源标记为静态 (EOF)")

class OutputNode(UnifiedNode):
    def __init__(self, 
                 input_uri: Optional[str] = None,
                 output_uri: Optional[str] = None,
                 protocol_prefix: str = "",
                 base_path: str = "",
                 batch_size: int = 1, 
                 parallel_size: int = 1):
        super().__init__(node_id="output", input_uri=input_uri, output_uri=output_uri, 
                         protocol_prefix=protocol_prefix, base_path=base_path, 
                         batch_size=batch_size, parallel_size=parallel_size)
