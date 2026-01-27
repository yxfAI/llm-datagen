"""
Pipeline 引擎实现：彻底解耦执行引擎与算子编排。
"""
import threading
import signal
import time
import os
import logging
from abc import ABC, abstractmethod 
from typing import Any, List, Dict, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# ------------------- 平台级 Logger 配置 -------------------
_platform_logger = logging.getLogger("DataGen.Platform")
_platform_logger.setLevel(logging.INFO)
_platform_logger.propagate = False
if not _platform_logger.handlers:
    try:
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "platform.log")
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(formatter)
        _platform_logger.addHandler(file_handler)
    except Exception:
        pass

from llm_datagen.core.pipeline import (
    IPipeline, ISequentialPipeline, IStreamingPipeline, IRecoverablePipeline,
    PipelineStatus
)
from llm_datagen.core.config import NodeConfig, WriterConfig
from llm_datagen.core.node import INode, IRecoverableNode, NodeStatus
from llm_datagen.core.operators import IOperator
from llm_datagen.core.hooks import (
    IPipelineHooks,
    DefaultPipelineHooks, 
    JsonFileCheckpointHooks,
    PipelineHooksAdapter
)
from llm_datagen.impl.node import NodeContextImpl, UnifiedNode, UnifiedOperatorNode, InputNode, OutputNode
from llm_datagen.impl.bus.bus import RecoverableStreamFactory, StreamFactory, get_protocol_extension

class PipelineContextImpl:
    """Pipeline 执行上下文：管理全局信号"""
    def __init__(self, is_cancelled_func: Callable[[], bool]):
        self._is_cancelled = is_cancelled_func
    def is_cancelled(self) -> bool: return self._is_cancelled() if self._is_cancelled else False


# ==============================================================================
# 1. BasePipeline: 核心身份与状态 management
# ==============================================================================

class BasePipeline(IRecoverablePipeline, ABC):
    """
    最基础的 Pipeline 抽象类。
    承担标识、状态、结果目录及基础生命周期管理。
    """
    LOG_TAG = "Pipeline"

    def __init__(self, hooks: Optional[IPipelineHooks] = None, results_dir: str = "tmp/results", writer_config: Optional[WriterConfig] = None):
        self.results_dir = results_dir
        self.writer_config = writer_config or WriterConfig()
        self._hooks = self._init_hooks(hooks)
        self._hooks_adapter = PipelineHooksAdapter(self._hooks)
        
        self._pipeline_id = None
        self._status = PipelineStatus.PENDING.value
        self._nodes: List[INode] = []
        self._ctx: Optional[PipelineContextImpl] = None
        self._shutdown_event = threading.Event()
        self._start_time = 0
        self._end_time = 0
        self.config = {}

    def _init_hooks(self, hooks: Optional[IPipelineHooks]) -> IPipelineHooks:
        """智能初始化持久化钩子，确保 Composite 模式下也能正确指向 results_dir"""
        from llm_datagen.core.hooks import CompositePipelineHooks
        def is_persistence(h): return isinstance(h, JsonFileCheckpointHooks)
        
        if hooks:
            # 确保传入的自定义 Hooks 也指向同一个 results_dir
            if hasattr(hooks, 'base_dir'): hooks.base_dir = self.results_dir
            elif hasattr(hooks, 'hooks'):
                for h in hooks.hooks:
                    if hasattr(h, 'base_dir'): h.base_dir = self.results_dir

            has_p = is_persistence(hooks) or (hasattr(hooks, 'hooks') and any(is_persistence(h) for h in hooks.hooks))
            if has_p:
                return hooks
            else:
                return CompositePipelineHooks([JsonFileCheckpointHooks(base_dir=self.results_dir), hooks])
        return JsonFileCheckpointHooks(base_dir=self.results_dir)

    @property
    def nodes(self) -> List[INode]: return self._nodes

    @nodes.setter
    def nodes(self, value: List[INode]): self._nodes = value

    @property
    def pipeline_id(self) -> str: return self._pipeline_id

    @property
    def status(self) -> str: return self._status

    @status.setter
    def status(self, value: str): self._status = value

    def get_duration(self) -> float:
        if self._start_time == 0: return 0
        return (self._end_time or time.time()) - self._start_time

    def cancel(self):
        if self._status == PipelineStatus.CANCELING.value or self._status == PipelineStatus.CANCELED.value:
            return
            
        self._status = PipelineStatus.CANCELING.value
        _platform_logger.warning(f"🛑 Pipeline 进入取消中状态: {self.pipeline_id}")
        self._shutdown_event.set()
        for node in self._nodes:
            if hasattr(node, 'cancel'): node.cancel()

    def save_checkpoint(self, node: Optional[INode] = None):
        """保存进度、状态以及消耗：委托给 HooksAdapter"""
        if node:
            if isinstance(node, IRecoverableNode):
                cp = node.get_progress()
                if cp is not None:
                    # 1. 判定物理状态
                    node_status = "running"
                    if node.status == NodeStatus.COMPLETED: node_status = "completed"
                    elif node.status == NodeStatus.FAILED: node_status = "failed"
                    
                    # 2. 提取当前已发生的 Token 消耗
                    usage = {}
                    if hasattr(self._hooks, 'node_usages'):
                        usage = self._hooks.node_usages.get(node.node_id, {})
                    
                    # 3. 写入全量检查点
                    self._hooks_adapter.on_checkpoint(self.pipeline_id, node.node_id, {
                        "current": cp.get("current", 0) if isinstance(cp, dict) else cp,
                        "total": cp.get("total", 0) if isinstance(cp, dict) else 0,
                        "status": node_status,
                        "usage": usage # 核心：Token 消耗也上车
                    })
        else:
            for n in self._nodes:
                self.save_checkpoint(n)

    def get_runtime(self) -> Dict[str, Any]:
        """获取运行时身份快照（最简基础数据）"""
        return {
            "pipeline_id": self.pipeline_id,
            "status": self.status,
            "duration": self.get_duration(),
            "config": self.config,
            "writer_config": self.writer_config.to_dict()
        }

    def save_runtime(self, file_path: Optional[str] = None):
        """保存运行时蓝图到磁盘"""
        import json
        path = file_path or os.path.join(self.results_dir, self.pipeline_id, "runtime.json")
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.get_runtime(), f, indent=2, ensure_ascii=False)
        
        tag = getattr(self, "LOG_TAG", "Pipeline")
        _platform_logger.info(f"💾 [{tag}] 运行时蓝图已保存: {path}")

    @abstractmethod
    def resume_from_runtime(self, runtime_data: Dict[str, Any]):
        """子类必须实现恢复逻辑"""
        pass

    def resume(self, pipeline_id: str):
        """自动化恢复：根据 Pipeline ID 自动从磁盘加载蓝图并完成复活"""
        import json
        runtime_path = os.path.join(self.results_dir, pipeline_id, "runtime.json")
        if not os.path.exists(runtime_path):
            raise FileNotFoundError(f"无法找到 Pipeline '{pipeline_id}' 的蓝图文件: {runtime_path}")
            
        with open(runtime_path, "r", encoding="utf-8") as f:
            runtime_data = json.load(f)
            
        return self.resume_from_runtime(runtime_data)

    def resume_from_file(self, file_path: str):
        import json
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Checkpoint file not found: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            runtime_data = json.load(f)
        return self.resume_from_runtime(runtime_data)


# ==============================================================================
# 2. NodePipeline: 节点驱动层 (只知道 Node，不知道 Operator)
# ==============================================================================

class NodePipeline(BasePipeline):
    """
    节点执行基类：纯粹的执行引擎。
    管理 Node 列表的声明周期（open/close）、信号处理及位点恢复。
    """
    def open(self):
        self._start_time = time.time()
        
        # 核心修复：如果已经在恢复中，保持 RESUMING 状态，直到真正运行
        if self._status != PipelineStatus.RESUMING.value:
            self._status = PipelineStatus.RUNNING.value
            
        _platform_logger.info(f"🚀 [Engine] Pipeline 启动: {self.pipeline_id} (Mode={'Resume' if self._status == PipelineStatus.RESUMING.value else 'New'})")
        
        self._setup_signal_handlers()
        
        if not self._ctx:
            self._ctx = PipelineContextImpl(lambda: self._shutdown_event.is_set())

        self._hooks_adapter.on_pipeline_start(self.pipeline_id, self.config)
        
        # 为每个物理节点注入 Context
        for node in self._nodes:
            # 注入全局写入配置
            if hasattr(node, "set_writer_config"):
                node.set_writer_config(self.writer_config)
            
            node_ctx = self._create_node_context(node)
            if hasattr(node, "set_context"): 
                node.set_context(node_ctx)

    def _setup_signal_handlers(self):
        def handler(sig, frame): self.cancel()
        if threading.current_thread() is threading.main_thread():
            try: 
                signal.signal(signal.SIGINT, handler)
                signal.signal(signal.SIGTERM, handler)
            except: pass

    def close(self, success: bool, error: Exception = None):
        self._end_time = time.time()
        self._status = PipelineStatus.COMPLETED.value if success else PipelineStatus.FAILED.value
        
        status_str = "SUCCESS" if success else f"FAILED ({error})"
        _platform_logger.info(f"🏁 [Engine] Pipeline 结束: {self.pipeline_id} | Status: {status_str} | Duration: {self.get_duration():.2f}s")
        
        self._hooks_adapter.on_pipeline_end(self.pipeline_id, success, error)

    def resume_from_runtime(self, runtime_data: Dict[str, Any]):
        """执行引擎层的恢复：仅负责 Hooks 状态和物理节点的位点同步"""
        self._pipeline_id = runtime_data.get("pipeline_id")
        self._status = PipelineStatus.RESUMING.value 
        
        # 0. 恢复写入策略
        if "writer_config" in runtime_data:
            from llm_datagen.core.config import WriterConfig
            self.writer_config = WriterConfig(**runtime_data["writer_config"])
            
        # 1. 恢复 Hooks 业务状态 (如有)
        if "hook_state" in runtime_data and runtime_data["hook_state"] and hasattr(self._hooks, "load_state_data"):
            self._hooks.load_state_data(runtime_data["hook_state"])
            
        # 2. 核心：加载磁盘持久化位点 (这是最实时的真理来源)
        if hasattr(self._hooks, "load_state"): 
            self._hooks.load_state(self.pipeline_id, self.config)

        # 3. 遍历当前物理节点并注入恢复状态
        for node in self._nodes:
            # 获取该节点的实时检查点 (这是磁盘上最新的真相)
            cp = None
            if hasattr(self._hooks, 'get_checkpoint'):
                cp = self._hooks.get_checkpoint(node.node_id)
            
            # 准备节点的恢复快照
            node_rt = next((n for n in runtime_data.get("nodes", []) if n.get("node_id") == node.node_id), {})
            
            # 融合 Checkpoint：如果磁盘有更新的进度，覆盖快照中的旧值
            if cp and isinstance(cp, dict):
                curr = cp.get("current", 0)
                total = cp.get("total", 0)
                status = cp.get("status", "running")
                
                # 显式注入状态给快照对象
                node_rt["progress"] = {"current": curr, "total": total}
                
                # 核心修复：不再通过进度猜完成状态，只相信显式的 status 标记
                # 尤其是在流式模式下，curr >= total 在崩溃时往往是成立的假象
                if status == "completed":
                    node_rt["status"] = NodeStatus.COMPLETED
                elif status == "failed":
                    node_rt["status"] = NodeStatus.FAILED
                else:
                    node_rt["status"] = NodeStatus.RESUMING
            
            # 注入恢复
            node.resume_from_runtime(runtime_data=node_rt)
            
            # 核心改进：对于已完成的节点，确保其物理封条存在，防止下游阻塞
            if node.status == NodeStatus.COMPLETED:
                if hasattr(node, "output_stream") and node.output_stream:
                    if hasattr(node.output_stream, "seal"):
                        node.output_stream.seal()
            
            _platform_logger.info(f"🧬 [Engine] 已为节点 {node.node_id} 注入恢复位点: {cp}")
            
        return self

    def _create_node_context(self, node: INode):
        pipeline_id = self.pipeline_id
        
        def _on_usage(m):
            # 自动嗅探格式并转发，确保 Token 统计正确
            provider = m.get("provider", "unknown")
            model = m.get("model", "unknown")
            # 过滤掉非数值字段，只传递真正的 metrics 指标
            numeric_metrics = {k: v for k, v in m.items() if isinstance(v, (int, float)) and k not in ["provider", "model"]}
            self._hooks_adapter.on_usage(pipeline_id, node.node_id, provider, model, numeric_metrics)

        return NodeContextImpl(
            node_id=node.node_id, 
            context_id=pipeline_id, 
            on_progress=lambda curr, total, meta, nid=node.node_id: self._hooks_adapter.on_node_progress(pipeline_id, nid, curr, total, meta),
            on_usage=_on_usage,
            on_log=lambda msg, lv, nid=node.node_id: self._hooks_adapter.on_node_log(pipeline_id, nid, msg, lv),
            on_error=lambda e, items, nid=node.node_id: self._hooks_adapter.on_node_error(pipeline_id, nid, e, items),
            is_cancelled_func=self._ctx.is_cancelled,
            # 核心改进：允许 Node 主动触发 Pipeline 存盘
            save_checkpoint_func=lambda nid=node.node_id: self.save_checkpoint(node)
        )


class SequentialPipeline(NodePipeline, ISequentialPipeline):
    """顺序执行引擎：逐个运行节点"""
    def run(self):
        self._shutdown_event.clear()
        self.open()
        
        _platform_logger.info(f"🚀 [Sequential] Pipeline 启动: {self.pipeline_id}")
        
        success = True; error = None
        try:
            for node in self.nodes:
                if self._shutdown_event.is_set(): break
                
                # 核心改进：跳过已完成的节点 (恢复模式的关键)
                if node.status == NodeStatus.COMPLETED:
                    _platform_logger.info(f"⏭️  节点 {node.node_id} 已在历史记录中完成，跳过")
                    continue

                print(f"\n🎬 正在运行节点: {node.node_id}")
                try:
                    self._hooks_adapter.on_node_start(self.pipeline_id, node.node_id, {})
                    node.run()
                    # 运行成功，立即显式更新并保存检查点
                    node.status = NodeStatus.COMPLETED
                    self.save_checkpoint(node)
                    self._hooks_adapter.on_node_finish(self.pipeline_id, node.node_id)
                except Exception as e:
                    # 运行失败，立即显式更新并保存检查点
                    node.status = NodeStatus.FAILED
                    self.save_checkpoint(node)
                    self._hooks_adapter.on_node_error(self.pipeline_id, node.node_id, e, [])
                    raise
                finally: node.close()
        except Exception as e:
            success = False; error = e; raise
        finally: self.close(success, error)


class StreamingPipeline(NodePipeline, IStreamingPipeline):
    """流式执行引擎：并行运行节点"""
    def run(self):
        self._shutdown_event.clear()
        self.open()
        
        print(f"\n🚀 [Streaming] 正在并行启动所有节点...")
        _platform_logger.info(f"🚀 [Streaming] Pipeline 启动: {self.pipeline_id}")
            
        success = True; error = None
        try:
            # 核心修复：同步预热
            # 在启动并行线程池之前，先同步执行所有非完成节点的 open()
            # 确保上游的 unseal (撕封条) 动作绝对领先于下游 Reader 对 EOF 的判定
            for node in self.nodes:
                if node.status != NodeStatus.COMPLETED:
                    # 预先注入 Context 并执行 open
                    node_ctx = self._create_node_context(node)
                    node.open(ctx=node_ctx)

            for node in self.nodes:
                if node.status == NodeStatus.COMPLETED:
                    _platform_logger.info(f"⏭️  [Streaming] 节点 {node.node_id} 已完成，跳过调度")
                    continue
                self._hooks_adapter.on_node_start(self.pipeline_id, node.node_id, {})

            with ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
                futures = {executor.submit(node.run): node for node in self.nodes if node.status != NodeStatus.COMPLETED}
                pending = set(futures.keys())
                while pending:
                    if self._shutdown_event.is_set():
                        for f in pending: f.cancel()
                        break
                    done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                    for f in done:
                        node = futures[f]
                        try:
                            f.result()
                            # 只有在非取消状态下正常结束，才标记为已完成
                            if not self._shutdown_event.is_set():
                                node.status = NodeStatus.COMPLETED
                                self.save_checkpoint(node)
                                self._hooks_adapter.on_node_finish(self.pipeline_id, node.node_id)
                            node.close()
                        except Exception as e:
                            _platform_logger.error(f"🚨 [Streaming] 节点 {node.node_id} 异常，正在取消 Pipeline...")
                            node.status = NodeStatus.FAILED
                            self.save_checkpoint(node)
                            self._hooks_adapter.on_node_error(self.pipeline_id, node.node_id, e, [])
                            # 核心改进：进入取消流程，不立即退出，等待其他线程
                            self.cancel()
                            success = False
                            error = e
                            break
                
                # 如果失败，等待剩余任务收尾
                if not success:
                    wait(pending, timeout=5.0)
        except Exception as e:
            success = False; error = e; raise
        finally:
            if self._status == PipelineStatus.CANCELING.value:
                self._status = PipelineStatus.CANCELED.value
            self.close(success, error)


class UnifiedNodePipeline(BasePipeline):
    """
    统一节点 Master：
    1. 直接接收物理节点列表 (INode)
    2. 负责执行引擎选型 (Streaming vs Sequential)
    3. 维持物理 Master 级参数 (streaming, batch_size, parallel_size)
    """
    LOG_TAG = "Unified"

    def __init__(self, 
                 nodes: List[INode] = None, 
                 streaming: bool = False,
                 batch_size: int = 1,
                 parallel_size: int = 1,
                 hooks: Optional[IPipelineHooks] = None,
                 results_dir: str = "tmp/results",
                 writer_config: Optional[WriterConfig] = None):
        super().__init__(hooks=hooks, results_dir=results_dir, writer_config=writer_config)
        self._nodes = nodes or []
        self._streaming = streaming
        self._batch_size = batch_size
        self._parallel_size = parallel_size
        self._impl: Optional[NodePipeline] = None

    def create(self, 
               pipeline_id: str = None,
               streaming: Optional[bool] = None):
        """物理创建入口：选型并绑定引擎"""
        self._pipeline_id = pipeline_id or f"pipe_{int(time.time() * 1000)}"
        if streaming is not None:
            self._streaming = streaming
        
        # 选型引擎实现
        engine_cls = StreamingPipeline if self._streaming else SequentialPipeline
        self._impl = engine_cls(hooks=self._hooks, results_dir=self.results_dir, writer_config=self.writer_config)
        
        # 同步状态给实现层
        self._impl._pipeline_id = self.pipeline_id
        self._impl.nodes = self._nodes
        self._impl.config = self.config
        
        # 物理创建完成后立即持久化蓝图
        self.save_runtime()

    def get_runtime(self) -> Dict[str, Any]:
        """融合 Master 配置与物理进度"""
        rt = super().get_runtime()
        rt.update({
            "streaming": self._streaming,
            "batch_size": self._batch_size,
            "parallel_size": self._parallel_size,
            "nodes": [node.get_runtime() for node in self.nodes] if self.nodes else []
        })
        return rt

    def run(self):
        if not self._impl: raise RuntimeError("Pipeline not created or resumed.")
        return self._impl.run()

    def cancel(self):
        """信号下发"""
        if self._impl: self._impl.cancel()
        else: super().cancel()

    def resume_from_runtime(self, runtime_data: Dict[str, Any]):
        """Master 级恢复：恢复配置 -> 选型引擎 -> 同步进度"""
        self._pipeline_id = runtime_data["pipeline_id"]
        self._streaming = runtime_data.get("streaming", False)
        self._batch_size = runtime_data.get("batch_size", 1)
        self._parallel_size = runtime_data.get("parallel_size", 1)
        
        # 选型引擎
        engine_cls = StreamingPipeline if self._streaming else SequentialPipeline
        self._impl = engine_cls(hooks=self._hooks, results_dir=self.results_dir, writer_config=self.writer_config)
        self._impl._pipeline_id = self.pipeline_id
        self._impl.nodes = self._nodes
        
        # 引擎层恢复实时进度
        self._impl.resume_from_runtime(runtime_data)
        return self

    # --- 属性代理 ---
    @property
    def status(self) -> str: return self._impl.status if self._impl else super().status
    @property
    def nodes(self) -> List[INode]: return self._impl.nodes if self._impl else self._nodes
    def get_duration(self) -> float: return self._impl.get_duration() if self._impl else super().get_duration()

    def __getattr__(self, name): 
        if self._impl and hasattr(self._impl, name): return getattr(self._impl, name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")


class UnifiedOperatorPipeline(UnifiedNodePipeline):
    """
    统一算子 Master：
    1. 负责逻辑算子编排 (Operators -> Nodes)
    2. 维持算子 Master 级参数 (input_uri, output_uri, ...)
    """
    LOG_TAG = "Unified"

    def __init__(self, 
                 operators: List[IOperator] = None, 
                 input_uri: str = None,
                 output_uri: str = None,
                 batch_size: int = 1,
                 parallel_size: int = 1,
                 streaming: bool = False,
                 protocol_prefix: str = "node_",
                 base_path: str = "tmp",
                 default_protocol: str = "jsonl://",
                 bus_factory: RecoverableStreamFactory = None,
                 hooks: Optional[IPipelineHooks] = None,
                 results_dir: str = "tmp/results",
                 writer_config: Optional[WriterConfig] = None):
        # 将物理参数传给 UnifiedNodePipeline
        super().__init__(nodes=[], 
                         streaming=streaming, 
                         batch_size=batch_size, 
                         parallel_size=parallel_size, 
                         hooks=hooks, 
                         results_dir=results_dir,
                         writer_config=writer_config)
        self._operators = operators or []
        self._bus_factory = bus_factory or RecoverableStreamFactory
        
        # 编排特有参数
        self._input_uri = input_uri
        self._output_uri = output_uri
        self._protocol_prefix = protocol_prefix
        self._base_path = base_path
        self._default_protocol = default_protocol

    def create(self, 
               pipeline_id: str = None,
               input_uri: Optional[str] = None, 
               output_uri: Optional[str] = None, 
               streaming: Optional[bool] = None,
               batch_size: Optional[int] = None,
               parallel_size: Optional[int] = None,
               protocol_prefix: Optional[str] = None,
               base_path: Optional[str] = None,
               default_protocol: Optional[str] = None,
               node_configs: Optional[List[NodeConfig]] = None):
        """算子编排入口：逻辑映射 + 引擎选型"""
        self._pipeline_id = pipeline_id or f"pipe_{int(time.time() * 1000)}"
        
        # 允许通过 create 覆盖配置
        if input_uri: self._input_uri = input_uri
        if output_uri: self._output_uri = output_uri
        if batch_size: self._batch_size = batch_size
        if parallel_size: self._parallel_size = parallel_size
        if protocol_prefix is not None: self._protocol_prefix = protocol_prefix
        if base_path is not None: self._base_path = base_path
        if default_protocol: self._default_protocol = default_protocol

        if not self._input_uri or not self._output_uri:
            raise ValueError(f"Pipeline '{self._pipeline_id}' requires both 'input_uri' and 'output_uri'.")
            
        _platform_logger.info(f"🏗️  正在编排逻辑蓝图: {self._pipeline_id}")
        
        plans = self._plan_topology(node_configs)
        self._weld_topology(plans)
        self._nodes = self._materialize_topology(plans)
        self._clear_streams_if_needed()

        # 2. 调用父类 (UnifiedNodePipeline) 完成引擎选型与持久化
        super().create(pipeline_id=self._pipeline_id, streaming=streaming)

    def get_runtime(self) -> Dict[str, Any]:
        """扩展算子编排特有参数"""
        rt = super().get_runtime() # 已包含 streaming, batch_size, parallel_size
        rt.update({
            "input_uri": self._input_uri,
            "output_uri": self._output_uri,
            "protocol_prefix": self._protocol_prefix,
            "base_path": self._base_path,
            "default_protocol": self._default_protocol
        })
        return rt

    def resume_from_runtime(self, runtime_data: Dict[str, Any]):
        """恢复编排参数 -> 重建蓝图 -> 恢复引擎进度"""
        self._input_uri = runtime_data.get("input_uri")
        self._output_uri = runtime_data.get("output_uri")
        self._protocol_prefix = runtime_data.get("protocol_prefix", "")
        self._base_path = runtime_data.get("base_path", "")
        self._default_protocol = runtime_data.get("default_protocol", "jsonl://")
        
        # 重建物理节点
        self._nodes = self._reconstruct_topology(runtime_data)
        
        # 恢复物理配置与引擎
        super().resume_from_runtime(runtime_data)
        return self

    # --- 逻辑编排工具方法 (保留原 BaseOperatorPipeline 的精华) ---
    def _plan_topology(self, node_configs: Optional[List[NodeConfig]] = None) -> List[Dict]:
        """构建逻辑蓝图：将算子和 I/O 搬运工统一"""
        plans = []
        plans.append({"node_id": "input", "type": "io_in", "config": {"input_uri": self._input_uri, "batch_size": self._batch_size, "parallel_size": self._parallel_size, "protocol_prefix": self._protocol_prefix, "base_path": self._base_path}})
        for i, op in enumerate(self._operators):
            raw_conf = node_configs[i] if (node_configs and i < len(node_configs)) else {}
            conf_dict = raw_conf.to_dict() if isinstance(raw_conf, NodeConfig) else raw_conf
            full_conf = {"batch_size": self._batch_size, "parallel_size": self._parallel_size, "protocol_prefix": self._protocol_prefix, "base_path": self._base_path}
            full_conf.update(conf_dict)
            plans.append({"node_id": f"node_{i}", "type": "functional", "operator": op, "config": full_conf})
        plans.append({"node_id": "output", "type": "io_out", "config": {"output_uri": self._output_uri, "batch_size": self._batch_size, "parallel_size": self._parallel_size, "protocol_prefix": self._protocol_prefix, "base_path": self._base_path}})
        return plans

    def _weld_topology(self, plans: List[Dict]):
        for i in range(len(plans)):
            conf = plans[i]["config"]
            if i > 0:
                prev_conf = plans[i-1]["config"]
                if conf.get("input_uri") and not prev_conf.get("output_uri"): prev_conf["output_uri"] = conf["input_uri"]
                elif not conf.get("input_uri") and prev_conf.get("output_uri"): conf["input_uri"] = prev_conf["output_uri"]
                elif conf.get("input_uri") and prev_conf.get("output_uri") and conf["input_uri"] != prev_conf["output_uri"]:
                    raise ValueError(f"Topology Error: URI mismatch between {plans[i-1]['node_id']} and {plans[i]['node_id']}")
        for i, plan in enumerate(plans):
            conf = plan["config"]
            if not conf.get("output_uri") and i < len(plans) - 1:
                proto = self._default_protocol; ext = get_protocol_extension(proto)
                conf["output_uri"] = f"{proto}{self._pipeline_id.strip('/')}/{plan['node_id']}{ext}"
                plans[i+1]["config"]["input_uri"] = conf["output_uri"]

    def _materialize_topology(self, plans: List[Dict]) -> List[INode]:
        final_nodes = []
        for plan in plans:
            conf = plan["config"]; nid = plan["node_id"]; ntype = plan["type"]
            in_uri = conf.get("input_uri"); out_uri = conf.get("output_uri")
            in_prefix = "" if in_uri == self._input_uri else (conf.get("protocol_prefix") or "")
            out_prefix = "" if out_uri == self._output_uri else (conf.get("protocol_prefix") or "")
            in_base = "" if in_uri == self._input_uri else (conf.get("base_path") or "")
            out_base = "" if out_uri == self._output_uri else (conf.get("base_path") or "")
            in_s = self._bus_factory.create(in_uri, protocol_prefix=in_prefix, base_path=in_base) if in_uri else StreamFactory.create(None)
            out_s = self._bus_factory.create(out_uri, protocol_prefix=out_prefix, base_path=out_base) if out_uri else StreamFactory.create(None)
            if ntype == "io_in": node = InputNode(input_uri=in_uri, output_uri=out_uri, batch_size=conf.get("batch_size", 1), parallel_size=conf.get("parallel_size", 1), protocol_prefix=conf.get("protocol_prefix", ""), base_path=conf.get("base_path", ""))
            elif ntype == "io_out": node = OutputNode(input_uri=in_uri, output_uri=out_uri, batch_size=conf.get("batch_size", 1), parallel_size=conf.get("parallel_size", 1), protocol_prefix=conf.get("protocol_prefix", ""), base_path=conf.get("base_path", ""))
            else: node = UnifiedOperatorNode(operator=plan["operator"], node_id=nid, input_uri=in_uri, output_uri=out_uri, batch_size=conf.get("batch_size", 1), parallel_size=conf.get("parallel_size", 1), protocol_prefix=conf.get("protocol_prefix", ""), base_path=conf.get("base_path", ""))
            node.bind_io(in_s, out_s); final_nodes.append(node)
        return final_nodes

    def _reconstruct_topology(self, runtime_data: Dict[str, Any]) -> List[INode]:
        node_states = runtime_data.get("nodes", []); plans = []; func_idx = 0
        for ns in node_states:
            nid = ns["node_id"]
            conf = {"batch_size": ns.get("batch_size", 1), "parallel_size": ns.get("parallel_size", 1), "input_uri": ns.get("input_uri"), "output_uri": ns.get("output_uri"), "protocol_prefix": ns.get("protocol_prefix", ""), "base_path": ns.get("base_path", "")}
            if nid == "input": plan = {"node_id": nid, "type": "io_in", "config": conf}
            elif nid == "output": plan = {"node_id": nid, "type": "io_out", "config": conf}
            else:
                op = self._operators[func_idx] if func_idx < len(self._operators) else None
                plan = {"node_id": nid, "type": "functional", "operator": op, "config": conf}
                func_idx += 1
            plans.append(plan)
        return self._materialize_topology(plans)

    def _clear_streams_if_needed(self):
        source = self._nodes[0].input_stream if self._nodes else None
        for n in self._nodes:
            if hasattr(n, 'output_stream') and n.output_stream and n.output_stream != source: n.output_stream.clear_data()
        res_dir = os.path.join(self.results_dir, self.pipeline_id)
        for target in ["checkpoint.json", "report.json", "runtime.json"]:
            path = os.path.join(res_dir, target)
            if os.path.exists(path):
                try: os.remove(path); _platform_logger.info(f"  🗑️  已物理删除旧文件: {target}")
                except Exception as e: _platform_logger.warning(f"  ⚠️  删除 {target} 失败: {e}")


# 别名，向后兼容
UnifiedPipeline = UnifiedOperatorPipeline
