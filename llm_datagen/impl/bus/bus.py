"""
DataGen 流实现库：能力声明与协议化管理。
"""
import threading
import os
import json
import logging
import queue
from abc import ABC
from typing import Any, Dict, Optional, Union

from llm_datagen.core.bus import IReader, IWriter, IRecoverableStream, IDataStream
from llm_datagen.core.storage import IStorage
from llm_datagen.core.channel import IChannel
from llm_datagen.core.config import WriterConfig
from llm_datagen.impl.storage.jsonl_storage import JsonlStorage
from llm_datagen.impl.storage.csv_storage import CsvStorage
from llm_datagen.impl.storage.memory_storage import MemoryStorage
from llm_datagen.impl.channel.threading_channel import ThreadingChannel
from llm_datagen.impl.bus.stream_bridge import StreamBridge

_bus_logger = logging.getLogger("DataGen.Bus")

# --- 1. 基础实现类 ---
class BaseStream(IRecoverableStream, ABC):
    PROTOCOL = "base"
    SUFFIX = ""

    def __init__(self):
        self._raw_uri = None
        self._protocol_prefix = ""
        self._base_path = ""
        self._last_reader = None
        self._is_opened = False

    def create(self, uri: str, protocol_prefix: str = "", base_path: str = ""):
        """初始化配置：显式拦截 None，确保物理安全"""
        self._raw_uri = uri
        self._protocol_prefix = protocol_prefix or ""
        self._base_path = base_path or ""
        
        # 核心改进：不再主动修改用户提供的 URI。
        # 如果路径错了，应该是 Pipeline 推导层的责任，或者是用户输入错误。
        
        self.open()
        return self

    @property
    def protocol(self) -> str: return self.PROTOCOL

    @property
    def raw_uri(self) -> str:
        """暴露原始未拼接的 URI"""
        return self._raw_uri

    @property
    def uri(self) -> str:
        raw = self._raw_uri or ""
        protocol = raw.split("://")[0] + "://" if "://" in raw else ""
        path_part = raw.split("://")[-1] if "://" in raw else raw
        
        if self._protocol_prefix:
            # 核心改进：鲁棒的路径拼接
            # 1. 提取 prefix 的路径部分
            prefix_path = self._protocol_prefix.split("://")[-1] if "://" in self._protocol_prefix else self._protocol_prefix
            # 2. 提取 prefix 的协议
            prefix_proto = self._protocol_prefix.split("://")[0] + "://" if "://" in self._protocol_prefix else protocol
            
            # 3. 智能缝合：确保 prefix_path 和 path_part 之间有斜杠
            prefix_path = prefix_path.rstrip("/")
            path_part = path_part.lstrip("/")
            
            return f"{prefix_proto}{prefix_path}/{path_part}"
            
        return raw if "://" in raw else f"{protocol}{path_part}"

    @property
    def protocol_prefix(self) -> str: return self._protocol_prefix

    @property
    def base_path(self) -> str: return self._base_path

    @base_path.setter
    def base_path(self, value: str): self._base_path = value or ""

    @property
    def is_opened(self) -> bool: return self._is_opened

    def open(self): self._is_opened = True
    def close(self): self._is_opened = False

    def clear_data(self):
        """默认实现：重置状态。子类可重写以执行物理删除。"""
        if self._last_reader and hasattr(self._last_reader, 'resume'):
            self._last_reader.resume(0)

    def unseal(self):
        """默认实现：无操作。子类可重写以执行物理撤销封条。"""
        pass

    def seal(self):
        """默认实现：无操作。子类可重写以执行物理贴上封条。"""
        pass

    def get_current_progress(self) -> Any:
        if self._last_reader: return self._last_reader.get_current_progress()
        return 0

    def get_runtime(self) -> Dict[str, Any]:
        return {
            "uri": self._raw_uri,
            "protocol_prefix": self.protocol_prefix,
            "base_path": self.base_path
        }

    def save_runtime(self, file_path: str) -> None:
        state = self.get_runtime()
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2, ensure_ascii=False)

    def resume_from_runtime(self, runtime_data: Dict[str, Any]) -> None:
        if "uri" in runtime_data:
            self.create(
                uri=runtime_data.get("uri"),
                protocol_prefix=runtime_data.get("protocol_prefix", ""),
                base_path=runtime_data.get("base_path", "")
            )

# --- 2. 具体实现 ---

class MemoryStream(BaseStream):
    PROTOCOL = "memory"
    def __init__(self): 
        super().__init__()
        self._storage = MemoryStorage(); self._channel = ThreadingChannel()
    def get_reader(self, progress=None): 
        self._last_reader = GenericReader(self._storage, self._channel, progress or 0)
        return self._last_reader
    def get_writer(self, options: Optional[Union[Dict, WriterConfig]] = None): 
        return GenericWriter(self._storage, self._channel, options)

class FileStream(BaseStream, ABC):
    @property
    def path(self) -> str:
        raw = self._raw_uri or ""
        path_part = raw.split("://")[-1] if "://" in raw else raw
        safe_base = self.base_path or ""
        # 物理安全性校验：防止 path_part 包含 None 字符串
        if "None" in path_part:
            logger.warning(f"Suspect 'None' string in physical path: {path_part}")
        return os.path.abspath(os.path.join(safe_base, path_part)) if safe_base else os.path.abspath(path_part)

    def clear_data(self):
        """物理删除文件及封条"""
        super().clear_data()
        if os.path.exists(self.path):
            os.remove(self.path)
        self.unseal()

    def unseal(self):
        """物理撕掉封条"""
        done_file = f"{self.path}.done"
        if os.path.exists(done_file):
            os.remove(done_file)
            _bus_logger.info(f"🔓 已物理撕掉封条: {done_file}")

    def seal(self):
        """物理贴上封条"""
        done_file = f"{self.path}.done"
        if not os.path.exists(done_file):
            with open(done_file, 'w') as f:
                f.write("sealed_by_resume")
            _bus_logger.info(f"🔒 已物理补齐封条: {done_file}")

class JsonlStream(FileStream):
    PROTOCOL = "jsonl"
    SUFFIX = ".jsonl"
    def __init__(self): 
        super().__init__()
        self._storage = None; self._channel = ThreadingChannel()
    def create(self, uri: str, protocol_prefix: str = "", base_path: str = ""):
        super().create(uri, protocol_prefix, base_path)
        self._storage = JsonlStorage(self.path)
        return self
    def get_reader(self, progress=None): 
        reader = GenericReader(self._storage, self._channel, progress or 0)
        self._last_reader = reader
        return reader
    def get_writer(self, options: Optional[Union[Dict, WriterConfig]] = None): 
        return GenericWriter(self._storage, self._channel, options)

class CsvStream(FileStream):
    PROTOCOL = "csv"
    SUFFIX = ".csv"
    def __init__(self, delimiter: str = ','): 
        super().__init__()
        self.delimiter = delimiter
        self._storage = None; self._channel = ThreadingChannel()
    def create(self, uri: str, protocol_prefix: str = "", base_path: str = ""):
        super().create(uri, protocol_prefix, base_path)
        self._storage = CsvStorage(self.path, delimiter=self.delimiter)
        return self
    def get_reader(self, progress=None): 
        reader = GenericReader(self._storage, self._channel, progress or 0)
        self._last_reader = reader
        return reader
    def get_writer(self, options: Optional[Union[Dict, WriterConfig]] = None): 
        return GenericWriter(self._storage, self._channel, options)

class UnifiedFileStream(FileStream):
    PROTOCOL = "file"
    def __init__(self):
        super().__init__()
        self._impl: Optional[BaseStream] = None

    def create(self, uri: str, protocol_prefix: str = "", base_path: str = ""):
        self._raw_uri = uri
        self._protocol_prefix = protocol_prefix or ""
        self._base_path = base_path or ""
        
        # 1. 计算当前的最终逻辑 URI
        target_uri = self.uri or ""
        if target_uri.startswith("memory://"):
            raise ValueError("UnifiedFileStream only supports file protocols.")
            
        # 2. 核心改进：路径自愈 (只有路径缺失后缀时才补全)
        # 判定标准：如果不以已知的合法后缀结尾，则进行补全
        actual_protocol = target_uri.split("://")[0] if "://" in target_uri else self.PROTOCOL
        known_suffixes = [".jsonl", ".csv"]
        
        if not any(target_uri.endswith(s) for s in known_suffixes):
            suffix = get_protocol_extension(actual_protocol)
            # 修改原始 URI 以包含后缀
            self._raw_uri = f"{self._raw_uri}{suffix}"
            target_uri = self.uri # 重新计算包含后缀的逻辑地址
            
        # 3. 根据自愈后的协议分发实现
        if target_uri.startswith("jsonl://") or target_uri.startswith("file://"):
            self._impl = JsonlStream().create(self._raw_uri, self._protocol_prefix, self._base_path)
        elif target_uri.startswith("csv://"):
            self._impl = CsvStream().create(self._raw_uri, self._protocol_prefix, self._base_path)
        else:
            proto = target_uri.split("://")[0] if "://" in target_uri else "unknown"
            raise ValueError(f"Unsupported protocol '{proto}' for URI: {target_uri}")
            
        return self

    def get_reader(self, progress=None): 
        self._last_reader = self._impl.get_reader(progress)
        return self._last_reader
    def get_writer(self, options: Optional[Union[Dict, WriterConfig]] = None): 
        return self._impl.get_writer(options)
    def clear_data(self):
        if self._impl: self._impl.clear_data()
    
    def unseal(self):
        if self._impl: self._impl.unseal()
    
    def seal(self):
        if self._impl: self._impl.seal()
        else: super().clear_data()
    def get_runtime(self) -> Dict[str, Any]: return self._impl.get_runtime() if self._impl else super().get_runtime()
    def save_runtime(self, file_path: str) -> None:
        if self._impl: self._impl.save_runtime(file_path)
        else: super().save_runtime(file_path)
    def resume_from_runtime(self, runtime_data: Dict[str, Any]) -> None:
        self._raw_uri = runtime_data.get("uri")
        self._protocol_prefix = runtime_data.get("protocol_prefix", "")
        self._base_path = runtime_data.get("base_path", "")
        
        # 核心修复：根据恢复的 URI 重新探测并创建 impl，确保多态性
        if self._raw_uri:
            self.create(self._raw_uri, self._protocol_prefix, self._base_path)
        
        if self._impl: 
            self._impl.resume_from_runtime(runtime_data)

# --- 3. 工厂与引擎 ---

class GenericReader(IReader):
    def __init__(self, storage: IStorage, channel: IChannel, progress: Any = None):
        self.storage = storage; self.channel = channel; self._completed = progress or 0
        self._bridge = StreamBridge(storage, channel)
    def read(self, batch_size: int = 1, timeout: Optional[float] = None):
        # 记录本次读取前的起始物理位点
        for batch in self._bridge.read_stream(self._completed, batch_size, timeout or 5.0):
            data_list, id_list = [], []
            for i, raw in enumerate(batch):
                # 核心修复：优先使用物理行号作为索引兜底，确保恢复后 ID 绝对连续
                physical_idx = self._completed + i
                idx = raw["_i"] if isinstance(raw, dict) and "_i" in raw else physical_idx
                data_list.append(raw.get("data", raw) if isinstance(raw, dict) else raw)
                id_list.append(idx)
            
            if data_list: 
                self._completed += len(data_list)
                yield data_list, id_list
    def get_current_progress(self): return self._completed
    @property
    def total_count(self): return self.storage.size()
    @property
    def completed_count(self): return self._completed
    def close(self): self.channel.set_eof()
    def resume(self, progress): self._completed = progress

class GenericWriter(IWriter):
    def __init__(self, storage: IStorage, channel: IChannel, options: Optional[Union[Dict, WriterConfig]] = None):
        self.storage = storage; self.channel = channel
        # 核心修复：初始化写入计数为存储当前大小，确保恢复后 auto_id 连续且不冲突
        self._written_count = storage.size()
        self._lock = threading.Lock()
        
        # 策略转换
        if isinstance(options, WriterConfig):
            self.config = options
        elif isinstance(options, dict):
            self.config = WriterConfig(**options)
        else:
            self.config = WriterConfig()

        self._use_async = self.config.async_mode
        if self._use_async:
            self._queue = queue.Queue(maxsize=self.config.queue_size)
            self._stop_event = threading.Event()
            self._flush_batch_size = self.config.flush_batch_size
            self._worker = threading.Thread(target=self._background_worker, name="AsyncWriter", daemon=True)
            self._worker.start()
            _bus_logger.info(
                f"⚡ GenericWriter 已启动异步模式: "
                f"queue={self.config.queue_size}, batch={self.config.flush_batch_size}, "
                f"interval={self.config.flush_interval}s, retry={self.config.retry_interval}s"
            )

    def _background_worker(self):
        """异步写入后台线程"""
        last_flush_time = time.time()
        
        while not self._stop_event.is_set() or not self._queue.empty():
            batch = []
            try:
                # 1. 阻塞等待第一个元素，超时时间使用 retry_interval (退火时间)
                item = self._queue.get(timeout=self.config.retry_interval)
                batch.append(item)
                
                # 2. 尝试贪婪聚合后续元素，直到满足数量阈值 或 时间阈值
                while len(batch) < self._flush_batch_size:
                    now = time.time()
                    elapsed = now - last_flush_time
                    
                    if elapsed >= self.config.flush_interval:
                        break # 达到时间阈值，强制刷盘
                    
                    try:
                        # 剩余可等待时间
                        remaining = self.config.flush_interval - elapsed
                        batch.append(self._queue.get(timeout=max(0.001, remaining)))
                    except queue.Empty:
                        break # 超时未拿到新数据，退出聚合
                
                # 3. 统一执行物理写入
                with self._lock:
                    self._perform_batch_write(batch)
                    last_flush_time = time.time()
                
                # 4. 确认任务完成
                for _ in range(len(batch)):
                    self._queue.task_done()
                    
            except queue.Empty:
                continue
            except Exception as e:
                _bus_logger.error(f"🚨 异步写入线程崩溃: {e}", exc_info=True)

    def _perform_batch_write(self, batch_data):
        """执行一组批次的物理写入"""
        all_envelopes = []
        for items, anchors in batch_data:
            start_idx = self._written_count + len(all_envelopes)
            for i, it in enumerate(items):
                if it is None: continue
                idx = anchors[i] if (anchors and i < len(anchors)) else f"auto_{start_idx + i}"
                
                if isinstance(it, dict):
                    it["_i"] = idx
                    all_envelopes.append(it)
                else:
                    all_envelopes.append({"_i": idx, "data": it})
        
        if all_envelopes:
            # 物理写入存储
            self.storage.append(all_envelopes)
            self._written_count += len(all_envelopes)
            # 通知总线信号
            self.channel.notify()

    def write(self, items, anchors=None, commit=True):
        if self._use_async:
            # 异步模式：入队
            self._queue.put((items, anchors))
        else:
            # 同步模式
            with self._lock:
                self._perform_batch_write([(items, anchors)])

    def close(self):
        if self._use_async:
            _bus_logger.info("⏳ 正在冲刷异步写入缓冲区...")
            self._stop_event.set()
            if self._worker.is_alive():
                self._worker.join(timeout=30)
            _bus_logger.info("✅ 异步写入缓冲区已冲刷完毕")
        
        self.channel.set_eof()
        if hasattr(self.storage, 'mark_finished'):
            self.storage.mark_finished()

class StreamFactory:
    @staticmethod
    def create(uri: str, protocol_prefix: str = "", base_path: str = "") -> IDataStream:
        if uri is None or uri.startswith("memory://"):
            return MemoryStream().create(uri, protocol_prefix, base_path)
        return UnifiedFileStream().create(uri, protocol_prefix, base_path)

class RecoverableStreamFactory(StreamFactory):
    @staticmethod
    def create(uri: str, protocol_prefix: str = "", base_path: str = "") -> Optional[IRecoverableStream]:
        if uri is None: return None
        if uri.startswith("memory://"): raise ValueError("RecoverableStreamFactory does not support 'memory://'.")
        return UnifiedFileStream().create(uri, protocol_prefix, base_path)

    @staticmethod
    def resume(runtime_data: Dict[str, Any]) -> Optional[IRecoverableStream]:
        if not runtime_data: return None
        stream = UnifiedFileStream()
        stream.resume_from_runtime(runtime_data)
        if not stream.is_opened: stream.open()
        return stream

def get_protocol_extension(protocol: str) -> str:
    if not protocol: return ".jsonl"
    clean_protocol = protocol.replace("://", "").lower()
    mapping = {"jsonl": ".jsonl", "csv": ".csv", "file": ".jsonl", "memory": ""}
    return mapping.get(clean_protocol, ".jsonl")
