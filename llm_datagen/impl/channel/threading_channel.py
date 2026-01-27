import threading
import time
from llm_datagen.core.channel import IChannel

class ThreadingChannel(IChannel):
    """基于 threading.Condition 的本地通信频道"""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._eof = False
        self._version = 0 # 信号版本，防止错过通知

    def notify(self) -> None:
        with self._condition:
            self._version += 1
            self._condition.notify_all()

    def wait(self, timeout: float = None) -> bool:
        start_version = self._version
        with self._condition:
            if self._eof:
                return False
            # 如果在进入等待前版本已经变了，说明已经有新数据了
            if self._version > start_version:
                return True
            return self._condition.wait(timeout)

    def set_eof(self) -> None:
        with self._condition:
            self._eof = True
            self._condition.notify_all()

    def is_eof(self) -> bool:
        with self._lock:
            return self._eof

    def reset(self) -> None:
        with self._lock:
            self._eof = False
            self._version += 1 # 变更版本号，唤醒潜在的过时等待
