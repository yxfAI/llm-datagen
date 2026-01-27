import time
from typing import Generator, List, Any, Optional
from llm_datagen.core.storage import IStorage
from llm_datagen.core.channel import IChannel

class StreamBridge:
    """
    流式桥接器：负责将“存储”与“通道”结合
    实现“边写边读”的核心逻辑
    """
    
    def __init__(self, storage: IStorage, channel: IChannel):
        self.storage = storage
        self.channel = channel

    def read_stream(
        self, 
        start_offset: int, 
        batch_size: int, 
        timeout: float = 5.0
    ) -> Generator[List[Any], None, None]:
        """
        流式读取核心循环：支持高可靠的边写边读。
        """
        current_offset = start_offset
        zero_data_retries = 0 # 用于应对启动瞬间的旧封条竞争
        
        while True:
            # 1. 首先尝试从物理存储中拉取数据
            batch = self.storage.read(current_offset, batch_size)
            
            if batch:
                yield batch
                current_offset += len(batch)
                zero_data_retries = 0 # 只要有数据，重置重试计数
                # 如果拉到了满额批次，大概率后面还有，立即继续读，不进等待
                if len(batch) >= batch_size:
                    continue
            
            # 2. 如果目前没读到新数据，检查是否已结束
            is_done = self.channel.is_eof() or (hasattr(self.storage, 'is_finished') and self.storage.is_finished())
            
            if is_done:
                # 核心鲁棒性检查：应对“早产 EOF”
                # 如果进度为 0 且看到结束标识，极其可疑（可能是旧封条未被及时清理）
                if current_offset == 0 and zero_data_retries < 5:
                    time.sleep(0.1) # 给予上游 unseal() 的机会
                    zero_data_retries += 1
                    continue
                
                # 核心修复：信号冗余宽限。即使判定结束，也给予微小的物理 Buffer 落盘时间
                time.sleep(0.05)
                
                # 一旦进入结束状态，必须“彻底排干”存储
                while True:
                    final_batch = self.storage.read(current_offset, batch_size)
                    if not final_batch:
                        break
                    yield final_batch
                    current_offset += len(final_batch)
                break
            
            # 3. 既没数据也没结束，进入睡眠等待信号
            if not self.channel.wait(timeout):
                # 超时退火：尝试读一次，产出由于 batch_size 未满而滞留在存储里的残余数据
                final_batch = self.storage.read(current_offset, batch_size)
                if final_batch:
                    yield final_batch
                    current_offset += len(final_batch)
                else:
                    # 核心改进：如果既没信号也没物理数据，短暂休眠避免 CPU 空转
                    time.sleep(0.1)
                continue
