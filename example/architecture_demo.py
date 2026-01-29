"""
DataGen 架构深度测试集 (Architecture Deep Test Suite)
1. 极简模式 (Case 1: Path Derivation)
2. 流式模式 (Case 2: Streaming)
3. 容错恢复 (Case 3: Recovery & Mirror)
4. 多级算子与过滤 (Case 4: Long Chain & Filter) - 测试数据量变化时的进度准确性
5. 手动路径覆盖 (Case 5: Manual URI Override) - 测试自定义中间路径
6. 高并发背压测试 (Case 6: Backpressure Stress) - 测试 Semaphore 稳定性
"""
import time
import os
import json
import threading
from typing import List, Any, Optional, Dict

# 统一从 datagen 顶层导出
from llm_datagen import (
    UnifiedNodePipeline,
    UnifiedPipeline,
    GenericLLMOperator,
    FunctionOperator,
    UnifiedNode,
    WriterConfig
)

# ========== 第一步：注册 LLM 模型 ==========
def setup_llm_model():
    """设置 LLM 模型"""
    from llm_datagen.llm import model_container
    try:
        # 从同目录下的 hello.txt 获取 api_key
        key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hello.txt")
        api_key = ""
        if os.path.exists(key_path):
            with open(key_path, "r", encoding="utf-8") as f:
                api_key = f.read().strip()
        
        model_container.register(
            name="example_model",
            model="doubao-seed-1-6-251015",
            base_url="https://ark.cn-beijing.volces.com/api/v3",
            api_key=api_key,
            default_params={"temperature": 0.7, "max_tokens": 1000}
        )
        print("✓ LLM 模型注册成功 (Doubao)")
    except Exception as e:
        print(f"⚠️ LLM 注册异常: {e} (系统将使用模拟模式运行)")

# ========== 第二步：定义业务算子 ==========

class TranslationOperator(GenericLLMOperator):
    """翻译算子：在无网络环境下自动切换为模拟逻辑"""
    def __init__(self, model_name: str = "example_model"):
        super().__init__(config={
            "model_name": model_name,
            "custom_prompt": "请将以下文字翻译成英文：{text}"
        })

    def process_batch(self, items: List[Any], ctx: Any = None) -> List[Any]:
        try:
            # 模拟在沙箱环境中可能发生的网络连接失败或空返回
            res = super().process_batch(items, ctx)
            # print(f"TranslationOperator1234: {res}")
            if not res or all(not r.get("llm_output") for r in res): raise RuntimeError()
            return res
        except:
            results = []
            mock_translations = ["The weather is nice.", "AI is changing life.", "Code changes world."]
            for i, item in enumerate(items):
                text = item.get("text", "") if isinstance(item, dict) else str(item)
                res_item = item.copy() if isinstance(item, dict) else {"text": text}
                res_item["llm_output"] = f"[SIM] {mock_translations[i % len(mock_translations)]}"
                if ctx: ctx.report_usage({"prompt_tokens": len(text), "completion_tokens": 20})
                results.append(res_item)
            return results

class FilterOperator(FunctionOperator):
    """过滤算子：仅保留长度大于 15 的结果"""
    def __init__(self, min_len: int = 15):
        def filter_func(item: Any):
            text = item.get("llm_output", "")
            # 返回空列表表示过滤掉该项
            return [item] if len(text) > min_len else []
        super().__init__(func=filter_func)

class WordCountOperator(FunctionOperator):
    """统计算子：统计单词个数"""
    def __init__(self):
        def count_words(item: Any):
            text = item.get("llm_output", "")
            item["word_count"] = len([w for w in text.split() if w.strip()])
            return item
        super().__init__(func=count_words)

class SegmentOperator(FunctionOperator):
    """分词算子：将一段文字拆分为多个单词项 (1:N 模式)"""
    def __init__(self):
        def segment_func(item: Any):
            text = item.get("llm_output", "")
            # 简单模拟分词：按空格或标点拆分
            words = text.replace(".", "").replace(",", "").split()
            # 核心改进：使用框架自动注入的 _i 作为物理溯源 ID
            parent_i = item.get("_i")
            # 返回一个列表，框架会自动将其扁平化并作为多个独立 items 输出
            return [{"word": w, "parent_i": parent_i} for w in words]
        super().__init__(func=segment_func)

def setup_data(file_path, count=100):
    """准备模拟数据"""
    os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
    sample_texts = ["今天天气真好", "人工智能改变生活", "代码改变世界"]
    with open(file_path, 'w', encoding='utf-8') as f:
        for i in range(count):
            text = sample_texts[i % len(sample_texts)]
            f.write(json.dumps({"text": text, "id": i}, ensure_ascii=False) + '\n')

# ========== 场景 1-3 (保持原有逻辑) ==========
def run_case_1():
    print("\n>>> 场景 1: 极简模式 (自动路径推导)")
    input_file = "tmp/demo/case1_input.jsonl"
    setup_data(input_file, count=20)
    
    # 新推荐写法：参数收敛到构造函数
    pipeline = UnifiedPipeline(
        operators=[TranslationOperator(), WordCountOperator()],
        input_uri=f"jsonl://{input_file}", 
        output_uri="jsonl://tmp/demo/result1.jsonl", 
        base_path="tmp/",
        protocol_prefix="hello"
    )
    pipeline.create(pipeline_id="case1_simple")
    pipeline.run()

def run_case_2():
    print("\n>>> 场景 2: 并行流式模式 (Streaming Engine)")
    input_file = "tmp/demo/case2_input.jsonl"
    setup_data(input_file, count=50)
    
    # 支持在构造函数中指定执行模式
    pipeline = UnifiedPipeline(
        operators=[TranslationOperator(), WordCountOperator()],
        input_uri=f"jsonl://{input_file}", 
        output_uri="jsonl://tmp/demo/case2_out.jsonl", 
        streaming=True, 
        parallel_size=5
    )
    pipeline.create(pipeline_id="case2_streaming")
    pipeline.run()

def run_case_3():
    writer_cfg = WriterConfig(
        async_mode=True,
        flush_batch_size=50,
        flush_interval=20,
        queue_size=1000
    )
    print("\n>>> 场景 3: 自动断点恢复模式 (Mirror Recovery)")
    pid = "case3_recovery"
    input_file = "tmp/demo/case3_input.jsonl"
    setup_data(input_file, count=40)
    
    # [A] 模拟崩溃运行
    pipe1 = UnifiedPipeline(
        operators=[TranslationOperator()],
        input_uri=f"jsonl://{input_file}", 
        output_uri="jsonl://tmp/demo/case3_fail.jsonl",
        batch_size=2,

        writer_config=writer_cfg
    )
    pipe1.create(pipeline_id=pid)
    
    # 注意：nodes[0] 是 InputNode, nodes[1] 是第一个算子节点
    target_node = pipe1.nodes[1]
    
    # 模拟崩溃逻辑
    target_node._ensure_impl()
    original_p = target_node._impl.process_batch
    def crash_p(*args, **kwargs):
        if target_node.get_progress() >= 15: raise Exception("Crash simulated")
        return original_p(*args, **kwargs)
    target_node._impl.process_batch = crash_p
    
    try: pipe1.run()
    except: print(f"✓ 模拟崩溃完成，当前进度: {target_node.get_progress()}")

    # [B] 恢复运行
    print("\n--- [B] 正在执行自动恢复 ---")
    pipe2 = UnifiedPipeline(operators=[TranslationOperator()])
    pipe2.resume(pipeline_id=pid)
    pipe2.run()

# ========== 新场景 4: 多级算子长链与过滤 ==========
def run_case_4():
    print("\n>>> 场景 4: 多级长链 + 过滤 (测试非 1:1 数据流)")
    input_file = "tmp/demo/case4_input.jsonl"
    setup_data(input_file, count=50)
    writer_cfg = WriterConfig(
        async_mode=True,
        flush_batch_size=50,
        flush_interval=20,
        queue_size=1000
    )
    pipeline = UnifiedPipeline(
        operators=[
            TranslationOperator(), 
            FilterOperator(min_len=50),
            WordCountOperator()
        ],
        input_uri=f"jsonl://{input_file}",
        output_uri="jsonl://tmp/demo/case4_final.jsonl",
        batch_size=5,
        writer_config=writer_cfg
    )
    pipeline.create(pipeline_id="case4_filter_chain")
    pipeline.run()
    print("✓ 场景 4 完成。")

# ========== 新场景 5: 手动 URI 路径覆盖 ==========
def run_case_5():
    print("\n>>> 场景 5: 手动 URI 路径覆盖 (测试显式路径优先级)")
    input_file = "tmp/demo/case5_input.jsonl"
    setup_data(input_file, count=10)
    
    pipeline = UnifiedPipeline(
        operators=[TranslationOperator(), WordCountOperator()],
        input_uri=f"jsonl://{input_file}",
        output_uri="jsonl://tmp/demo/case5_final.jsonl"
    )
    
    # 通过 node_configs 手动指定 node_0 的输出路径
    custom_uri = "jsonl://tmp/custom_location/intermediate_data.jsonl"
    
    pipeline.create(
        pipeline_id="case5_override",
        node_configs=[
            {"output_uri": custom_uri}, 
            {}
        ]
    )
    
    print(f"--- 验证路径覆盖 ---")
    rt = pipeline.get_runtime()
    n0_out = rt['nodes'][1]['output_uri']
    n1_in = rt['nodes'][2]['input_uri']
    print(f"Node 0 Out: {n0_out}")
    print(f"Node 1 In : {n1_in}")
    
    if n0_out == custom_uri and n1_in == custom_uri:
        print("✓ 路径覆盖与自动焊接同步校验成功")
    else:
        print("❌ 路径覆盖失效")
    pipeline.run()

# ========== 新场景 6: 高并发背压压力测试 ==========
def run_case_6():
    print("\n>>> 场景 6: 高并发背压测试 (1000条数据 / 20并发)")
    input_file = "tmp/demo/case6_input.jsonl"
    setup_data(input_file, count=1000)
    
    pipeline = UnifiedPipeline(
        operators=[TranslationOperator()],
        input_uri=f"jsonl://{input_file}",
        output_uri="jsonl://tmp/demo/case6_final.jsonl",
        parallel_size=20,
        batch_size=5
    )
    pipeline.create(pipeline_id="case6_stress")
    
    print("🚀 启动高压力测试...")
    start = time.time()
    pipeline.run()
    print(f"✓ 场景 6 完成，总耗时: {time.time()-start:.2f}s")

# ========== 新场景 7: 爆炸分发模式 (1:N 模式) + 两次恢复测试 ==========
def run_case_7():
    print("\n>>> 场景 7: 爆炸分发 (1:N 模式) + 两次恢复测试")
    pid = "case7_explosion"
    input_file = "tmp/demo/case7_input.jsonl"
    setup_data(input_file, count=50)
    
    def inject_crash(node, threshold, message="Crash simulated"):
        node._ensure_impl()
        original_p = node._impl.process_batch
        def crash_p(*args, **kwargs):
            if node.get_progress() >= threshold:
                raise Exception(f"{message} at {threshold}")
            return original_p(*args, **kwargs)
        node._impl.process_batch = crash_p

    print("\n--- [A] 第一次运行：在 Translation (node_0) 阶段模拟崩溃 ---")
    pipe1 = UnifiedPipeline(
        operators=[TranslationOperator(), SegmentOperator()],
        input_uri=f"jsonl://{input_file}",
        output_uri="jsonl://tmp/demo/case7_final.jsonl",
        streaming=True,
        batch_size=5
    )
    pipe1.create(pipeline_id=pid)
    inject_crash(pipe1.nodes[1], 20, "Translation Crash")
    
    try: pipe1.run()
    except Exception as e: print(f"✓ 捕获到预期崩溃: {e}")

    print("\n--- [B] 第二次运行：恢复，并在 Segmentation (node_1) 阶段模拟第二次崩溃 ---")
    pipe2 = UnifiedPipeline(operators=[TranslationOperator(), SegmentOperator()])
    pipe2.resume(pipeline_id=pid)
    inject_crash(pipe2.nodes[2], 35, "Segmentation Crash")
    
    try: pipe2.run()
    except Exception as e: print(f"✓ 捕获到第二次预期崩溃: {e}")

    print("\n--- [C] 第三次运行：最终恢复并完成 ---")
    pipe3 = UnifiedPipeline(operators=[TranslationOperator(), SegmentOperator()])
    pipe3.resume(pipeline_id=pid)
    pipe3.run()
    
    # 验证最终结果
    output_path = "tmp/demo/case7_final.jsonl"
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            print(f"✓ 场景 7 最终完成。50 条输入产生了 {len(lines)} 条分词输出。")

def run_case_8():
    """新增场景：UnifiedNodePipeline 直接使用物理节点"""
    print("\n>>> 场景 8: 物理 Master 模式 (直接操作 UnifiedNode)")
    input_file = "tmp/demo/case8_input.jsonl"
    setup_data(input_file, count=10)
    
    from llm_datagen import StreamFactory
    
    # 1. 手动构建物理节点
    node1 = UnifiedNode(node_id="manual_n1", batch_size=2)
    
    # 手动创建的输入流，如果是静态文件，需要立即封口(seal)，否则 Reader 会一直等待
    in_s = StreamFactory.create(f"jsonl://{input_file}")
    in_s.seal() 
    
    node1.bind_io(in_s, StreamFactory.create("jsonl://tmp/demo/case8_mid.jsonl"))
    # 注入简单逻辑
    node1.set_processor(lambda items, ctx: [{"text": item.get("text", "") + " [Processed]"} for item in items])
    
    # 2. 使用 UnifiedNodePipeline (物理 Master)
    pipeline = UnifiedNodePipeline(nodes=[node1])
    pipeline.create(pipeline_id="case8_physical", streaming=False)
    pipeline.run()
    print("✓ 场景 8 完成。")

# ========== 新场景 9: 异步写入压力测试 (WriterConfig Stress Test) ==========
def run_case_9():
    print("\n>>> 场景 9: 异步写入压力测试 (WriterConfig Stress Test)")
    input_file = "tmp/demo/case9_input.jsonl"
    setup_data(input_file, count=10000)
    
    # 配置高性能异步写入策略
    writer_cfg = WriterConfig(
        async_mode=True,
        flush_batch_size=50,
        flush_interval=20,
        queue_size=1000
    )
    
    pipeline = UnifiedPipeline(
        operators=[ TranslationOperator(), 
            FilterOperator(min_len=50),
            WordCountOperator(),
             SegmentOperator()],
        input_uri=f"jsonl://{input_file}",
        output_uri="jsonl://tmp/demo/case9_final.jsonl",
        writer_config=writer_cfg,
        parallel_size=10,
        batch_size=5,
        streaming=True
    )
    pipeline.create(pipeline_id="case9_async_stress")
    
    start = time.time()
    pipeline.run()
    print(f"✓ 场景 9 完成。异步刷盘模式下耗时: {time.time()-start:.2f}s")
    
    # 验证数据完整性
    output_path = "tmp/demo/case9_final.jsonl"
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as f:
            count = sum(1 for _ in f)
            print(f"📊 数据校验: 输入 500 条 -> 输出 {count} 条 {'[PASS]' if count==500 else '[FAIL]'}")

# ========== 新场景 10: 流式可靠性测试 (针对早产 EOF / 延迟启动) ==========
def run_case_10():
    print("\n>>> 场景 10: 流式可靠性测试 (针对早产 EOF / 延迟启动)")
    input_file = "tmp/demo/case10_input.jsonl"
    setup_data(input_file, count=20)
    
    class SlowStartupOperator(FunctionOperator):
        """模拟一个启动极慢的上游，诱发下游早产 EOF"""
        def __init__(self):
            def slow_func(item):
                # 第一条数据故意延迟很久才产出
                if item.get("id") == 0:
                    time.sleep(2.0) 
                return item
            super().__init__(func=slow_func)

    pipeline = UnifiedPipeline(
        operators=[SlowStartupOperator(), WordCountOperator()],
        input_uri=f"jsonl://{input_file}",
        output_uri="jsonl://tmp/demo/case10_final.jsonl",
        streaming=True, # 开启并行流
        batch_size=1
    )
    pipeline.create(pipeline_id="case10_eof_robustness")
    
    print("🚀 启动流式链路，观测下游是否能稳健等待上游数据 (预期会有重试日志)...")
    pipeline.run()
    
    output_path = "tmp/demo/case10_final.jsonl"
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as f:
            count = sum(1 for _ in f)
            print(f"📊 可靠性校验: 输出 {count}/20 条 {'[PASS]' if count==20 else '[FAIL]'}")

# ========== 新场景 11: 极小背压测试 (Small Queue Blocking) ==========
def run_case_11():
    print("\n>>> 场景 11: 极小异步队列背压测试 (验证生产者阻塞)")
    input_file = "tmp/demo/case11_input.jsonl"
    setup_data(input_file, count=100)
    
    # 设置极小的队列大小，强制触发生产者阻塞
    writer_cfg = WriterConfig(
        async_mode=True,
        queue_size=2,          # 极小队列
        flush_batch_size=10,   # 攒够 10 条才刷
        flush_interval=5.0     # 且刷盘时间间隔很长
    )
    
    pipeline = UnifiedPipeline(
        operators=[TranslationOperator()],
        input_uri=f"jsonl://{input_file}",
        output_uri="jsonl://tmp/demo/case11_final.jsonl",
        writer_config=writer_cfg,
        parallel_size=20, # 高并发产生数据
        batch_size=1
    )
    pipeline.create(pipeline_id="case11_backpressure")
    
    print("🚀 启动测试，观察在磁盘刷盘前，生产者是否会被 queue.put 阻塞...")
    start = time.time()
    pipeline.run()
    print(f"✓ 场景 11 完成，耗时: {time.time()-start:.2f}s")
    
    output_path = "tmp/demo/case11_final.jsonl"
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as f:
            count = sum(1 for _ in f)
            print(f"📊 数据校验: 输出 {count}/100 条 {'[PASS]' if count==100 else '[FAIL]'}")

# ========== 新场景 12: 空输入测试 (Empty Input Edge Case) ==========
def run_case_12():
    print("\n>>> 场景 12: 空输入边界测试")
    input_file = "tmp/demo/case12_empty.jsonl"
    setup_data(input_file, count=0) # 创建一个空文件
    
    pipeline = UnifiedPipeline(
        operators=[TranslationOperator(), WordCountOperator()],
        input_uri=f"jsonl://{input_file}",
        output_uri="jsonl://tmp/demo/case12_final.jsonl",
        streaming=True
    )
    pipeline.create(pipeline_id="case12_empty")
    
    print("🚀 运行空任务...")
    pipeline.run()
    print("✓ 场景 12 完成 (应无任何报错)。")

# ========== 新场景 13: CSV 协议测试 (CSV Protocol Test) ==========
def run_case_13():
    print("\n>>> 场景 13: CSV 协议测试 (CSV Protocol)")
    input_file = "tmp/demo/case13_input.csv"
    # 准备 CSV 数据
    os.makedirs(os.path.dirname(os.path.abspath(input_file)), exist_ok=True)
    with open(input_file, 'w', encoding='utf-8', newline='') as f:
        import csv
        writer = csv.DictWriter(f, fieldnames=["id", "text"])
        writer.writeheader()
        writer.writerow({"id": 0, "text": "Hello CSV"})
        writer.writerow({"id": 1, "text": "DataGen is powerful"})
    
    pipeline = UnifiedPipeline(
        operators=[TranslationOperator()],
        input_uri=f"csv://{input_file}",
        output_uri="csv://tmp/demo/case13_final.csv",
        streaming=False
    )
    pipeline.create(pipeline_id="case13_csv")
    
    print("🚀 运行 CSV 任务...")
    pipeline.run()
    
    # output_path = "tmp/demo/case13_final.csv"
    # if os.path.exists(output_path):
    #     with open(output_path, 'r', encoding='utf-8') as f:
    #         lines = f.readlines()
    #         print(f"📊 CSV 校验: 输出行数={len(lines)} (含表头) {'[PASS]' if len(lines)==3 else '[FAIL]'}")
    #         print(f"📄 样例内容: {lines[-1].strip()}")

if __name__ == "__main__":
    setup_llm_model()
    
    # run_case_1()
    # run_case_2()
    # run_case_3()
    # run_case_4()
    # run_case_5()
    # run_case_6()
    # run_case_7()
    # run_case_8()
    run_case_9()
    run_case_10()
    run_case_11()
    run_case_12()
    run_case_13()