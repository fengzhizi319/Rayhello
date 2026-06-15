"""
Ray 进阶示例 04：Placement Group（资源Placement）

本节目标：
    1. 理解 Placement Group 是什么：把多个 bundle（资源包）绑定在一起分配。
    2. 学会创建 Placement Group 并把 Actor/Task 调度到指定 Group。
    3. 理解 strategy：PACK / SPREAD / STRICT_PACK / STRICT_SPREAD。

关键 API：
    - ray.util.placement_group(bundles, strategy): 创建 Placement Group。
    - placement_group.ready(): 等待 Placement Group 资源就绪。
    - options(placement_group=pg): 把 Actor/Task 绑定到 Placement Group。

注意：
    - 本地模式资源是虚拟的，主要用于学习 API。
    - 真实集群中 Placement Group 对隐私计算/SPU 等场景至关重要。
"""

import time
import ray
from ray.util.placement_group import placement_group


@ray.remote(num_cpus=0.5)
class Worker:
    def __init__(self, worker_id: int):
        self.worker_id = worker_id

    def work(self, x: int) -> str:
        time.sleep(0.5)
        return f"Worker-{self.worker_id} processed {x}"


def main():
    context = ray.init(ignore_reinit_error=True)
    print("Ray 已启动，Dashboard 地址：", context.dashboard_url)
    
    # 输出集群资源信息
    print('\n=== 集群资源信息 ===')
    print('Dashboard URL:', context.dashboard_url)
    print('Cluster Resources:', ray.cluster_resources())
    print('Available Resources:', ray.available_resources())
    
    # -------------------------------------------------------
    # 重要概念：Placement Group 详解
    # -------------------------------------------------------
    print('\n=== Placement Group 重要概念说明 ===')
    print('1. Placement Group 是 Ray 的资源预留和调度机制')
    print('2. Bundle 是资源的基本单位，定义了 CPU、GPU、内存等资源需求')
    print('3. Strategy 定义了 Bundle 之间的放置关系')
    print('4. 可用于实现数据局部性、负载均衡、故障隔离等高级调度策略')

    # -------------------------------------------------------
    # 1. 创建 Placement Group
    # -------------------------------------------------------
    # bundles：每个 bundle 是一份资源申请。这里申请 3 个 bundle，每个 0.5 CPU。
    # strategy="PACK"：尽量把 3 个 bundle 放在同一节点（亲和性）。
    bundles = [{"CPU": 0.5} for _ in range(3)]
    pg = placement_group(bundles, strategy="PACK", name="demo_pg")

    print("\n=== 等待 Placement Group 资源就绪 ===")
    ray.get(pg.ready())
    print(f"Placement Group 创建成功：{pg.bundle_specs}")

    # -------------------------------------------------------
    # 2. 把 Actor 绑定到 Placement Group
    # -------------------------------------------------------
    print("\n=== 在 Placement Group 内创建 3 个 Worker Actor ===")
    workers = [
        Worker.options(placement_group=pg).remote(i)
        for i in range(3)
    ]

    start = time.time()
    refs = [w.work.remote(i) for i, w in enumerate(workers)]
    results = ray.get(refs)
    print(f"结果：{results}")
    print(f"耗时：{time.time() - start:.2f}s")

    # -------------------------------------------------------
    # 3. 不同策略的 Placement Group 测试
    # -------------------------------------------------------
    print("\n=== 不同策略的 Placement Group 对比 ===")
    
    # 3.1 PACK 策略：尽可能打包在同一节点
    print("\n--- 测试 PACK 策略 ---")
    pack_bundles = [{"CPU": 0.5} for _ in range(2)]
    pack_pg = placement_group(pack_bundles, strategy="PACK", name="pack_pg")
    ray.get(pack_pg.ready())
    print(f"PACK PG 创建成功：{pack_pg.bundle_specs}")
    
    # 3.2 SPREAD 策略：尽可能分散到不同节点
    print("\n--- 测试 SPREAD 策略 ---")
    spread_bundles = [{"CPU": 0.5} for _ in range(2)]
    spread_pg = placement_group(spread_bundles, strategy="SPREAD", name="spread_pg")
    ray.get(spread_pg.ready())
    print(f"SPREAD PG 创建成功：{spread_pg.bundle_specs}")
    
    # 3.3 STRICT_PACK 策略：严格打包在同一节点
    print("\n--- 测试 STRICT_PACK 策略 ---")
    strict_pack_bundles = [{"CPU": 0.5} for _ in range(2)]
    strict_pack_pg = placement_group(strict_pack_bundles, strategy="STRICT_PACK", name="strict_pack_pg")
    ray.get(strict_pack_pg.ready())
    print(f"STRICT_PACK PG 创建成功：{strict_pack_pg.bundle_specs}")
    
    # 3.4 创建多个具有不同策略的 Worker 并比较性能
    print("\n--- 性能对比测试 ---")
    
    # 在 PACK 策略的 PG 中创建 Workers
    pack_workers = [
        Worker.options(placement_group=pack_pg).remote(i+10) 
        for i in range(2)
    ]
    start_time = time.time()
    pack_refs = [w.work.remote(i) for i, w in enumerate(pack_workers)]
    pack_results = ray.get(pack_refs)
    print(f"PACK 策略结果：{pack_results}，耗时：{time.time() - start_time:.2f}s")
    
    # 在 SPREAD 策略的 PG 中创建 Workers
    spread_workers = [
        Worker.options(placement_group=spread_pg).remote(i+20) 
        for i in range(2)
    ]
    start_time = time.time()
    spread_refs = [w.work.remote(i) for i, w in enumerate(spread_workers)]
    spread_results = ray.get(spread_refs)
    print(f"SPREAD 策略结果：{spread_results}，耗时：{time.time() - start_time:.2f}s")
    
    # -------------------------------------------------------
    # 4. 策略对比说明（本地模式主要观察 API）
    # -------------------------------------------------------
    print("\n=== Placement Strategy 说明 ===")
    strategies = {
        "PACK": "尽量把 bundle 打包到同一节点（减少网络通信）",
        "SPREAD": "尽量把 bundle 分散到不同节点（提高容错/负载均衡）",
        "STRICT_PACK": "强制所有 bundle 在同一节点，否则失败",
        "STRICT_SPREAD": "强制每个 bundle 在不同节点，否则失败",
    }
    for name, desc in strategies.items():
        print(f"  {name}: {desc}")
    
    # -------------------------------------------------------
    # 5. 清理所有 Placement Group
    # -------------------------------------------------------
    print("\n=== 清理所有 Placement Group ===")
    ray.util.remove_placement_group(pg)
    print("原 PG 已移除")
    ray.util.remove_placement_group(pack_pg)
    print("PACK PG 已移除")
    ray.util.remove_placement_group(spread_pg)
    print("SPREAD PG 已移除")
    ray.util.remove_placement_group(strict_pack_pg)
    print("STRICT_PACK PG 已移除")

    ray.shutdown()
    print("Ray 已关闭")


if __name__ == "__main__":
    main()
