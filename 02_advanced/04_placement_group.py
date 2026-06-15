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

    # -------------------------------------------------------
    # 6. 补充：测试指定 Bundle Index 的 Task/Actor 分配
    # -------------------------------------------------------
    print("\n=== 测试指定 Bundle Index 的 Task/Actor 分配 ===")

    # 重新创建一个包含 3 个 bundles 的 Placement Group
    # 使用 SPREAD 策略以便在多节点环境下更好地观察分布
    test_bundles = [{"CPU": 0.5}, {"CPU": 0.5}, {"CPU": 0.5}]
    test_pg = placement_group(test_bundles, strategy="SPREAD", name="test_bundle_index_pg")
    ray.get(test_pg.ready())
    print(f"测试用 PG 创建成功：{test_pg.bundle_specs}")

    # 定义一个简单的 Actor 来报告它所在的节点
    @ray.remote(num_cpus=0.5)
    class ReportingActor:
        def __init__(self, actor_id):
            self.actor_id = actor_id
            # 获取 Node ID，兼容不同 Ray 版本的返回类型
            raw_node_id = ray.get_runtime_context().get_node_id()
            if hasattr(raw_node_id, 'hex'):
                # 如果是 NodeID 对象，调用 hex() 方法
                self.node_id = raw_node_id.hex()
            else:
                # 否则，假设它已经是字符串
                self.node_id = raw_node_id

        def get_location_info(self):
            # 截取 Node ID 前几位方便查看
            return f"Actor {self.actor_id} running on Node ID: {self.node_id[:8]}..."

    # 定义一个简单的 Task 来报告它所在的节点
    @ray.remote(num_cpus=0.5)
    def reporting_task(task_id):
        # 获取 Node ID，兼容不同 Ray 版本的返回类型
        raw_node_id = ray.get_runtime_context().get_node_id()
        if hasattr(raw_node_id, 'hex'):
            # 如果是 NodeID 对象，调用 hex() 方法
            current_node_id = raw_node_id.hex()
        else:
            # 否则，假设它已经是字符串
            current_node_id = raw_node_id
        return f"Task {task_id} running on Node ID: {current_node_id[:8]}..."

    # 在 Placement Group 的不同 bundles 上启动 Actors
    actor_handles = []
    for i in range(2): # 启动 2 个 Actor，分配到 bundle 0 和 bundle 1
        actor_handle = ReportingActor.options(
            placement_group=test_pg,
            placement_group_bundle_index=i # 关键：指定 bundle index
        ).remote(f"A{i}")
        actor_handles.append(actor_handle)

    # 在 Placement Group 的不同 bundles 上启动 Tasks
    task_refs = []
    # A0 在 bundle 0, A1 在 bundle 1
    # 为了避免与 Actor 竞争资源，Task 分配到剩余的 bundle
    # 启动 T2 到 bundle 2 (目前是空闲的)
    task_ref_t2 = reporting_task.options(
        placement_group=test_pg,
        placement_group_bundle_index=2 # 分配到 bundle 2
    ).remote("T2")
    task_refs.append(task_ref_t2)

    # 如果需要启动更多 Task，需要规划足够的 bundles 或确保它们不与 Actor 冲突
    # 例如，启动 T3 到 bundle 0 会与 A0 冲突，导致挂起
    # 因此，我们只启动 T2 到空闲的 bundle 2 来演示功能

    # 获取 Actor 的位置信息
    actor_locations = ray.get([h.get_location_info.remote() for h in actor_handles])
    print("\nActor 位置信息 (根据指定的 bundle index 分配):")
    for loc in actor_locations:
        print(f"  {loc}")

    # 获取 Task 的位置信息
    task_locations = ray.get(task_refs)
    print("\nTask 位置信息 (根据指定的 bundle index 分配):")
    for loc in task_locations:
        print(f"  {loc}")

    # 清理测试用的 PG
    ray.util.remove_placement_group(test_pg)
    print("\n测试用 PG 已移除")

    ray.shutdown()
    print("Ray 已关闭")


if __name__ == "__main__":
    main()
