"""
Ray 基础示例 02：远程 Actor（Remote Actor）

本节目标：
    1. 学会用 @ray.remote 把 Python 类变成分布式 Actor。
    2. 理解 Actor 是有状态的 Worker，方法调用之间状态会保持。
    3. 比较 Actor 与 Task 的适用场景。

关键 API：
    - @ray.remote
    - ActorClass.remote(*args): 创建 Actor 实例（会实际分配一个 Worker 进程）。
    - actor.method.remote(*args): 异步调用 Actor 方法，返回 ObjectRef。
    - ray.get(ref): 获取结果。

注意：
    - Actor 是单线程顺序执行方法的，同一 Actor 的多个方法调用会排队。
    - 如果需要并行，应创建多个 Actor 实例。
"""

import time
import ray


# -----------------------------------------------------------
# 1. 定义一个 Actor 类
# -----------------------------------------------------------
# -----------------------------------------------------------
# 1. 定义一个基础 Actor 类
# -----------------------------------------------------------
@ray.remote
class Counter:
    def __init__(self, initial: int = 0):
        """Actor 构造函数，只会在 Actor 所在 Worker 上执行一次。"""
        self.value = initial
        print(f"Counter Actor 创建，初始值：{initial}")

    def increment(self, n: int = 1) -> int:
        """修改并返回 Actor 内部状态。"""
        self.value += n
        time.sleep(0.5)  # 模拟处理耗时
        return self.value

    def get_value(self) -> int:
        return self.value


# -----------------------------------------------------------
# 1.1 定义一个带资源需求的 Actor 类
# -----------------------------------------------------------
@ray.remote(num_cpus=1, num_gpus=0)  # 指定该 Actor 需要 1 个 CPU 核心
class ResourceIntensiveCounter:
    def __init__(self, initial: int = 0):
        """
        带资源需求的 Actor 构造函数
        - num_cpus=1: 该 Actor 实例需要 1 个 CPU 核心
        - num_gpus=0: 该 Actor 实例不需要 GPU
        """
        self.value = initial
        print(f"ResourceIntensiveCounter Actor 创建，初始值：{initial}，占用 1 个 CPU 核心")

    def increment(self, n: int = 1) -> int:
        """修改并返回 Actor 内部状态，模拟资源密集型操作。"""
        self.value += n
        time.sleep(0.5)  # 模拟处理耗时
        return self.value

    def get_value(self) -> int:
        return self.value


@ray.remote
def heavy_computation(x: int) -> int:
    """无状态 Task，供对比使用。"""
    time.sleep(0.5)
    return x * x


@ray.remote(num_cpus=0.5)  # 指定该 Task 需要 0.5 个 CPU 核心
def resource_consuming_task(x: int) -> int:
    """
    资源消耗型 Task，指定了 CPU 需求
    - num_cpus=0.5: 该 Task 需要 0.5 个 CPU 核心
    """
    time.sleep(0.5)
    return x * x * 2


def main():
    context = ray.init(ignore_reinit_error=True)
    print("Ray 已启动，Dashboard 地址：", context.dashboard_url)
    
    # 输出集群资源信息
    print('\n=== 集群资源信息 ===')
    print('Dashboard URL:', context.dashboard_url)
    print('Cluster Resources:', ray.cluster_resources())
    print('Available Resources:', ray.available_resources())
    print('Nodes:', ray.nodes())

    # -------------------------------------------------------
    # 2. 创建 Actor 实例
    # -------------------------------------------------------
    print("\n=== 创建单个 Counter Actor ===")
    counter = Counter.remote(initial=0)
    print(f"Actor handle: {counter}")

    # -------------------------------------------------------
    # 2.1 创建带资源需求的 Actor 实例
    # -------------------------------------------------------
    print("\n=== 创建带资源需求的 ResourceIntensiveCounter Actor ===")
    print(f"创建前可用资源：{ray.available_resources()}")
    resource_counter = ResourceIntensiveCounter.remote(initial=10)
    print(f"ResourceIntensiveCounter Actor handle: {resource_counter}")
    print(f"创建后可用资源：{ray.available_resources()}")
    
    # -------------------------------------------------------
    # 2.2 测试资源密集型 Actor 的方法调用
    # -------------------------------------------------------
    print("\n=== 调用 ResourceIntensiveCounter 的方法 ===")
    start_time = time.time()
    resource_refs = [resource_counter.increment.remote(i + 1) for i in range(3)]
    resource_results = ray.get(resource_refs)
    final_resource_value = ray.get(resource_counter.get_value.remote())
    print(f"ResourceIntensiveCounter 每次返回：{resource_results}")
    print(f"ResourceIntensiveCounter 最终状态：{final_resource_value}")
    print(f"ResourceIntensiveCounter 方法调用耗时：{time.time() - start_time:.2f}s")
    print(f"调用后可用资源：{ray.available_resources()}")

    # -------------------------------------------------------
    # 2.3 演示资源密集型 Task
    # -------------------------------------------------------
    print("\n=== 资源密集型 Task 执行 ===")
    print(f"执行前可用资源：{ray.available_resources()}")
    resource_task_refs = [resource_consuming_task.remote(i) for i in range(4)]
    resource_task_results = ray.get(resource_task_refs)
    print(f"资源密集型 Task 结果：{resource_task_results}")
    print(f"执行后可用资源：{ray.available_resources()}")
    
    # -------------------------------------------------------
    # 2.4 展示资源竞争情况
    # -------------------------------------------------------
    print("\n=== 资源竞争演示（如果 CPU 核心不足，任务会排队） ===")
    print(f"当前可用资源：{ray.available_resources()}")
    
    # 创建更多需要 1 个 CPU 的 Actor（如果系统 CPU 不足，会有排队现象）
    if ray.available_resources().get('CPU', 0) >= 1:
        print("系统有足够的 CPU 资源，创建更多 ResourceIntensiveCounter 实例...")
        more_counters = [ResourceIntensiveCounter.remote(initial=i*10) for i in range(int(ray.available_resources().get('CPU', 0)))]
        print(f"成功创建 {len(more_counters)} 个额外的 ResourceIntensiveCounter")
        print(f"创建后可用资源：{ray.available_resources()}")
    else:
        print("系统 CPU 资源不足，无法创建更多 ResourceIntensiveCounter 实例")

    # -------------------------------------------------------
    # 3. 调用 Actor 方法（状态保持）
    # -------------------------------------------------------
    print("\n=== 顺序调用 Actor 方法（状态累加）===")
    refs = [counter.increment.remote(i + 1) for i in range(5)]
    # Actor 方法在单个 Actor 内是顺序执行的，因此总耗时约 5 * 0.5 = 2.5s
    start = time.time()
    results = ray.get(refs)
    print(f"每次返回：{results}")
    print(f"最终状态：{ray.get(counter.get_value.remote())}")
    print(f"顺序调用耗时：{time.time() - start:.2f}s")

    # -------------------------------------------------------
    # 4. 创建多个 Actor 实现并行
    # -------------------------------------------------------
    print("\n=== 创建 3 个 Counter Actor 并行工作 ===")
    counters = [Counter.remote(initial=0) for _ in range(3)]
    start = time.time()
    refs = [c.increment.remote(1) for c in counters]
    results = ray.get(refs)
    print(f"3 个 Actor 并行返回：{results}，耗时：{time.time() - start:.2f}s")

    # -------------------------------------------------------
    # 5. Task 与 Actor 对比
    # -------------------------------------------------------
    print("\n=== Task 无状态并行 ===")
    start = time.time()
    refs = [heavy_computation.remote(i) for i in range(5)]
    results = ray.get(refs)
    print(f"Task 结果：{results}，耗时：{time.time() - start:.2f}s")
    
    # -------------------------------------------------------
    # 6. 补充资源管理测试
    # -------------------------------------------------------
    # 6.1 创建带资源需求的 Actor 实例
    # -------------------------------------------------------
    print("\n=== 创建带资源需求的 ResourceIntensiveCounter Actor ===")
    print(f"创建前可用资源：{ray.available_resources()}")
    resource_counter = ResourceIntensiveCounter.remote(initial=10)
    print(f"ResourceIntensiveCounter Actor handle: {resource_counter}")
    print(f"创建后可用资源：{ray.available_resources()}")
    
    # -------------------------------------------------------
    # 6.2 测试资源密集型 Actor 的方法调用
    # -------------------------------------------------------
    print("\n=== 调用 ResourceIntensiveCounter 的方法 ===")
    start_time = time.time()
    resource_refs = [resource_counter.increment.remote(i + 1) for i in range(3)]
    resource_results = ray.get(resource_refs)
    final_resource_value = ray.get(resource_counter.get_value.remote())
    print(f"ResourceIntensiveCounter 每次返回：{resource_results}")
    print(f"ResourceIntensiveCounter 最终状态：{final_resource_value}")
    print(f"ResourceIntensiveCounter 方法调用耗时：{time.time() - start_time:.2f}s")
    print(f"调用后可用资源：{ray.available_resources()}")

    # -------------------------------------------------------
    # 6.3 演示资源密集型 Task
    # -------------------------------------------------------
    print("\n=== 资源密集型 Task 执行 ===")
    print(f"执行前可用资源：{ray.available_resources()}")
    resource_task_refs = [resource_consuming_task.remote(i) for i in range(4)]
    resource_task_results = ray.get(resource_task_refs)
    print(f"资源密集型 Task 结果：{resource_task_results}")
    print(f"执行后可用资源：{ray.available_resources()}")
    
    # -------------------------------------------------------
    # 6.4 展示资源竞争情况
    # -------------------------------------------------------
    print("\n=== 资源竞争演示（如果 CPU 核心不足，任务会排队） ===")
    print(f"当前可用资源：{ray.available_resources()}")
    
    # 创建更多需要 1 个 CPU 的 Actor（如果系统 CPU 不足，会有排队现象）
    if ray.available_resources().get('CPU', 0) >= 1:
        print("系统有足够的 CPU 资源，创建更多 ResourceIntensiveCounter 实例...")
        more_counters = [ResourceIntensiveCounter.remote(initial=i*10) for i in range(int(ray.available_resources().get('CPU', 0)))]
        print(f"成功创建 {len(more_counters)} 个额外的 ResourceIntensiveCounter")
        print(f"创建后可用资源：{ray.available_resources()}")
    else:
        print("系统 CPU 资源不足，无法创建更多 ResourceIntensiveCounter 实例")

    ray.shutdown()
    print("\nRay 已关闭")


if __name__ == "__main__":
    main()
