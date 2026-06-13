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


@ray.remote
def heavy_computation(x: int) -> int:
    """无状态 Task，供对比使用。"""
    time.sleep(0.5)
    return x * x


def main():
    context = ray.init(ignore_reinit_error=True)
    print("Ray 已启动，Dashboard 地址：", context.dashboard_url)

    # -------------------------------------------------------
    # 2. 创建 Actor 实例
    # -------------------------------------------------------
    print("\n=== 创建单个 Counter Actor ===")
    counter = Counter.remote(initial=0)
    print(f"Actor handle: {counter}")

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

    ray.shutdown()
    print("\nRay 已关闭")


if __name__ == "__main__":
    main()
