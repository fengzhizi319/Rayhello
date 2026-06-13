"""
Ray 基础示例 03：对象存储（Object Store）

本节目标：
    1. 理解 Ray 的 Object Store 是 Task/Actor 之间共享数据的中心。
    2. 掌握 ray.put() 显式把对象放入对象存储。
    3. 学会在 Task 之间传递 ObjectRef，避免重复传输大对象。

关键 API：
    - ray.put(obj): 把对象放入 Object Store，返回 ObjectRef。
    - ray.get(ref): 从 Object Store 取出对象。
    - 在 Task 参数中直接传递 ObjectRef，Ray 会自动解析。

注意：
    - Object Store 有容量限制（默认约 30% 内存），大对象要合理分片。
    - ObjectRef 可以被多个 Task/Method 引用，底层只存一份数据。
"""

import time
import numpy as np
import ray


@ray.remote
def process_with_data(data: np.ndarray, multiplier: float) -> float:
    """
    接收 numpy 数组，返回求和 * multiplier。
    参数可以直接是 ObjectRef，Ray 会自动 get。
    """
    time.sleep(0.5)
    return float(np.sum(data) * multiplier)


@ray.remote
def share_data_between_tasks(data: list, task_id: int) -> int:
    """
    两个 Task 共享同一个 ObjectRef 指向的数据。
    当把 ObjectRef 作为 remote() 参数时，Ray 会自动从 Object Store 拉取真实值传入。
    """
    return len(data) + task_id


def main():
    context = ray.init(ignore_reinit_error=True)
    print("Ray 已启动，Dashboard 地址：", context.dashboard_url)

    # -------------------------------------------------------
    # 1. 显式把对象放入 Object Store
    # -------------------------------------------------------
    print("\n=== ray.put() 示例 ===")
    arr = np.arange(1_000_000)
    arr_ref = ray.put(arr)  # 放入对象存储
    print(f"本地对象大小：{arr.nbytes / 1024 / 1024:.2f} MB")
    print(f"ObjectRef: {arr_ref}")

    # 通过 ray.get() 取出（本地演示，实际在分布式节点间传输）
    arr_back = ray.get(arr_ref)
    print(f"取回后形状：{arr_back.shape}")

    # -------------------------------------------------------
    # 2. 把 ObjectRef 传给 Task
    # -------------------------------------------------------
    print("\n=== 多个 Task 共享同一个 ObjectRef ===")
    # 只传输一次大对象到 Object Store，多个 Task 引用同一个 ref
    start = time.time()
    refs = [process_with_data.remote(arr_ref, i + 1) for i in range(4)]
    results = ray.get(refs)
    print(f"结果：{results}，耗时：{time.time() - start:.2f}s")

    # 对比：每次传值会导致对象被序列化/传输多次
    print("\n=== 对比：每次复制大对象 ===")
    start = time.time()
    refs = [process_with_data.remote(arr.copy(), i + 1) for i in range(4)]
    results = ray.get(refs)
    print(f"结果：{results}，耗时：{time.time() - start:.2f}s")

    # -------------------------------------------------------
    # 3. ObjectRef 在 Task 之间传递
    # -------------------------------------------------------
    print("\n=== Task A 产生 ObjectRef，Task B 消费 ===")
    data_list = list(range(100))
    list_ref = ray.put(data_list)

    # 直接把 ObjectRef 传给另一个 Task，Ray 会自动解析为真实对象
    refs = [share_data_between_tasks.remote(list_ref, i) for i in range(5)]
    results = ray.get(refs)
    print(f"结果：{results}")

    # -------------------------------------------------------
    # 4. 等待多个 ObjectRef（ray.wait）
    # -------------------------------------------------------
    print("\n=== ray.wait() 等待任意一个完成 ===")
    refs = [process_with_data.remote(arr_ref, i + 1) for i in range(5)]
    ready, remaining = ray.wait(refs, num_returns=1, timeout=10)
    print(f"最先完成：{ray.get(ready[0])}")
    print(f"剩余未完成的 ObjectRef 数量：{len(remaining)}")

    ray.shutdown()
    print("\nRay 已关闭")


if __name__ == "__main__":
    main()
