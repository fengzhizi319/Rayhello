"""
Ray 基础示例 01：远程函数（Remote Task）

本节目标：
    1. 学会用 @ray.remote 把普通函数变成可在集群中并行执行的 Task。
    2. 理解 ray.get() 是阻塞式获取结果，而 remote() 调用是异步提交。
    3. 观察多个 Task 如何被 Ray 调度到不同 Worker 进程并行执行。

关键 API：
    - ray.init(): 初始化本地 Ray 集群（或连接到已有集群）。
    - @ray.remote: 装饰器，声明这是一个远程函数/类。
    - func.remote(*args, **kwargs): 提交一个 Task，立即返回 ObjectRef。
    - ray.get(object_ref): 阻塞等待并获取 ObjectRef 指向的真实结果。
    - ray.shutdown(): 关闭当前集群连接。
"""

import time
import ray


# -----------------------------------------------------------
# 1. 定义一个普通函数，然后用 @ray.remote 装饰成远程 Task
# -----------------------------------------------------------
def slow_add_local(a: int, b: int) -> int:
    """本地版本，用于串行对比。"""
    time.sleep(0.5)
    return a + b


@ray.remote
def slow_add(a: int, b: int) -> int:
    """
    模拟一个耗时计算：两数相加，耗时 0.5 秒。
    注意：Task 里不要访问外部变量，必须是纯函数（无状态）。
    """
    time.sleep(0.5)  # 模拟计算耗时
    return a + b


def main():
    # -------------------------------------------------------
    # 2. 初始化 Ray（本地模式）
    # -------------------------------------------------------
    # ignore_reinit_error=True 避免重复初始化报错，方便 jupyter/interactive 环境
    context = ray.init(ignore_reinit_error=True)
    print("Ray 已启动，Dashboard 地址：", context.dashboard_url)

    # -------------------------------------------------------
    # 3. 串行执行 vs 并行执行对比
    # -------------------------------------------------------
    n_tasks = 8

    print("\n=== 串行执行（普通 Python）===")
    start = time.time()
    results_serial = []
    for i in range(n_tasks):
        # 普通函数调用：在本地主进程顺序执行
        results_serial.append(slow_add_local(i, i + 1))
    print(f"串行结果：{results_serial}，耗时：{time.time() - start:.2f}s")

    print("\n=== 并行执行（Ray Task）===")
    start = time.time()
    refs = []
    for i in range(n_tasks):
        # 提交 n_tasks 个 Task，立即返回 n_tasks 个 ObjectRef
        ref = slow_add.remote(i, i + 1)
        refs.append(ref)
        print(f"  已提交 Task {i}, ObjectRef: {ref}")

    # 此时 n_tasks 个 Task 正在后台并行执行
    # ray.get(refs) 会阻塞等待所有结果返回
    results_parallel = ray.get(refs)
    print(f"并行结果：{results_parallel}，耗时：{time.time() - start:.2f}s")

    # -------------------------------------------------------
    # 4. 单个 Task 的 ObjectRef 使用
    # -------------------------------------------------------
    print("\n=== 单个 ObjectRef 示例 ===")
    ref = slow_add.remote(10, 20)
    print(f"ObjectRef: {ref}，尚未 get")
    value = ray.get(ref)
    print(f"结果：{value}")

    # -------------------------------------------------------
    # 5. 清理
    # -------------------------------------------------------
    ray.shutdown()
    print("\nRay 已关闭")


if __name__ == "__main__":
    main()
