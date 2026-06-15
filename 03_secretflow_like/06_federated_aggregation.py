"""
Ray 实战示例 06：模拟 SecretFlow 风格的联邦聚合

本节目标：
    1. 理解 Ray 如何作为 SecretFlow 的底层分布式执行引擎。
    2. 模拟多个 Party（参与方）各自持有本地数据，通过 Actor 并行计算本地统计量。
    3. 协调方（Aggregator）收集各 Party 的中间结果并做安全聚合。
    4. 结合 Placement Group 把不同 Party 的计算资源做逻辑隔离。
    5. 支持连接到真实多节点 Ray 集群（head + worker），演示联邦学习跨节点部署。

关键概念：
    - Party：联邦学习中的参与方，每个 Party 拥有自己的本地数据。
    - Local Actor：每个 Party 上运行一个 Ray Actor，负责本地数据读取和计算。
    - Aggregator Actor：协调方 Actor，负责收集结果并聚合。
    - Placement Group：用于把不同 Party 的 Actor 放到不同节点。

多节点 Ray 集群启动方式：
    1. 选择一台机器作为 Head Node：
       ray start --head --port=6379 --dashboard-host=0.0.0.0
    2. 其他节点作为 Worker Node 加入集群：
       ray start --address="<head_node_ip>:6379"
    3. 运行本脚本连接到 Head Node：
       python 06_federated_aggregation.py --ray-address=ray://<head_node_ip>:10001
       或设置环境变量：
       RAY_ADDRESS=ray://<head_node_ip>:10001 python 06_federated_aggregation.py

注意：
    - 本示例使用明文求和/均值聚合，仅用于学习 Ray 调度模式，不是真正的安全计算。
    - SecretFlow 在 Ray 之上封装了 SPU（安全多方计算单元），会在 SPU 中执行真正的密文运算。
"""

import argparse
import os
import time

import numpy as np
import ray
from ray.util.placement_group import placement_group

from federated_aggregation_lib import (
    Aggregator,
    Party,
    init_ray,
    run_federated_training,
)


def main():
    """
    主函数：演示完整的联邦学习流程。

    流程概述：
        1. 解析命令行参数，决定本地模式或多节点模式
        2. 初始化 Ray 集群
        3. 创建 Placement Group 进行资源隔离
        4. 启动多个 Party Actor（参与方）
        5. 启动 Aggregator Actor（协调方）
        6. 执行多轮联邦训练迭代
        7. 测试不同的聚合策略和收敛性
        8. 清理资源
    """
    # -------------------------------------------------------
    # 0. 解析命令行参数
    # -------------------------------------------------------
    parser = argparse.ArgumentParser(
        description="模拟 SecretFlow 风格的联邦聚合示例",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
多节点集群运行示例：
  1. Head Node:  ray start --head --port=6379
  2. Worker Node: ray start --address="<head_ip>:6379"
  3. 运行脚本:    python 06_federated_aggregation.py --ray-address=ray://<head_ip>:10001
        """,
    )
    parser.add_argument(
        "--ray-address",
        type=str,
        default=os.environ.get("RAY_ADDRESS", None),
        help='Ray head node 地址（默认读取环境变量 RAY_ADDRESS），例如 ray://192.168.1.10:10001',
    )
    parser.add_argument(
        "--num-cpus",
        type=int,
        default=None,
        help="本地模式下的 CPU 数量限制（仅本地模式有效）",
    )
    args = parser.parse_args()

    # -------------------------------------------------------
    # 1. 初始化 Ray 运行时环境
    # -------------------------------------------------------
    context = init_ray(address=args.ray_address, num_cpus=args.num_cpus)
    print("Ray 已启动，Dashboard 地址：", context.dashboard_url)

    # 输出集群资源信息
    print('\n=== 集群资源信息 ===')
    print('Dashboard URL:', context.dashboard_url)
    print('Cluster Resources:', ray.cluster_resources())
    print('Available Resources:', ray.available_resources())
    print('Nodes:', ray.nodes())

    # -------------------------------------------------------
    # 2. 重要概念：联邦学习与 Ray 的结合
    # -------------------------------------------------------
    print('\n=== 联邦学习与 Ray 结合的重要概念 ===')
    print('1. 联邦学习是一种分布式机器学习范式，数据保留在本地不移动')
    print('2. Ray 作为底层分布式执行引擎，提供任务调度和资源管理')
    print('3. Party Actor 代表各个参与方，拥有自己的本地数据和计算能力')
    print('4. Aggregator Actor 协调各方计算结果的聚合')
    print('5. Placement Group 用于模拟真实的分布式部署环境')
    print('6. 多节点模式下，不同 Party 可被调度到不同物理节点')

    # -------------------------------------------------------
    # 3. 创建 Placement Group（模拟把不同 Party 放到不同节点）
    # -------------------------------------------------------
    num_parties = 3
    # 创建资源束（bundle）：每个 Party 和 Aggregator 各占用 0.5 CPU
    bundles = [{"CPU": 0.5} for _ in range(num_parties + 1)]  # 3 Party + 1 Aggregator
    pg = placement_group(bundles, strategy="SPREAD", name="federated_pg")
    ray.get(pg.ready())  # 等待 Placement Group 就绪
    print(f"\nPlacement Group 就绪：{pg.bundle_specs}")

    # -------------------------------------------------------
    # 4. 创建 Party Actor（绑定到 Placement Group）
    # -------------------------------------------------------
    print("\n=== 创建联邦参与方 Party Actors ===")
    # 使用 .options(placement_group=pg) 将 Actor 绑定到 Placement Group
    # 在真实多节点集群中，SPREAD 策略会尽量把不同 Party 放到不同节点
    parties = [
        Party.options(placement_group=pg).remote(
            party_id=f"Party-{i}",
            num_samples=1000 + i * 200,
            dim=5,
        )
        for i in range(num_parties)
    ]

    # 通过远程调用获取各 Party 的数据形状信息
    shapes = ray.get([p.get_data_shape.remote() for p in parties])
    for party_id, shape in enumerate(shapes):
        print(f"  Party-{party_id} 数据 shape: {shape}")

    # -------------------------------------------------------
    # 5. 创建 Aggregator Actor
    # -------------------------------------------------------
    aggregator = Aggregator.options(placement_group=pg).remote()
    print("\n协调方 Aggregator 已创建")

    # -------------------------------------------------------
    # 6. 模拟联邦学习多轮迭代（FedAvg 算法）
    # -------------------------------------------------------
    print("\n=== 开始联邦训练 ===")
    start = time.time()
    global_weights = run_federated_training(
        parties=parties,
        aggregator=aggregator,
        num_rounds=3,
        learning_rate=0.1,
        dim=5,
    )
    print(f"\n聚合后全局权重范数：{np.linalg.norm(global_weights):.4f}")
    print(f"联邦训练完成，总耗时：{time.time() - start:.2f}s")

    # -------------------------------------------------------
    # 7. 补充联邦学习详细测试
    # -------------------------------------------------------
    print("\n=== 补充联邦学习详细测试 ===")

    # 7.1 分析各轮训练的详细信息（梯度变化、样本分布等）
    print("\n--- 分析各轮训练的梯度变化 ---")
    num_rounds_analysis = 2
    learning_rate_analysis = 0.1
    global_weights_analysis = np.zeros(5)

    for rnd in range(num_rounds_analysis):
        print(f"\n--- 第 {rnd + 1} 轮分析 ---")

        # 各 Party 并行计算本地梯度
        local_refs = [p.compute_local_gradient.remote(global_weights_analysis) for p in parties]
        local_results = ray.get(local_refs)

        # 打印每个 Party 的梯度信息，观察各方贡献差异
        for result in local_results:
            print(f"  {result['party_id']} - 样本数: {result['num_samples']}, 梯度范数: {np.linalg.norm(result['gradient']):.4f}")

        # Aggregator 聚合
        aggregated_gradient_analysis = ray.get(aggregator.aggregate.remote(local_results))
        global_weights_analysis = global_weights_analysis - learning_rate_analysis * aggregated_gradient_analysis
        print(f"  聚合后全局权重范数：{np.linalg.norm(global_weights_analysis):.4f}")

    # 7.2 测试不同聚合策略：简单平均 vs 加权平均
    print("\n--- 测试简单平均聚合策略 ---")
    @ray.remote
    def simple_average_aggregate(local_results: list) -> np.ndarray:
        """
        简单平均聚合策略：不考虑样本数差异，直接对所有梯度求算术平均。
        """
        gradients = [r["gradient"] for r in local_results]
        avg_gradient = np.mean(gradients, axis=0)
        print(f"简单平均聚合完成，梯度范数：{np.linalg.norm(avg_gradient):.4f}")
        return avg_gradient

    # 执行简单平均聚合并观察结果
    local_refs = [p.compute_local_gradient.remote(global_weights_analysis) for p in parties]
    local_results = ray.get(local_refs)
    simple_avg_result = ray.get(simple_average_aggregate.remote(local_results))

    # 7.3 测试联邦学习收敛性：观察权重变化是否趋于稳定
    print("\n--- 测试联邦学习收敛性 ---")
    convergence_threshold = 1e-3
    max_iterations = 10
    learning_rate_conv = 0.6
    global_weights_conv = np.random.randn(5)
    prev_global_weights = None

    for iteration in range(max_iterations):
        print(f"\n--- 收敛性测试 - 第 {iteration + 1} 轮 ---")

        # 各 Party 并行计算本地梯度
        local_refs = [p.compute_local_gradient.remote(global_weights_conv) for p in parties]
        local_results = ray.get(local_refs)

        # Aggregator 聚合梯度
        aggregated_gradient_conv = ray.get(aggregator.aggregate.remote(local_results))
        # 更新全局权重
        new_global_weights = global_weights_conv - learning_rate_conv * aggregated_gradient_conv

        # 计算权重变化量（L2 范数），判断是否收敛
        if prev_global_weights is not None:
            weight_diff = np.linalg.norm(new_global_weights - prev_global_weights)
            print(f"  权重变化范数：{weight_diff:.6f}")
            if weight_diff < convergence_threshold:
                print(f"  达到收敛阈值 ({convergence_threshold})，停止训练")
                break

        # 更新权重，进入下一轮
        prev_global_weights = new_global_weights
        global_weights_conv = new_global_weights
        print(f"  当前全局权重范数：{np.linalg.norm(global_weights_conv):.4f}")

    # -------------------------------------------------------
    # 8. 清理资源
    # -------------------------------------------------------
    # 移除 Placement Group，释放预留的资源
    ray.util.remove_placement_group(pg)
    print("\nPlacement Group 已移除")

    # 关闭 Ray 运行时环境
    ray.shutdown()
    print("Ray 已关闭")


if __name__ == "__main__":
    main()
