"""
Ray 实战示例 06：模拟 SecretFlow 风格的联邦聚合

本节目标：
    1. 理解 Ray 如何作为 SecretFlow 的底层分布式执行引擎。
    2. 模拟多个 Party（参与方）各自持有本地数据，通过 Actor 并行计算本地统计量。
    3. 协调方（Aggregator）收集各 Party 的中间结果并做安全聚合。
    4. 结合 Placement Group 把不同 Party 的计算资源做逻辑隔离。

关键概念：
    - Party：联邦学习中的参与方，每个 Party 拥有自己的本地数据。
    - Local Actor：每个 Party 上运行一个 Ray Actor，负责本地数据读取和计算。
    - Aggregator Actor：协调方 Actor，负责收集结果并聚合。
    - Placement Group：用于把不同 Party 的 Actor 放到不同节点（本地模式仅演示 API）。

注意：
    - 本示例使用明文求和/均值聚合，仅用于学习 Ray 调度模式，不是真正的安全计算。
    - SecretFlow 在 Ray 之上封装了 SPU（安全多方计算单元），会在 SPU 中执行真正的密文运算。
"""

import time
import numpy as np
import ray
from ray.util.placement_group import placement_group


@ray.remote(num_cpus=0.5)
class Party:
    """模拟联邦学习中的一个参与方。"""

    def __init__(self, party_id: str, num_samples: int = 1000, dim: int = 10):
        self.party_id = party_id
        # 模拟每个 Party 本地私有数据
        np.random.seed(hash(party_id) % (2 ** 32))
        self.data = np.random.randn(num_samples, dim)
        print(f"Party {party_id} 已创建，本地数据 shape: {self.data.shape}")

    def compute_local_gradient(self, weights: np.ndarray) -> dict:
        """
        模拟本地训练：给定全局权重，计算本地梯度。
        返回值用 dict 包装，方便后续聚合。
        """
        time.sleep(0.5)  # 模拟训练耗时
        # 为了演示效果，构造一个非零伪梯度：
        # pred = X @ weights + sample_noise
        # grad = X^T @ pred / N
        sample_noise = np.random.randn(self.data.shape[0]) * 0.1
        pred = self.data @ weights + sample_noise
        grad = self.data.T @ pred / len(self.data)
        return {
            "party_id": self.party_id,
            "gradient": grad,
            "num_samples": len(self.data),
        }

    def get_data_shape(self) -> tuple:
        return self.data.shape


@ray.remote(num_cpus=0.5)
class Aggregator:
    """协调方：收集各 Party 的本地梯度并聚合。"""

    def __init__(self):
        self.round = 0

    def aggregate(self, local_results: list) -> np.ndarray:
        """
        加权平均聚合梯度。
        真实 SecretFlow 中，这一步会在 SPU 内做安全聚合。
        """
        total_samples = sum(r["num_samples"] for r in local_results)
        aggregated = np.zeros_like(local_results[0]["gradient"])
        for r in local_results:
            weight = r["num_samples"] / total_samples
            aggregated += weight * r["gradient"]
        self.round += 1
        print(f"Aggregator 完成第 {self.round} 轮聚合，总样本数：{total_samples}")
        return aggregated


def main():
    context = ray.init(ignore_reinit_error=True)
    print("Ray 已启动，Dashboard 地址：", context.dashboard_url)

    # -------------------------------------------------------
    # 1. 创建 Placement Group（模拟把不同 Party 放到不同节点）
    # -------------------------------------------------------
    num_parties = 3
    bundles = [{"CPU": 0.5} for _ in range(num_parties + 1)]  # 3 Party + 1 Aggregator
    pg = placement_group(bundles, strategy="SPREAD", name="federated_pg")
    ray.get(pg.ready())
    print(f"\nPlacement Group 就绪：{pg.bundle_specs}")

    # -------------------------------------------------------
    # 2. 创建 Party Actor（绑定到 Placement Group）
    # -------------------------------------------------------
    print("\n=== 创建联邦参与方 Party Actors ===")
    parties = [
        Party.options(placement_group=pg).remote(
            party_id=f"Party-{i}",
            num_samples=1000 + i * 200,
            dim=5,
        )
        for i in range(num_parties)
    ]

    # 查看各 Party 数据
    shapes = ray.get([p.get_data_shape.remote() for p in parties])
    for party_id, shape in enumerate(shapes):
        print(f"  Party-{party_id} 数据 shape: {shape}")

    # -------------------------------------------------------
    # 3. 创建 Aggregator Actor
    # -------------------------------------------------------
    aggregator = Aggregator.options(placement_group=pg).remote()
    print("\n协调方 Aggregator 已创建")

    # -------------------------------------------------------
    # 4. 模拟联邦学习多轮迭代
    # -------------------------------------------------------
    num_rounds = 3
    dim = 5
    global_weights = np.zeros(dim)

    print("\n=== 开始联邦训练 ===")
    start = time.time()
    for rnd in range(num_rounds):
        print(f"\n--- 第 {rnd + 1} 轮 ---")

        # Step 4.1：各 Party 并行计算本地梯度
        local_refs = [p.compute_local_gradient.remote(global_weights) for p in parties]
        local_results = ray.get(local_refs)

        # Step 4.2：Aggregator 聚合（真实场景会调用 SPU 安全聚合）
        global_weights_ref = aggregator.aggregate.remote(local_results)
        global_weights = ray.get(global_weights_ref)

        print(f"聚合后全局权重范数：{np.linalg.norm(global_weights):.4f}")

    print(f"\n联邦训练完成，总耗时：{time.time() - start:.2f}s")

    # -------------------------------------------------------
    # 5. 清理
    # -------------------------------------------------------
    ray.util.remove_placement_group(pg)
    print("\nPlacement Group 已移除")

    ray.shutdown()
    print("Ray 已关闭")


if __name__ == "__main__":
    main()
