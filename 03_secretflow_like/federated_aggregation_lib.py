"""
联邦聚合核心库：可被示例脚本和单元测试共同导入。

本模块包含联邦学习示例的核心组件：
    - Party: 参与方 Actor，持有本地数据并计算本地梯度
    - Aggregator: 协调方 Actor，聚合各 Party 的梯度
    - aggregate_gradients: 纯函数版本的加权平均聚合
    - init_ray: 支持本地模式和多节点集群的 Ray 初始化
    - run_federated_training: 标准 FedAvg 训练循环
"""

import os
import time
from typing import Optional

import numpy as np
import ray
from ray.util.placement_group import placement_group


@ray.remote(num_cpus=0.5)
class Party:
    """
    模拟联邦学习中的一个参与方（Party）。

    每个 Party Actor 代表一个独立的参与方，拥有自己的私有数据，
    在本地进行计算而不共享原始数据，符合联邦学习的隐私保护原则。
    """

    def __init__(self, party_id: str, num_samples: int = 1000, dim: int = 10):
        """
        初始化 Party，生成模拟的本地私有数据。

        Args:
            party_id: 参与方的唯一标识符（如 "Party-0", "Party-1"）
            num_samples: 本地数据集的样本数量，默认1000
            dim: 特征维度，默认10维
        """
        self.party_id = party_id
        # 使用 party_id 的哈希值作为随机种子，确保不同 Party 的数据不同但可复现
        np.random.seed(hash(party_id) % (2 ** 32))
        # 生成服从标准正态分布的随机数据：shape = (num_samples, dim)
        self.data = np.random.randn(num_samples, dim)
        print(f"Party {party_id} 已创建，本地数据 shape: {self.data.shape}")

    def compute_local_gradient(self, weights: np.ndarray) -> dict:
        """
        模拟本地训练：给定全局权重，计算本地梯度。

        这是联邦学习的核心步骤：每个 Party 基于当前的全局模型参数，
        在本地数据上计算梯度，然后将梯度（而非原始数据）发送给协调方。

        Args:
            weights: 当前的全局权重向量，shape = (dim,)

        Returns:
            包含以下字段的字典：
            - party_id: 参与方ID
            - gradient: 计算得到的本地梯度，shape = (dim,)
            - num_samples: 本地样本数量，用于加权聚合

        数学原理：
            伪标签预测: pred = X @ weights + noise
            梯度计算: grad = X^T @ pred / N
            这类似于线性回归的最小二乘梯度
        """
        time.sleep(0.5)  # 模拟真实训练的计算耗时

        # 添加小的随机噪声，使梯度非零且更接近真实场景
        sample_noise = np.random.randn(self.data.shape[0]) * 0.1

        # 前向传播：计算预测值（线性变换 + 噪声）
        # self.data: (N, dim), weights: (dim,) → pred: (N,)
        pred = self.data @ weights + sample_noise

        # 反向传播：计算梯度
        # self.data.T: (dim, N), pred: (N,) → grad: (dim,)
        grad = self.data.T @ pred / len(self.data)

        return {
            "party_id": self.party_id,
            "gradient": grad,
            "num_samples": len(self.data),
        }

    def get_data_shape(self) -> tuple:
        """
        获取本地数据的形状信息。

        Returns:
            数据形状的元组 (num_samples, dim)
        """
        return self.data.shape


@ray.remote(num_cpus=0.5)
class Aggregator:
    """
    协调方（Aggregator）：收集各 Party 的本地梯度并执行聚合。

    在真实的 SecretFlow 框架中，这一步会在 SPU（安全多方计算单元）内
    执行加密的安全聚合，确保各方无法窥探彼此的梯度信息。
    本示例使用明文聚合，仅用于演示 Ray 的调度模式。
    """

    def __init__(self):
        """初始化聚合器，记录当前训练轮次。"""
        self.round = 0

    def aggregate(self, local_results: list) -> np.ndarray:
        """
        执行加权平均聚合（FedAvg 算法的核心步骤）。

        根据各 Party 的样本数量进行加权平均，样本数越多的 Party，
        其梯度对全局模型的贡献越大。

        Args:
            local_results: 包含所有 Party 本地计算结果的列表，
                          每个元素是 compute_local_gradient() 返回的字典

        Returns:
            聚合后的全局梯度向量，shape = (dim,)

        聚合公式：
            total_samples = Σ num_samples_i
            weight_i = num_samples_i / total_samples
            aggregated_grad = Σ (weight_i * gradient_i)

        注意：
            真实 SecretFlow 中，这一步会在 SPU 内做安全聚合，
            使用秘密分享、同态加密等密码学技术保护隐私。
        """
        # 计算所有 Party 的总样本数
        total_samples = sum(r["num_samples"] for r in local_results)

        # 初始化聚合梯度为零向量，形状与第一个 Party 的梯度相同
        aggregated = np.zeros_like(local_results[0]["gradient"])

        # 按样本数加权累加各 Party 的梯度
        for r in local_results:
            weight = r["num_samples"] / total_samples  # 计算该 Party 的权重
            aggregated += weight * r["gradient"]       # 加权累加

        self.round += 1
        print(f"Aggregator 完成第 {self.round} 轮聚合，总样本数：{total_samples}")
        return aggregated


def aggregate_gradients(local_results: list) -> np.ndarray:
    """
    纯函数版本的加权平均聚合，便于单元测试和不依赖 Ray 环境的验证。

    Args:
        local_results: 各 Party 本地计算结果列表，每个元素包含
                      "gradient" 和 "num_samples" 字段

    Returns:
        加权平均后的聚合梯度

    Raises:
        ValueError: 当 local_results 为空时抛出
    """
    if not local_results:
        raise ValueError("local_results cannot be empty")

    total_samples = sum(r["num_samples"] for r in local_results)
    aggregated = np.zeros_like(local_results[0]["gradient"])
    for r in local_results:
        weight = r["num_samples"] / total_samples
        aggregated += weight * r["gradient"]
    return aggregated


def init_ray(address: Optional[str] = None, num_cpus: Optional[int] = None):
    """
    初始化 Ray 运行时环境。

    支持两种模式：
      1. 本地模式：不传 address，直接在本地启动 Ray 集群
      2. 多节点模式：传入 head node 地址，连接到已有的 Ray 集群

    Args:
        address: Ray head node 地址，例如 "ray://192.168.1.10:10001"
                 如果为 None，则在本地启动 Ray
        num_cpus: 本地模式下的 CPU 数量限制（多节点模式下无效）

    Returns:
        Ray 上下文对象
    """
    if address:
        print(f"正在连接到 Ray 集群：{address}")
        return ray.init(address=address)
    else:
        print("未提供 Ray 集群地址，使用本地模式启动 Ray")
        kwargs = {"ignore_reinit_error": True}
        if num_cpus is not None:
            kwargs["num_cpus"] = num_cpus
        return ray.init(**kwargs)


def run_federated_training(
    parties,
    aggregator,
    num_rounds: int = 3,
    learning_rate: float = 0.1,
    dim: int = 5,
    initial_weights: Optional[np.ndarray] = None,
) -> np.ndarray:
    """
    执行标准联邦训练（FedAvg）循环。

    Args:
        parties: Party Actor handle 列表
        aggregator: Aggregator Actor handle
        num_rounds: 训练轮数
        learning_rate: 学习率
        dim: 权重维度
        initial_weights: 初始全局权重，默认为零向量

    Returns:
        训练结束后的全局权重
    """
    if initial_weights is None:
        global_weights = np.zeros(dim)
    else:
        global_weights = initial_weights.copy()

    for _ in range(num_rounds):
        # 各 Party 并行计算本地梯度
        local_refs = [p.compute_local_gradient.remote(global_weights) for p in parties]
        local_results = ray.get(local_refs)

        # Aggregator 聚合梯度
        aggregated_gradient = ray.get(aggregator.aggregate.remote(local_results))

        # 按学习率更新全局权重
        global_weights = global_weights - learning_rate * aggregated_gradient

    return global_weights
