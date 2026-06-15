"""
Unit tests for 03_secretflow_like/06_federated_aggregation.py

本测试文件覆盖以下核心逻辑：
    1. aggregate_gradients 纯函数的加权平均计算
    2. Party Actor 的本地梯度计算
    3. Aggregator Actor 的加权聚合
    4. run_federated_training 端到端训练流程

运行方式：
    pytest tests/test_federated_aggregation.py -v

注意：
    测试使用本地 Ray 模式，不需要真实多节点集群。
"""

import sys
from pathlib import Path

import numpy as np
import pytest
import ray

# 将 03_secretflow_like 目录加入 Python 路径，以便导入 federated_aggregation_lib
sys.path.insert(0, str(Path(__file__).parent.parent / "03_secretflow_like"))

from federated_aggregation_lib import (
    Aggregator,
    Party,
    aggregate_gradients,
    run_federated_training,
)


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    """
    为整个测试模块启动和关闭本地 Ray 集群。

    scope="module" 表示所有测试用例共享同一个 Ray 实例，避免频繁启停。
    """
    context = ray.init(ignore_reinit_error=True)
    yield context
    ray.shutdown()


def test_aggregate_gradients_basic():
    """
    测试 aggregate_gradients 的基本加权平均逻辑。

    场景：两个 Party，样本数分别为 100 和 300，梯度分别为 [1, 1] 和 [3, 3]。
    期望结果：[(100*1 + 300*3)/400, (100*1 + 300*3)/400] = [2.5, 2.5]
    """
    local_results = [
        {"party_id": "Party-0", "gradient": np.array([1.0, 1.0]), "num_samples": 100},
        {"party_id": "Party-1", "gradient": np.array([3.0, 3.0]), "num_samples": 300},
    ]
    result = aggregate_gradients(local_results)
    expected = np.array([2.5, 2.5])
    np.testing.assert_allclose(result, expected, rtol=1e-7)


def test_aggregate_gradients_empty():
    """
    测试 aggregate_gradients 对空输入的异常处理。
    """
    with pytest.raises(ValueError, match="local_results cannot be empty"):
        aggregate_gradients([])


def test_aggregate_gradients_single_party():
    """
    测试只有一个 Party 时，聚合结果等于该 Party 的梯度。
    """
    local_results = [
        {"party_id": "Party-0", "gradient": np.array([2.0, -1.0, 0.5]), "num_samples": 1000},
    ]
    result = aggregate_gradients(local_results)
    np.testing.assert_allclose(result, np.array([2.0, -1.0, 0.5]), rtol=1e-7)


def test_party_local_gradient_shape():
    """
    测试 Party Actor 计算得到的本地梯度形状是否正确。
    """
    party = Party.remote(party_id="Party-test", num_samples=500, dim=5)
    weights = np.zeros(5)
    result = ray.get(party.compute_local_gradient.remote(weights))

    assert result["party_id"] == "Party-test"
    assert result["num_samples"] == 500
    assert result["gradient"].shape == (5,)
    assert isinstance(result["gradient"], np.ndarray)


def test_party_get_data_shape():
    """
    测试 Party Actor 返回的数据形状信息是否正确。
    """
    party = Party.remote(party_id="Party-shape", num_samples=800, dim=10)
    shape = ray.get(party.get_data_shape.remote())
    assert shape == (800, 10)


def test_aggregator_weighted_average():
    """
    测试 Aggregator Actor 是否按样本数正确加权聚合。
    """
    aggregator = Aggregator.remote()
    local_results = [
        {"party_id": "Party-0", "gradient": np.array([10.0, 0.0]), "num_samples": 100},
        {"party_id": "Party-1", "gradient": np.array([0.0, 10.0]), "num_samples": 900},
    ]
    result = ray.get(aggregator.aggregate.remote(local_results))
    # 加权结果: [100*10/1000, 900*10/1000] = [1.0, 9.0]
    expected = np.array([1.0, 9.0])
    np.testing.assert_allclose(result, expected, rtol=1e-7)


def test_run_federated_training_zeros():
    """
    测试 run_federated_training 从零权重开始训练，返回的权重维度是否正确。
    """
    num_parties = 2
    dim = 5
    parties = [Party.remote(f"Party-{i}", num_samples=400, dim=dim) for i in range(num_parties)]
    aggregator = Aggregator.remote()

    final_weights = run_federated_training(
        parties=parties,
        aggregator=aggregator,
        num_rounds=2,
        learning_rate=0.1,
        dim=dim,
    )

    assert final_weights.shape == (dim,)
    assert isinstance(final_weights, np.ndarray)


def test_run_federated_training_initial_weights():
    """
    测试 run_federated_training 使用指定初始权重，输出与输入维度一致。
    """
    num_parties = 2
    dim = 3
    parties = [Party.remote(f"Party-init-{i}", num_samples=300, dim=dim) for i in range(num_parties)]
    aggregator = Aggregator.remote()
    initial_weights = np.array([0.5, -0.5, 0.0])

    final_weights = run_federated_training(
        parties=parties,
        aggregator=aggregator,
        num_rounds=1,
        learning_rate=0.2,
        dim=dim,
        initial_weights=initial_weights,
    )

    assert final_weights.shape == (dim,)
    # 至少保证权重被更新了（不等于初始权重）
    assert not np.allclose(final_weights, initial_weights)


def test_party_gradient_nonzero():
    """
    测试当全局权重为随机非零向量时，Party 计算出的梯度不为零。
    """
    party = Party.remote(party_id="Party-nonzero", num_samples=1000, dim=5)
    weights = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    result = ray.get(party.compute_local_gradient.remote(weights))

    grad_norm = np.linalg.norm(result["gradient"])
    assert grad_norm > 1e-6, "期望非零权重输入产生非零梯度"
