"""
测试：验证 Actor 与 Bundle 的绑定关系。

验证通过 Placement Group 创建的 Actor 能够被正确绑定到 bundle，
并能在运行时获取其所在的节点信息。在单节点集群中所有 Actor 都在同一节点，
但每个 Actor 仍占用独立的 bundle 资源。
"""

import socket

import pytest
import ray
from ray.util.placement_group import placement_group


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    """为整个测试模块启动和关闭本地 Ray 集群。"""
    context = ray.init(ignore_reinit_error=True, num_cpus=4)
    yield context
    ray.shutdown()


@ray.remote(num_cpus=0.5)
class _BindingWorker:
    """用于测试 Actor-Bundle 绑定的临时 Worker Actor。"""

    def __init__(self, worker_id):
        self.worker_id = worker_id
        ctx = ray.get_runtime_context()
        self.node_id = ctx.get_node_id()
        self.hostname = socket.gethostname()

    def get_info(self):
        return {
            "worker_id": self.worker_id,
            "node_id": self.node_id,
            "hostname": self.hostname,
        }


def test_actor_bundle_binding():
    """
    测试 Actor 绑定到 Placement Group Bundle 后的资源隔离与位置信息。
    """
    num_bundles = 4
    bundles = [{"CPU": 0.5} for _ in range(num_bundles)]
    pg = placement_group(bundles, strategy="SPREAD", name="test_binding_pg")
    ray.get(pg.ready())

    try:
        assert len(pg.bundle_specs) == num_bundles

        workers = [
            _BindingWorker.options(placement_group=pg).remote(worker_id=i)
            for i in range(num_bundles)
        ]
        infos = ray.get([w.get_info.remote() for w in workers])

        for info in infos:
            assert "worker_id" in info
            assert "node_id" in info
            assert "hostname" in info

        unique_nodes = {info["node_id"] for info in infos}
        # 单节点场景下所有 Actor 在同一节点
        assert len(unique_nodes) == 1

        # 验证资源预留生效
        available_cpu = ray.available_resources().get("CPU", 0)
        assert available_cpu <= 2.0, f"预留后可用 CPU 应不超过 2.0，实际为 {available_cpu}"
    finally:
        ray.util.remove_placement_group(pg)
