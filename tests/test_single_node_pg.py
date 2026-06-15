"""
测试：单节点集群中 Placement Group 的行为。

验证在只有一个节点的 Ray 集群中，SPREAD 策略的 Placement Group
仍然可以创建多个 bundle，并将 Actor 调度到该节点上（资源隔离依然有效）。
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
class _Worker:
    """用于测试的临时 Worker Actor。"""

    def get_node_info(self):
        ctx = ray.get_runtime_context()
        return {
            "hostname": socket.gethostname(),
            "node_id": ctx.get_node_id(),
            "worker_id": str(ctx.get_worker_id()),
        }


def test_single_node_pg_spread():
    """
    测试单节点下 SPREAD Placement Group 的行为。

    期望：所有 bundle 和 Actor 都在同一个节点上，但资源预留仍然生效。
    """
    num_bundles = 4
    bundles = [{"CPU": 0.5} for _ in range(num_bundles)]
    pg = placement_group(bundles, strategy="SPREAD", name="test_single_node_pg")
    ray.get(pg.ready())

    try:
        assert len(pg.bundle_specs) == num_bundles
        for spec in pg.bundle_specs:
            assert spec == {"CPU": 0.5}

        workers = [_Worker.options(placement_group=pg).remote() for _ in range(num_bundles)]
        node_infos = ray.get([w.get_node_info.remote() for w in workers])

        unique_nodes = {info["node_id"] for info in node_infos}
        assert len(unique_nodes) == 1, "单节点集群中所有 Actor 应在同一节点上"

        # 验证资源被预留：初始 4 CPU，预留 4 * 0.5 = 2 CPU，应剩余 2 CPU
        available_cpu = ray.available_resources().get("CPU", 0)
        assert available_cpu <= 2.0, f"预留后可用 CPU 应不超过 2.0，实际为 {available_cpu}"
    finally:
        ray.util.remove_placement_group(pg)
