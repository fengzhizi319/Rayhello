"""
一键运行所有 Ray Demo 示例。

执行方式：
    python run_all.py

脚本会按编号依次运行 01~06 的示例。
每个示例运行前会打印分隔线，方便学习时观察输出。
"""

import subprocess
import sys
from pathlib import Path


SCRIPTS = [
    "01_basic/01_remote_task.py",
    "01_basic/02_remote_actor.py",
    "01_basic/03_object_store.py",
    "02_advanced/04_placement_group.py",
    "02_advanced/05_ray_dataset.py",
    "03_secretflow_like/06_federated_aggregation.py",
]


def run_script(script_path: str) -> bool:
    """运行单个 Python 脚本，返回是否成功。"""
    full_path = Path(__file__).parent / script_path
    print("\n" + "=" * 60)
    print(f"正在运行：{script_path}")
    print("=" * 60)
    try:
        result = subprocess.run(
            [sys.executable, str(full_path)],
            check=True,
            text=True,
        )
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"运行失败：{script_path}，返回码：{e.returncode}")
        return False


def main():
    print("Ray 学习 Demo 一键运行脚本")
    print(f"Python 解释器：{sys.executable}")

    failed = []
    for script in SCRIPTS:
        if not run_script(script):
            failed.append(script)

    print("\n" + "=" * 60)
    print("运行总结")
    print("=" * 60)
    print(f"总计：{len(SCRIPTS)}，成功：{len(SCRIPTS) - len(failed)}，失败：{len(failed)}")
    if failed:
        print("失败脚本：")
        for s in failed:
            print(f"  - {s}")
        sys.exit(1)
    else:
        print("所有示例运行成功！")


if __name__ == "__main__":
    main()
