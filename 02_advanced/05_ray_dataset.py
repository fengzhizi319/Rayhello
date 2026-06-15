"""
Ray 进阶示例 05：Ray Dataset（分布式数据）

本节目标：
    1. 学会用 Ray Data 加载、转换、聚合分布式数据。
    2. 理解 Dataset 的 lazy execution：转换操作不会立即执行。
    3. 掌握在隐私计算场景中对数据做 map/filter/groupby 预处理。

关键 API：
    - ray.data.from_items(items): 从内存创建 Dataset。
    - ds.map_batches(fn): 对 batch 数据做转换。
    - ds.groupby(key).sum()/count(): 分组聚合。
    - ds.show(limit): 触发执行并打印前 N 条。
    - ds.take(n): 触发执行并返回前 n 条数据。

注意：
    - Ray Data 默认 batch 处理，转换函数接收 dict of numpy arrays 或 pandas DataFrame。
"""

import ray
import numpy as np


def add_features(batch: dict) -> dict:
    """
    对 batch 数据做特征增强。
    batch 是一个 dict，key 是列名，value 是 numpy array。
    """
    scores = batch["score"]
    batch["score_squared"] = scores ** 2
    batch["passed"] = (scores >= 60).astype(np.int8)
    return batch


def double_score(batch: dict) -> dict:
    batch["score"] = batch["score"] * 2
    return batch


def main():
    context = ray.init(ignore_reinit_error=True)
    print("Ray 已启动，Dashboard 地址：", context.dashboard_url)
    
    # 输出集群资源信息
    print('\n=== 集群资源信息 ===')
    print('Dashboard URL:', context.dashboard_url)
    print('Cluster Resources:', ray.cluster_resources())
    print('Available Resources:', ray.available_resources())
    
    # -------------------------------------------------------
    # 重要概念：Ray Dataset 详解
    # -------------------------------------------------------
    print('\n=== Ray Dataset 重要概念说明 ===')
    print('1. Ray Dataset 是 Ray 的分布式数据处理库')
    print('2. 支持惰性执行（Lazy Execution），转换操作不会立即执行')
    print('3. 支持多种数据格式：Pandas DataFrame、PyArrow Table、Numpy Array 等')
    print('4. 自动并行处理，充分利用集群资源')
    print('5. 支持 map、filter、groupby、join 等常见数据操作')

    # -------------------------------------------------------
    # 1. 创建 Dataset
    # -------------------------------------------------------
    print("\n=== 创建 Dataset ===")
    items = [
        {"party": "A", "name": f"user_{i}", "score": 50 + i % 50}
        for i in range(100)
    ] + [
        {"party": "B", "name": f"user_{i}", "score": 30 + i % 70}
        for i in range(100)
    ]
    ds = ray.data.from_items(items)
    print(f"Dataset Schema: {ds.schema()}")
    print(f"总行数：{ds.count()}")

    # -------------------------------------------------------
    # 2. Lazy Transformation
    # -------------------------------------------------------
    print("\n=== 转换操作（Lazy） ===")
    transformed = (
        ds
        .map_batches(double_score, batch_format="numpy")
        .map_batches(add_features, batch_format="numpy")
        .filter(lambda row: row["score"] >= 80)
    )
    print("转换已定义，但尚未执行。下面调用 take/show 触发执行。")

    # -------------------------------------------------------
    # 3. 触发执行并查看结果
    # -------------------------------------------------------
    print("\n=== 触发执行：查看前 5 条 ===")
    transformed.show(5)

    # -------------------------------------------------------
    # 4. 分组聚合：按 party 统计平均分
    # -------------------------------------------------------
    print("\n=== 按 party 聚合 ===")
    agg = (
        ds
        .map_batches(add_features, batch_format="numpy")
        .groupby("party")
        .mean("score")
    )
    agg.show()

    # -------------------------------------------------------
    # 5. 把结果转为本地对象
    # -------------------------------------------------------
    print("\n=== 取回本地 Top 5 ===")
    top5 = transformed.take(5)
    for row in top5:
        print(row)
    
    # -------------------------------------------------------
    # 6. 补充 Ray Dataset 测试
    # -------------------------------------------------------
    print("\n=== 补充 Ray Dataset 测试 ===")
    
    # 6.1 测试 map_batches 函数的不同批处理格式
    print("\n--- 测试 Pandas 格式的批处理 ---")
    def add_features_pandas(df):
        """
        使用 Pandas DataFrame 格式进行批处理
        """
        df['score_squared'] = df['score'] ** 2
        df['passed'] = df['score'] >= 60
        return df
    
    pandas_transformed = ds.map_batches(
        add_features_pandas,
        batch_format="pandas"  # 使用 Pandas DataFrame 格式
    )
    print("Pandas 格式转换完成，查看前3行：")
    pandas_transformed.show(3)
    
    # 6.2 测试更复杂的数据聚合
    print("\n--- 测试复杂聚合操作 ---")
    complex_agg = (
        ds
        .map_batches(add_features, batch_format="numpy")
        .groupby("party")
        .aggregate(
            mean_score=("score", "mean"),
            max_score=("score", "max"),
            min_score=("score", "min"),
            count=("score", "count")
        )
    )
    print("复杂聚合结果：")
    complex_agg.show()
    
    # 6.3 测试数据排序
    print("\n--- 测试数据排序 ---")
    sorted_ds = ds.sort(key="score", descending=True)
    print("按分数降序排列前5条：")
    sorted_ds.show(5)
    
    # 6.4 测试数据采样
    print("\n--- 测试数据采样 ---")
    sampled_ds = ds.random_sample(0.1)  # 随机采样10%的数据
    print(f"原始数据集大小：{ds.count()}，采样后大小：{sampled_ds.count()}")
    
    # 6.5 测试 distinct 操作
    print("\n--- 测试去重操作 ---")
    distinct_parties = ds.select_columns(["party"]).distinct()
    print("唯一 party 值：")
    distinct_parties.show()
    
    ray.shutdown()
    print("\nRay 已关闭")


if __name__ == "__main__":
    main()
