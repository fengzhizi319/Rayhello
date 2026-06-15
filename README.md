# Ray 学习 Demo 工程

> 本工程面向希望学习 [Ray](https://www.ray.io/) 的开发者。Ray 是 SecretFlow 等隐私计算框架底层采用的分布式调度引擎，理解 Ray 对后续学习 SecretFlow 大有裨益。

## 一、工程结构

```
Rayhello/
├── README.md                 # 本文件：学习路线与运行说明
├── requirements.txt          # Python 依赖
├── 01_basic/                 # 基础篇：Task / Actor / Object Store
│   ├── 01_remote_task.py
│   ├── 02_remote_actor.py
│   └── 03_object_store.py
├── 02_advanced/              # 进阶篇：Placement Group / Ray Dataset
│   ├── 04_placement_group.py
│   └── 05_ray_dataset.py
└── 03_secretflow_like/       # 实战篇：模拟 SecretFlow 的联邦聚合
    └── 06_federated_aggregation.py
```

## 二、学习路线（推荐按编号顺序执行）

### 前置准备

1. 确保 Python >= 3.8（本机演示使用 `python3`）
2. 建议创建虚拟环境并安装依赖：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. 验证 Ray 可用：

```bash
python3 -c "import ray; print(ray.__version__)"
# 若已激活虚拟环境，可直接用 python
```

### Step 1：理解 Ray 的远程函数（Task）

**核心概念**：

- 用 `@ray.remote` 装饰普通函数，即可变成可在集群任意节点执行的 **Task**。
- `func.remote(...)` 不会直接返回结果，而是返回一个 **ObjectRef**（Future）。
- 需要结果时调用 `ray.get(ref)`。

**运行**：

```bash
# 已激活虚拟环境
python 01_basic/01_remote_task.py

# 未激活虚拟环境
python3 01_basic/01_remote_task.py
```

我来详细解释一下 Ray 中的 `remote` 方法及其工作原理：

#### Ray 的 `remote` 方法详解

1. **什么是 `.remote`**  
   `.remote` 是 Ray 提供的一个装饰器方法，用于将普通的 Python 函数或类方法标记为**远程可执行任务（Remote Task）**。当你在一个被 `@ray.remote` 装饰的函数或类上调用 `.remote()` 时，它并不会在当前进程中同步执行，而是：

   - 向 Ray 集群的任务调度器提交一个执行请求
   - 立即返回一个 `ObjectRef`（对象引用），而不是函数的实际返回值
   - 由 Ray 的调度系统决定在哪个可用的工作节点（Worker）上异步执行该任务

2. **每个 `.remote` 都会创建一个新任务吗？**  
   是的，每次调用 `.remote()` 都会创建一个新的独立任务（Task）。例如，在你提供的代码中：

   ```python
   for i in range(n_tasks):
       ref = slow_add.remote(i, i + 1)  # 每次循环都创建一个新任务
       refs.append(ref)
       print(f"  已提交 Task {i}, ObjectRef: {ref}")
   ```


   这里的 `slow_add.remote(i, i + 1)` 每次循环都会提交一个新的任务到 Ray 集群，总共会创建 `n_tasks` 个任务。

3. **在哪里执行？类似远程新开进程吗？**  
   - **不是简单的新开进程**：Ray 的执行模型基于**Worker 进程池**。Ray 启动时会在集群节点上预创建一组 Worker 进程（可以是本地或远程节点）。当任务被调度时，它会被分配给一个空闲的 Worker 进程执行，而不是每次都创建新的操作系统进程。
   - **分布式执行**：如果是在多节点集群上运行，任务可能在任何可用的节点上执行。Ray 的调度器会根据资源需求（如 CPU、GPU、内存）和数据位置等信息智能地分配任务。
   - **资源隔离**：虽然共享 Worker 进程池，但 Ray 通过其内部机制确保了任务间的资源管理和隔离。

4. **与普通函数调用的区别**  
   - **普通调用** (`func()`)：同步执行，阻塞当前线程直到函数返回结果。
   - **远程调用** (`func.remote()`)：异步执行，立即返回 `ObjectRef`，允许主程序继续执行其他逻辑，稍后通过 `ray.get(ObjectRef)` 获取结果。

5. **Actor 中的 `.remote`**  
   对于 Actor（通过 `@ray.remote` 装饰的类），`.remote` 的行为略有不同：
   - `ActorClass.remote()`：创建一个新的 Actor 实例（分配一个专用的 Worker 进程来维护其状态）。
   - `actor_instance.method.remote()`：调用该 Actor 实例上的方法，此方法将在该 Actor 专属的 Worker 进程上执行，并且对于同一个 Actor 实例，其方法调用是**顺序执行**的（保证状态一致性）。

总结来说，`.remote` 是 Ray 实现分布式、并行计算的核心机制，它将本地的函数调用转换为集群范围内的异步任务调度，利用 Worker 池而非频繁创建新进程来高效执行任务。
**学习目标**：

- 掌握 `ray.init()` 启动本地集群。
- 理解并行调用与 `ray.get()` 阻塞取结果的区别。
- 观察 Ray Dashboard（默认 `http://localhost:8265`）。

### Step 2：理解 Ray Actor

**核心概念**：

- Actor 是有状态的远程 Worker，适合维护模型、数据库连接、计数器等状态。
- 每个 Actor 在独立进程中运行，方法调用通过 `.remote()` 异步执行。

**运行**：

```bash
python 01_basic/02_remote_actor.py
```

**学习目标**：

- 学会定义、创建、调用 Actor。
- 理解 Actor 状态如何在多次调用间保持。
- 对比 Task（无状态）与 Actor（有状态）的使用场景。

### Step 3：理解对象存储（Object Store）

**核心概念**：

- Ray 通过 Plasma/In-Memory Object Store 在 Task / Actor 之间共享对象。
- `ray.put()` 把对象放入 Store 并返回 ObjectRef。
- `ray.get()` 从 Store 取出对象。

**运行**：

```bash
python 01_basic/03_object_store.py
```

**学习目标**：

- 学会显式使用 `ray.put()` 减少大对象传输。
- 理解 ObjectRef 的生命周期与 `ray.shutdown()`。

### Step 4：Placement Group（资源Placement）

**核心概念**：

- Placement Group 允许把多个 Actor / Task 调度到同一节点或分散到不同节点。
- SecretFlow 中常常利用 Placement Group 把不同参与方的计算资源做隔离或亲和性部署。

**运行**：

```bash
python 02_advanced/04_placement_group.py
```

**学习目标**：

- 掌握 `placement_group()` 创建资源组。
- 理解 `bundles`、`strategy`（PACK / SPREAD / STRICT_PACK / STRICT_SPREAD）。
- 学会把 Actor 绑定到指定 Placement Group。

### Step 5：Ray Dataset（数据并行）

**核心概念**：

- Ray Data 提供分布式数据加载与转换接口。
- 适合隐私计算里对大规模样本做预处理、分片、聚合。

**运行**：

```bash
python 02_advanced/05_ray_dataset.py
```

**学习目标**：

- 学会 `ray.data.from_items()`、`map_batches()`、`groupby()`。

- 理解 Dataset 的延迟执行（lazy execution）。


  

#### put跟data的核心区别

##### 1. **`ray.put()` - 对象存储操作**

- **作用**：将单个 Python 对象放入 Ray 的分布式 Object Store（对象存储）
- **返回**：`ObjectRef`（对象引用）
- **使用场景**：
  - 在 Task/Actor 之间共享数据
  - 避免大对象重复传输
  - 显式控制对象的序列化与分发

```python
   # 示例：把数组放入对象存储
   arr = np.arange(1_000_000)
   arr_ref = ray.put(arr)  # 返回 ObjectRef
   result = process_with_data.remote(arr_ref, 2.0)  # 传递给远程任务
```


##### 2. **`ray.data` - 分布式数据处理框架**

- **作用**：提供类似 Pandas/Spark 的分布式数据集处理能力
- **返回**：`Dataset` 对象（支持惰性执行）
- **使用场景**：
  - 大规模数据的加载、转换、聚合
  - 批量数据处理（map/filter/groupby/join）
  - 流式处理超大数据集

```python
# 示例：创建分布式数据集
items = [{"party": "A", "score": i} for i in range(100)]
ds = ray.data.from_items(items)  # 返回 Dataset
transformed = ds.map_batches(add_features).filter(lambda x: x["score"] >= 80)
transformed.show(5)  # 触发执行
```


##### 对比总结

| 特性         | `ray.put()`           | `ray.data`                     |
| ------------ | --------------------- | ------------------------------ |
| **数据类型** | 任意 Python 对象      | 结构化数据集（表格/记录）      |
| **返回值**   | `ObjectRef`           | `Dataset` 对象                 |
| **执行方式** | 立即执行              | 惰性执行（Lazy Execution）     |
| **主要用途** | 对象共享与传递        | 分布式数据处理流水线           |
| **底层机制** | 序列化到 Object Store | 分片存储 + 并行计算            |
| **适用规模** | 小到中等对象          | 大规模数据集（GB/TB级）        |
| **典型操作** | `ray.get(ref)` 取回   | `map/filter/groupby/aggregate` |

##### 实际关系

它们可以配合使用：

```python
# Dataset 的结果可以通过 ray.put() 共享给其他任务
ds = ray.data.from_items([...])
result = ds.take(100)  # 获取部分结果
result_ref = ray.put(result)  # 放入对象存储供其他任务使用
```


**简单理解**：

- `ray.put()` 是"把东西放进仓库"（底层存储机制）
- `ray.data` 是"对大量数据进行工厂流水线加工"（高层数据处理框架）
  让我搜索一下 Ray Dataset 支持的数据导入方法。




**已检索网络** *Ray Dataset data source import methods API* 10 个结果


根据搜索结果和 Ray Dataset 的官方 API，除了 `from_items()`，Ray Dataset 还支持多种数据导入方法。让我为你整理一下：

#### Ray Dataset 支持的数据导入方法

##### 1. **从文件读取**

```python
# CSV 文件
ds = ray.data.read_csv("path/to/data.csv")
ds = ray.data.read_csv("s3://bucket/data/*.csv")  # 支持通配符

# Parquet 文件（推荐用于大数据）
ds = ray.data.read_parquet("path/to/data.parquet")

# JSON 文件
ds = ray.data.read_json("path/to/data.json")

# 文本文件
ds = ray.data.read_text("path/to/data.txt")
```


##### 2. **从数据库/数据仓库读取**

```python
# Spark DataFrame
ds = ray.data.from_spark(spark_df)

# Databricks Unity Catalog
ds = ray.data.read_databricks_table("my_db.my_table")
```


##### 3. **从内存创建**

```python
# Python 列表/字典（你当前使用的）
ds = ray.data.from_items([{"col1": val1, "col2": val2}, ...])

# Pandas DataFrame
import pandas as pd
df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
ds = ray.data.from_pandas(df)

# Numpy 数组
import numpy as np
arr = np.array([[1, 2], [3, 4], [5, 6]])
ds = ray.data.from_numpy(arr)

# Arrow Table
import pyarrow as pa
table = pa.table({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
ds = ray.data.from_arrow(table)
```


##### 4. **从云存储读取**

```python
# S3
ds = ray.data.read_csv("s3://my-bucket/data/")
ds = ray.data.read_parquet("s3://anonymous@air-example-data/breast_cancer.csv")

# HDFS
ds = ray.data.read_csv("hdfs://namenode:8020/path/to/data/*.csv")

# Azure Blob Storage
ds = ray.data.read_csv("az://container/path/to/data.csv")

# Google Cloud Storage
ds = ray.data.read_csv("gs://bucket/path/to/data.csv")
```


##### 5. **特殊格式**

```python
# 图像数据
ds = ray.data.read_images("path/to/images/")

# 二进制数据
ds = ray.data.read_binary_files("path/to/files/")

# TFRecords (TensorFlow)
ds = ray.data.read_tfrecords("path/to/data.tfrecord")

# WebDataset
ds = ray.data.read_webdataset("path/to/webdataset.tar")
```


##### 6. **编程式创建**

```python
# 从生成器函数
def generate_data():
    for i in range(1000):
        yield {"id": i, "value": i * 2}

ds = ray.data.from_generator(generate_data)

# 从 Range（类似 range()）
ds = ray.data.range(1000)  # 生成 0-999 的整数
ds = ray.data.range_tensor(1000, shape=(10,))  # 生成张量
```


##### 完整示例对比

```python
import ray
import pandas as pd
import numpy as np

ray.init()

# 方法1: from_items（适合小规模测试数据）
ds1 = ray.data.from_items([{"name": f"user_{i}", "score": i} for i in range(100)])

# 方法2: read_csv（适合结构化数据文件）
ds2 = ray.data.read_csv("data/users.csv")

# 方法3: read_parquet（推荐，性能更好）
ds3 = ray.data.read_parquet("data/users.parquet")

# 方法4: from_pandas（从已有 DataFrame 转换）
df = pd.read_csv("data/users.csv")
ds4 = ray.data.from_pandas(df)

# 方法5: range（快速生成测试数据）
ds5 = ray.data.range(1000)

print(f"DS1 count: {ds1.count()}")
print(f"DS2 count: {ds2.count()}")
print(f"DS3 count: {ds3.count()}")

ray.shutdown()
```


#### 选择建议

| 场景                  | 推荐方法                     |
| --------------------- | ---------------------------- |
| 小规模测试数据        | `from_items()`               |
| CSV/JSON 文本文件     | `read_csv()` / `read_json()` |
| 大数据集（GB/TB级）   | `read_parquet()` ⭐           |
| 已有 Pandas DataFrame | `from_pandas()`              |
| 分布式文件系统        | `read_parquet("hdfs://...")` |
| 云存储数据            | `read_parquet("s3://...")`   |
| 快速生成序列数据      | `range()`                    |
| 自定义数据生成逻辑    | `from_generator()`           |

**最佳实践**：对于生产环境的大数据处理，优先使用 **Parquet 格式** + `read_parquet()`，因为 Parquet 是列式存储，压缩率高、读取速度快，且支持谓词下推等优化。

### Step 6：SecretFlow 风格实战 —— 联邦聚合

**核心概念**：

- 模拟 SecretFlow 中的多方安全计算场景：多个 Party 各自持有本地数据，通过 Ray Actor 模拟，每个 Party 本地计算梯度/统计量，然后聚合到协调方（Aggregator）。
- 这里为了学习目的，使用明文求和聚合，重点展示 Ray 的多 Actor 协作模式。

**运行**：

```bash
python 03_secretflow_like/06_federated_aggregation.py
```

**学习目标**：

- 综合运用 Actor + Object Store + Placement Group。
- 理解 Ray 如何作为 SecretFlow 的分布式执行引擎。
- 体会隐私计算场景下多参与方计算的调度模式。

#### 多节点 Ray 集群下的联邦学习

上面的命令在**本地单进程**中模拟了 3 个 Party。在真实联邦学习场景下，每个参与方通常位于不同的物理机器上，各自启动一个 Ray 进程，然后组成一个真正的 Ray 集群：

**1. 选择一台机器作为 Head Node（协调方所在机器）**

```bash
ray start --head --port=6379 --dashboard-host=0.0.0.0
```

启动成功后会显示类似 `Ray runtime started.` 以及 head node 的 IP 和端口。

**2. 其他参与方机器作为 Worker Node 加入集群**

在每台 Party 机器上执行：

```bash
ray start --address="<head_node_ip>:6379"
```

例如 head node IP 为 `192.168.1.10`：

```bash
ray start --address="192.168.1.10:6379"
```

**3. 在任一节点上运行联邦聚合脚本**

```bash
python 03_secretflow_like/06_federated_aggregation.py --ray-address=ray://192.168.1.10:10001
```

或者通过环境变量指定：

```bash
RAY_ADDRESS=ray://192.168.1.10:10001 python 03_secretflow_like/06_federated_aggregation.py
```

此时：

- `Party Actor` 会被 Placement Group 的 `SPREAD` 策略尽量分散到不同节点。
- `Aggregator Actor` 通常也运行在 Head Node 或指定节点上。
- 各方原始数据保留在本地，只有梯度/统计量通过网络传输到协调方。

**停止集群**：

```bash
# 在每台 Worker Node 上执行
ray stop

# 在 Head Node 上执行
ray stop
```

**真实场景 vs 本示例**：

| 对比项 | 本示例 | 真实联邦学习（如 SecretFlow） |
| ------ | ------ | ----------------------------- |
| 数据位置 | 同进程内存 | 各 Party 本地磁盘/数据库 |
| 通信内容 | 明文梯度 | 秘密分享/同态加密后的密文 |
| 聚合位置 | 普通 Aggregator Actor | SPU（安全多方计算单元） |
| 网络安全 | 无 | TLS、认证、安全信道 |
| 调度目标 | 学习 Ray API | 隐私保护下的分布式计算 |

## 三、常用调试命令

```bash
# 查看 Ray 版本
python -c "import ray; print(ray.__version__)"

# 启动本地集群后查看 Dashboard
# 默认地址 http://localhost:8265

# 查看当前集群资源
python -c "import ray; ray.init(); print(ray.cluster_resources()); ray.shutdown()"

# 运行某个示例
python 01_basic/01_remote_task.py

# 一键运行全部示例
python run_all.py

# 运行单元测试
pytest tests/ -v

# 运行指定测试文件
pytest tests/test_federated_aggregation.py -v
```

## 四、学习建议

1. **先单文件运行，再看代码注释**：每个 `.py` 文件开头都有“本节目标”和“关键 API”说明。
2. **打开 Ray Dashboard**：运行示例时访问 `http://localhost:8265`，观察 Job、Actors、Tasks 的调度情况。
3. **修改参数重跑**：例如把 `num_workers` 改大，观察并行度提升效果。
4. **阅读 Ray 官方文档**：
   - [Ray Core 官方文档](https://docs.ray.io/en/latest/ray-core/walkthrough.html)
   - [SecretFlow 架构](https://www.secretflow.org.cn/docs/secretflow/)

## 五、FAQ

**Q1：运行时报端口被占用？**  
A：Ray 默认使用 6379、8265 等端口，可显式指定：`ray.init(dashboard_port=8266)`。

**Q2：Windows 可以运行吗？**  
A：Ray 对 Windows 支持有限，推荐使用 WSL2 / Linux / macOS。

**Q3：需要 GPU 吗？**  
A：本 demo 全部在 CPU 上即可运行，无需 GPU。

**Q4：多节点集群连接不上怎么办？**  
A：请检查以下几点：
- Head Node 的 `6379` 端口和 `10001` 端口是否开放（防火墙 / 安全组）。
- Worker Node 执行 `ray start --address="<head_ip>:6379"` 时是否返回 `Ray runtime started.`。
- 运行脚本时使用的地址是否为 `ray://<head_ip>:10001`（Ray Client 默认端口是 10001）。
- 各节点 Ray 版本是否一致：`python -c "import ray; print(ray.__version__)"`。

**Q5：联邦学习中各方必须运行完全相同的代码吗？**  
A：不一定。典型做法是：
- 各方运行同一套 Ray 运行时（版本一致即可）。
- Party 方的计算逻辑（如 `Party` Actor）可以相同，也可以根据各自数据特点定制。
- 协调方只需要知道统一的梯度/统计量格式，就能执行聚合。
- SecretFlow 等框架会进一步封装通信协议和安全计算细节，各方只需调用高层 API。

---

祝你学习愉快！如果在某个示例卡住了，建议回到上一个编号重新阅读注释。