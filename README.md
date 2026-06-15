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

### Ray 的 `remote` 方法详解

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

---

祝你学习愉快！如果在某个示例卡住了，建议回到上一个编号重新阅读注释。
