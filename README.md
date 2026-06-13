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
