# distributed-block-placement

Python 分布式块放置规划器：输入块副本、节点容量、机架/可用区层级和当前
placement，生成满足故障域与容量约束、迁移数量尽量少的多步重平衡计划。

## 故障域模型

集群拓扑为三级层级：**zone（可用区）> rack（机架）> node（节点）**。

- **rack 是默认故障域**：同一机架共享供电/交换设备，可能整体失效。
  因此同一个块的任意两个健康副本**不得落在同一 rack**（硬性约束）。
- **zone 是更高一级的故障域**：规划器在满足 rack 约束的候选节点中，
  优先选择该块尚未使用过的 zone（软性偏好，zone 不足时降级）。
- **node 故障视为其所在 rack 内的单点失效**；整个 rack 下线时，
  该 rack 内所有节点的副本一并标记为不健康。

## 约束与迁移安全

- **副本数**：每个块的健康副本数（位于在线节点上的副本）必须等于
  `replication_factor`；不足时补副本，超出时删除多余副本。
- **容量**：`节点已用 + 块大小 <= 节点容量` 是硬约束，恰好放满允许。
- **迁移安全**：每次逻辑迁移都拆成 `copy`（先建目标副本）+ `delete`
  （后删源副本）两个有序步骤，保证迁移过程中每个块的健康副本数
  始终不低于 `min_healthy`。
- **最小迁移**：先修复硬约束（副本不足 → 故障域冲突 → 容量超限），
  之后仅当节点利用率差超过 `balance_threshold`（默认 0.15）才做
  均衡迁移，且单次迁移不得反转源/目标的负载关系。

## 计划格式

`RebalancePlanner.build_plan(cluster)` 返回 `Plan`：

- `steps: list[Step]`，每个 `Step` 含 `action`（`copy`/`delete`）、
  `block_id`、`source`（数据来​​源/被清理节点）、`target`（新副本节点）
  与 `reason`（如 `node-offline`、`fault-domain`、`capacity`、
  `rebalance`、`under-replicated`）。
- `warnings`：无法调度时（容量不足、机架不够等）记录原因，不静默失败。

`validate_plan(cluster, plan)` 逐步模拟计划，校验迁移中健康副本数、
容量、故障域与最终副本数，返回违规列表（空列表表示计划安全）。

## 用法

```python
from placement import Block, Cluster, Node, RebalancePlanner, validate_plan

nodes = {
    "n1": Node("n1", rack="r1", zone="z1", capacity=100),
    "n2": Node("n2", rack="r2", zone="z1", capacity=100),
    "n3": Node("n3", rack="r3", zone="z2", capacity=100),
}
blocks = {"b1": Block("b1", size=10, replication_factor=3, min_healthy=2)}
cluster = Cluster(nodes, blocks, placement={"b1": ["n1", "n1", "n2"]})

plan = RebalancePlanner().build_plan(cluster)
for step in plan.steps:
    print(step)
assert validate_plan(cluster, plan) == []
```

## 测试

```bash
python -m unittest
```

覆盖场景：单节点下线、整机架故障、节点加入、副本不足、容量边界
（超容量节点疏散、恰好放满、无处可放）、同机架三副本修复的最小迁移数，
以及迁移全程的健康副本校验。
