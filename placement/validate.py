"""Plan validation: simulate a plan step by step and check every invariant.

Used by the tests, but also useful for callers that want to verify a plan
before shipping it to the storage layer.
"""
from __future__ import annotations

from .model import Cluster
from .planner import COPY, DELETE, Plan


def validate_plan(cluster: Cluster, plan: Plan) -> list[str]:
    """Return a list of invariant violations (empty means the plan is safe)."""
    errors: list[str] = []
    nodes = cluster.nodes
    placement = {bid: list(nids) for bid, nids in cluster.placement.items()}
    for bid in cluster.blocks:
        placement.setdefault(bid, [])
    used = cluster.used_bytes()

    def check_health(step_idx: int) -> None:
        for bid, block in cluster.blocks.items():
            healthy = sum(1 for n in placement[bid] if nodes[n].online)
            if healthy < block.min_healthy:
                errors.append(
                    f"after step {step_idx}: block {bid} has {healthy} healthy "
                    f"replicas (< min_healthy={block.min_healthy})")

    for i, step in enumerate(plan.steps):
        block = cluster.blocks[step.block_id]
        if step.action == COPY:
            if step.target is None or step.target not in nodes:
                errors.append(f"step {i}: copy of {step.block_id} has invalid target")
                continue
            if not nodes[step.target].online:
                errors.append(f"step {i}: copy of {step.block_id} targets offline "
                              f"node {step.target}")
            if step.target in placement[step.block_id]:
                errors.append(f"step {i}: duplicate replica of {step.block_id} "
                              f"on {step.target}")
            placement[step.block_id].append(step.target)
            used[step.target] += block.size
            if used[step.target] > nodes[step.target].capacity:
                errors.append(f"step {i}: node {step.target} exceeds capacity")
        elif step.action == DELETE:
            if step.source not in placement[step.block_id]:
                errors.append(f"step {i}: delete of {step.block_id} on unknown "
                              f"node {step.source}")
                continue
            placement[step.block_id].remove(step.source)
            used[step.source] -= block.size
        else:
            errors.append(f"step {i}: unknown action {step.action!r}")
        check_health(i)

    for bid, block in cluster.blocks.items():
        replicas = placement[bid]
        healthy = [n for n in replicas if nodes[n].online]
        if len(replicas) != len(set(replicas)):
            errors.append(f"block {bid}: duplicate replicas on the same node")
        leftover = [n for n in replicas if not nodes[n].online]
        if leftover:
            errors.append(f"block {bid}: replicas left on offline nodes {leftover}")
        if len(healthy) != block.replication_factor:
            errors.append(f"block {bid}: {len(healthy)} healthy replicas, "
                          f"want {block.replication_factor}")
        racks = [nodes[n].rack for n in healthy]
        if len(set(racks)) != len(racks):
            errors.append(f"block {bid}: fault-domain violation, racks={racks}")

    for node in nodes.values():
        if node.online and used[node.node_id] > node.capacity:
            errors.append(f"node {node.node_id}: {used[node.node_id]} bytes used, "
                          f"capacity {node.capacity}")
    return errors
