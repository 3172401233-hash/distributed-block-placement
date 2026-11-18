"""Data model: nodes, blocks, cluster topology and current placement.

Hierarchy is zone > rack > node. A rack is the default fault domain:
no two healthy replicas of the same block may live in the same rack.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Node:
    node_id: str
    rack: str
    zone: str
    capacity: int          # total capacity in bytes (any consistent unit)
    online: bool = True    # False marks a node (or its rack) as down


@dataclass
class Block:
    block_id: str
    size: int
    replication_factor: int = 3
    min_healthy: int = 2   # healthy replicas that must survive any migration step


@dataclass
class Cluster:
    nodes: dict[str, Node]
    blocks: dict[str, Block]
    placement: dict[str, list[str]] = field(default_factory=dict)  # block_id -> [node_id]

    def used_bytes(self) -> dict[str, int]:
        used = {nid: 0 for nid in self.nodes}
        for bid, nids in self.placement.items():
            size = self.blocks[bid].size
            for nid in nids:
                used[nid] += size
        return used

    def utilization(self) -> dict[str, float]:
        used = self.used_bytes()
        return {nid: used[nid] / self.nodes[nid].capacity for nid in self.nodes}
