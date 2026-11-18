"""Rebalance planner.

Generates an ordered, multi-step migration plan that fixes (in order):

1. under-replicated blocks and replicas on offline nodes,
2. fault-domain violations (several replicas in one rack),
3. capacity violations (nodes over capacity),
4. load imbalance (only when it exceeds ``balance_threshold``).

Every logical migration is emitted as a COPY step followed by a DELETE
step, so a block never drops below ``min_healthy`` replicas mid-plan.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from .model import Block, Cluster, Node

COPY = "copy"
DELETE = "delete"


@dataclass
class Step:
    action: str                # COPY or DELETE
    block_id: str
    source: str | None         # COPY: replica serving the data; DELETE: node cleaned up
    target: str | None         # COPY: node receiving the replica; DELETE: None
    reason: str

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        if self.action == COPY:
            return f"COPY {self.block_id}: {self.source} -> {self.target} ({self.reason})"
        return f"DELETE {self.block_id} on {self.source} ({self.reason})"


@dataclass
class Plan:
    steps: list[Step] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def copy_steps(self) -> list[Step]:
        return [s for s in self.steps if s.action == COPY]

    @property
    def delete_steps(self) -> list[Step]:
        return [s for s in self.steps if s.action == DELETE]

    def __len__(self) -> int:
        return len(self.steps)


class RebalancePlanner:
    def __init__(self, balance_threshold: float = 0.15, max_balance_rounds: int | None = None):
        self.balance_threshold = balance_threshold
        self.max_balance_rounds = max_balance_rounds

    def build_plan(self, cluster: Cluster) -> Plan:
        self.cluster = cluster
        self.plan = Plan()
        # Working copy of the placement, mutated as steps are emitted.
        self.placement = {bid: list(nids) for bid, nids in cluster.placement.items()}
        for bid in cluster.blocks:
            self.placement.setdefault(bid, [])
        self.used = cluster.used_bytes()

        self._fix_under_replicated()
        self._fix_fault_domain()
        self._fix_capacity()
        self._rebalance()
        return self.plan

    # ------------------------------------------------------------------ phases

    def _fix_under_replicated(self) -> None:
        for bid, block in self.cluster.blocks.items():
            replicas = list(self.placement[bid])
            offline = [n for n in replicas if not self.cluster.nodes[n].online]
            healthy = [n for n in replicas if self.cluster.nodes[n].online]

            # Drop offline replicas from the working placement now so target
            # selection sees the real fault-domain picture; the DELETE steps
            # are appended only after the replacement COPYs for safety.
            for nid in offline:
                self.placement[bid].remove(nid)
                self.used[nid] -= block.size

            missing = block.replication_factor - len(healthy)
            for _ in range(max(0, missing)):
                reason = "node-offline: replace replica lost with failed node" if offline \
                    else "under-replicated: restore replication factor"
                target = self._choose_target(block)
                if target is None:
                    target = self._choose_target(block, allow_rack_reuse=True)
                    if target is not None:
                        self.plan.warnings.append(
                            f"{bid}: not enough racks; replica shares rack {target.rack!r}")
                if target is None:
                    self.plan.warnings.append(f"{bid}: no node with enough free capacity")
                    break
                self._emit_copy(bid, target.node_id, reason)

            for nid in offline:
                self.plan.steps.append(
                    Step(DELETE, bid, nid, None, "node-offline: drop replica on failed node"))

            while len(self._healthy(bid)) > block.replication_factor:
                victim = max(self._healthy(bid),
                             key=lambda n: self.used[n] / self.cluster.nodes[n].capacity)
                self._emit_delete(bid, victim, "over-replicated: above replication factor")

    def _fix_fault_domain(self) -> None:
        for bid in self.cluster.blocks:
            while True:
                by_rack: dict[str, list[str]] = defaultdict(list)
                for nid in self.placement[bid]:
                    by_rack[self.cluster.nodes[nid].rack].append(nid)
                crowded = [r for r, members in by_rack.items() if len(members) > 1]
                if not crowded:
                    break
                rack = max(crowded, key=lambda r: len(by_rack[r]))
                members = by_rack[rack]
                src = max(members, key=lambda n: self.used[n] / self.cluster.nodes[n].capacity)
                target = self._choose_target(self.cluster.blocks[bid])
                if target is None:
                    self.plan.warnings.append(
                        f"{bid}: cannot spread replicas out of rack {rack!r}")
                    break
                self._emit_move(bid, src, target.node_id,
                                f"fault-domain: rack {rack!r} holds multiple replicas")

    def _fix_capacity(self) -> None:
        def overage(node: Node) -> int:
            return self.used[node.node_id] - node.capacity

        overloaded = [n for n in self.cluster.nodes.values() if n.online and overage(n) > 0]
        for node in sorted(overloaded, key=overage, reverse=True):
            while overage(node) > 0:
                candidates = sorted(self.placement_on(node.node_id),
                                    key=lambda b: -self.cluster.blocks[b].size)
                moved = False
                for bid in candidates:
                    block = self.cluster.blocks[bid]
                    target = self._choose_target(block)
                    if target is None:
                        continue
                    self._emit_move(bid, node.node_id, target.node_id,
                                    f"capacity: node {node.node_id!r} over capacity")
                    moved = True
                    break
                if not moved:
                    self.plan.warnings.append(
                        f"node {node.node_id!r}: still over capacity, no valid target")
                    break

    def _rebalance(self) -> None:
        limit = self.max_balance_rounds or (4 * len(self.cluster.blocks) + 16)
        for _ in range(limit):
            if not self._balance_once():
                break

    # ------------------------------------------------------------------ helpers

    def placement_on(self, node_id: str) -> list[str]:
        return [bid for bid, nids in self.placement.items() if node_id in nids]

    def _healthy(self, bid: str) -> list[str]:
        return [n for n in self.placement[bid] if self.cluster.nodes[n].online]

    def _choose_target(self, block: Block, allow_rack_reuse: bool = False) -> Node | None:
        placed = self.placement[block.block_id]
        used_racks = {self.cluster.nodes[n].rack for n in placed}
        used_zones = {self.cluster.nodes[n].zone for n in placed}
        best: Node | None = None
        best_key = None
        for node in self.cluster.nodes.values():
            if not node.online or node.node_id in placed:
                continue
            if self.used[node.node_id] + block.size > node.capacity:
                continue
            rack_penalty = 1 if node.rack in used_racks else 0
            if rack_penalty and not allow_rack_reuse:
                continue
            zone_penalty = 1 if node.zone in used_zones else 0
            util = (self.used[node.node_id] + block.size) / node.capacity
            key = (rack_penalty, zone_penalty, util)
            if best_key is None or key < best_key:
                best, best_key = node, key
        return best

    def _emit_copy(self, bid: str, target: str, reason: str, source: str | None = None) -> None:
        block = self.cluster.blocks[bid]
        if source is None:
            healthy = self._healthy(bid)
            source = healthy[0] if healthy else None
        self.plan.steps.append(Step(COPY, bid, source, target, reason))
        self.placement[bid].append(target)
        self.used[target] += block.size

    def _emit_delete(self, bid: str, node_id: str, reason: str) -> None:
        block = self.cluster.blocks[bid]
        self.plan.steps.append(Step(DELETE, bid, node_id, None, reason))
        self.placement[bid].remove(node_id)
        self.used[node_id] -= block.size

    def _emit_move(self, bid: str, src: str, dst: str, reason: str) -> None:
        # COPY first, DELETE second: healthy count never dips mid-migration.
        source = src if self.cluster.nodes[src].online else None
        self._emit_copy(bid, dst, reason, source=source)
        self._emit_delete(bid, src, reason)

    def _balance_once(self) -> bool:
        online = [n for n in self.cluster.nodes.values() if n.online]
        if len(online) < 2:
            return False
        util = {n.node_id: self.used[n.node_id] / n.capacity for n in online}
        donors = sorted(online, key=lambda n: -util[n.node_id])
        receivers = sorted(online, key=lambda n: util[n.node_id])
        if util[donors[0].node_id] - util[receivers[0].node_id] <= self.balance_threshold:
            return False
        for donor in donors:
            for receiver in receivers:
                if util[donor.node_id] - util[receiver.node_id] <= self.balance_threshold:
                    break
                bid = self._best_block_to_move(donor, receiver)
                if bid is not None:
                    reason = (f"rebalance: {util[donor.node_id]:.0%} -> "
                              f"{util[receiver.node_id]:.0%} utilization")
                    self._emit_move(bid, donor.node_id, receiver.node_id, reason)
                    return True
        return False

    def _best_block_to_move(self, donor: Node, receiver: Node) -> str | None:
        best_bid = None
        best_key = None
        for bid in self.placement_on(donor.node_id):
            block = self.cluster.blocks[bid]
            if receiver.node_id in self.placement[bid]:
                continue
            if receiver.rack in {self.cluster.nodes[n].rack for n in self.placement[bid]}:
                continue
            if self.used[receiver.node_id] + block.size > receiver.capacity:
                continue
            new_donor = (self.used[donor.node_id] - block.size) / donor.capacity
            new_receiver = (self.used[receiver.node_id] + block.size) / receiver.capacity
            if new_receiver > new_donor + 1e-9:
                continue  # never invert the pair; keeps migration count minimal
            gain = (self.used[donor.node_id] / donor.capacity
                    - self.used[receiver.node_id] / receiver.capacity) - (new_donor - new_receiver)
            if gain <= 0:
                continue
            key = (-gain, block.size)
            if best_key is None or key < best_key:
                best_bid, best_key = bid, key
        return best_bid
