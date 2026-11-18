"""Distributed block placement planner.

Given block replicas, node capacities, a rack/zone hierarchy and the current
placement, build a minimal, multi-step rebalance plan that respects fault
domains and capacity limits while keeping enough healthy replicas throughout.
"""
from .model import Block, Cluster, Node
from .planner import COPY, DELETE, Plan, RebalancePlanner, Step
from .validate import validate_plan

__all__ = [
    "Block",
    "Cluster",
    "Node",
    "Plan",
    "RebalancePlanner",
    "Step",
    "COPY",
    "DELETE",
    "validate_plan",
]
