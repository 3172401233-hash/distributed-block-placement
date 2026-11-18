import unittest

from placement import Block, Cluster, Node, RebalancePlanner, validate_plan


def make_node(nid, rack, zone="z1", capacity=100, online=True):
    return Node(node_id=nid, rack=rack, zone=zone, capacity=capacity, online=online)


def make_block(bid, size, rf=3, min_healthy=2):
    return Block(block_id=bid, size=size, replication_factor=rf, min_healthy=min_healthy)


def plan_and_validate(cluster, **planner_kwargs):
    plan = RebalancePlanner(**planner_kwargs).build_plan(cluster)
    errors = validate_plan(cluster, plan)
    return plan, errors


class TestNodeOffline(unittest.TestCase):
    def test_single_node_offline(self):
        nodes = {
            "n1a": make_node("n1a", "r1"),
            "n1b": make_node("n1b", "r1"),
            "n2": make_node("n2", "r2"),
            "n3": make_node("n3", "r3"),
        }
        blocks = {
            "b1": make_block("b1", 10),
            "b2": make_block("b2", 10),
        }
        cluster = Cluster(nodes, blocks, {
            "b1": ["n1a", "n2", "n3"],
            "b2": ["n1b", "n2", "n3"],
        })
        nodes["n1a"].online = False  # node goes down after placement

        plan, errors = plan_and_validate(cluster)
        self.assertEqual(errors, [])

        copies = {s.target: s for s in plan.copy_steps}
        self.assertIn("n1b", copies)  # replacement lands back in rack r1
        self.assertEqual(copies["n1b"].block_id, "b1")
        self.assertIn("node-offline", copies["n1b"].reason)
        deletes = [s for s in plan.delete_steps if s.source == "n1a"]
        self.assertEqual(len(deletes), 1)  # stale replica cleaned up


class TestRackFailure(unittest.TestCase):
    def test_whole_rack_failure(self):
        nodes = {
            "n1": make_node("n1", "r1", zone="z1"),
            "n2": make_node("n2", "r2", zone="z1"),
            "n3": make_node("n3", "r3", zone="z2"),
            "n4": make_node("n4", "r4", zone="z2"),
        }
        blocks = {f"b{i}": make_block(f"b{i}", 10) for i in range(3)}
        cluster = Cluster(nodes, blocks,
                          {f"b{i}": ["n1", "n2", "n3"] for i in range(3)})
        nodes["n2"].online = False  # entire rack r2 lost

        plan, errors = plan_and_validate(cluster)
        self.assertEqual(errors, [])
        self.assertEqual(len(plan.copy_steps), 3)  # one replacement per block
        for step in plan.copy_steps:
            self.assertEqual(step.target, "n4")  # only surviving spare rack


class TestNodeJoin(unittest.TestCase):
    def test_new_node_absorbs_load(self):
        nodes = {
            "n1": make_node("n1", "r1"),
            "n2": make_node("n2", "r2"),
            "n3": make_node("n3", "r3"),
            "n4": make_node("n4", "r4"),  # freshly joined, empty
        }
        blocks = {f"b{i}": make_block(f"b{i}", 10) for i in range(9)}
        cluster = Cluster(nodes, blocks,
                          {f"b{i}": ["n1", "n2", "n3"] for i in range(9)})

        plan, errors = plan_and_validate(cluster)
        self.assertEqual(errors, [])
        self.assertGreater(len(plan.copy_steps), 0)

        used = cluster.used_bytes()
        for step in plan.steps:
            block = cluster.blocks[step.block_id]
            if step.action == "copy":
                used[step.target] += block.size
            else:
                used[step.source] -= block.size
        self.assertGreater(used["n4"], 0)
        utils = [used[n] / nodes[n].capacity for n in nodes]
        self.assertLessEqual(max(utils) - min(utils), 0.15 + 1e-9)


class TestUnderReplicated(unittest.TestCase):
    def test_under_replicated_block(self):
        nodes = {f"n{i}": make_node(f"n{i}", f"r{i}") for i in (1, 2, 3)}
        blocks = {"b1": make_block("b1", 10, rf=3, min_healthy=1)}
        cluster = Cluster(nodes, blocks, {"b1": ["n1"]})  # only one replica left

        plan, errors = plan_and_validate(cluster)
        self.assertEqual(errors, [])
        self.assertEqual(len(plan.copy_steps), 2)
        targets = {s.target for s in plan.copy_steps}
        self.assertEqual(targets, {"n2", "n3"})
        self.assertTrue(all("under-replicated" in s.reason for s in plan.copy_steps))


class TestCapacityBoundary(unittest.TestCase):
    def test_over_capacity_node_is_drained_minimally(self):
        nodes = {
            "n1": make_node("n1", "r1"),
            "n2": make_node("n2", "r2"),
            "n3": make_node("n3", "r3"),
        }
        blocks = {
            "b1": make_block("b1", 40, rf=1, min_healthy=1),
            "b2": make_block("b2", 40, rf=1, min_healthy=1),
            "b3": make_block("b3", 30, rf=1, min_healthy=1),
            "b4": make_block("b4", 95, rf=1, min_healthy=1),
        }
        cluster = Cluster(nodes, blocks, {
            "b1": ["n1"], "b2": ["n1"], "b3": ["n1"],  # n1 at 110/100
            "b4": ["n3"],                              # n3 nearly full
        })

        plan, errors = plan_and_validate(cluster, balance_threshold=1.0)
        self.assertEqual(errors, [])
        moves = [(s.block_id, s.target) for s in plan.copy_steps]
        self.assertEqual(moves, [("b1", "n2")])  # one largest block, not to full n3

    def test_exact_fit_and_no_fit(self):
        nodes = {
            "n1": make_node("n1", "r1"),
            "n2": make_node("n2", "r2"),
        }
        blocks = {
            "b1": make_block("b1", 90, rf=1, min_healthy=1),
            "b2": make_block("b2", 80, rf=1, min_healthy=1),
            "b3": make_block("b3", 20, rf=1, min_healthy=1),  # fits exactly on n2
            "b4": make_block("b4", 21, rf=1, min_healthy=1),  # fits nowhere
        }
        cluster = Cluster(nodes, blocks, {
            "b1": ["n1"], "b2": ["n2"], "b3": [], "b4": [],
        })

        plan, _ = plan_and_validate(cluster, balance_threshold=1.0)
        copies = {s.block_id: s.target for s in plan.copy_steps}
        self.assertEqual(copies.get("b3"), "n2")  # 80 + 20 == capacity, allowed
        self.assertNotIn("b4", copies)
        self.assertTrue(any("b4" in w for w in plan.warnings))


class TestFaultDomainViolation(unittest.TestCase):
    def test_three_replicas_in_one_rack(self):
        nodes = {
            "n1": make_node("n1", "r1"),
            "n2": make_node("n2", "r1"),
            "n3": make_node("n3", "r1"),
            "n4": make_node("n4", "r2"),
            "n5": make_node("n5", "r3"),
        }
        blocks = {"b1": make_block("b1", 10)}
        cluster = Cluster(nodes, blocks, {"b1": ["n1", "n2", "n3"]})

        plan, errors = plan_and_validate(cluster)
        self.assertEqual(errors, [])
        self.assertEqual(len(plan.copy_steps), 2)  # minimal: exactly two move out
        targets = {s.target for s in plan.copy_steps}
        self.assertEqual(targets, {"n4", "n5"})
        self.assertTrue(all("fault-domain" in s.reason for s in plan.copy_steps))


class TestPlanShape(unittest.TestCase):
    def test_steps_carry_source_target_reason(self):
        nodes = {f"n{i}": make_node(f"n{i}", f"r{i}") for i in (1, 2, 3, 4)}
        blocks = {"b1": make_block("b1", 10)}
        cluster = Cluster(nodes, blocks, {"b1": ["n1", "n1", "n2"]})
        # duplicate on n1 is invalid input; use distinct nodes in one rack instead
        nodes["n3"].rack = "r1"
        cluster.placement["b1"] = ["n1", "n3", "n2"]

        plan, errors = plan_and_validate(cluster)
        self.assertEqual(errors, [])
        self.assertGreater(len(plan.steps), 1)  # multi-step plan
        for step in plan.steps:
            self.assertTrue(step.reason)
            if step.action == "copy":
                self.assertIsNotNone(step.target)
            else:
                self.assertIsNotNone(step.source)
        # every COPY of a block precedes the matching DELETE (copy-before-delete)
        for bid in blocks:
            actions = [s.action for s in plan.steps if s.block_id == bid]
            self.assertEqual(actions, sorted(actions, key=lambda a: a != "copy"))


if __name__ == "__main__":
    unittest.main()
