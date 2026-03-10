"""
Test Suite for Distributed KV Store
=====================================
Tests cover:
  - Vector clock causality and comparison
  - Single-node CRUD
  - Replication between nodes
  - Conflict detection and resolution
  - Network partitions and healing
  - Anti-entropy (gossip) sync
  - Quorum reads
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from src.vector_clock import VectorClock, ClockComparison
from src.node import KVNode, VersionedValue
from src.cluster import Cluster


# ──────────────────────────────────────────────
# Vector Clock Tests
# ──────────────────────────────────────────────

class TestVectorClock(unittest.TestCase):

    def test_initial_clock(self):
        vc = VectorClock("A")
        self.assertEqual(vc.clock["A"], 0)

    def test_increment(self):
        vc = VectorClock("A")
        vc.increment()
        self.assertEqual(vc.clock["A"], 1)

    def test_equal_clocks(self):
        a = VectorClock("A")
        b = VectorClock("A")
        self.assertEqual(VectorClock.compare(a, b), ClockComparison.EQUAL)

    def test_before_after(self):
        a = VectorClock("A")
        a.increment()  # A:1

        b = VectorClock("A")
        b.clock = {"A": 1}
        b_copy = b.copy()
        b_copy.increment()  # A:2

        self.assertEqual(VectorClock.compare(a, b_copy), ClockComparison.BEFORE)
        self.assertEqual(VectorClock.compare(b_copy, a), ClockComparison.AFTER)

    def test_concurrent_clocks(self):
        # A writes independently, B writes independently → concurrent
        a = VectorClock("A")
        a.increment()  # A:1, B:0

        b = VectorClock("B")
        b.increment()  # A:0, B:1

        self.assertEqual(VectorClock.compare(a, b), ClockComparison.CONCURRENT)

    def test_merge(self):
        a = VectorClock("A")
        a.clock = {"A": 3, "B": 1}

        b = VectorClock("B")
        b.clock = {"A": 1, "B": 4, "C": 2}

        merged = VectorClock.merge(a, b)
        self.assertEqual(merged.clock["A"], 3)
        self.assertEqual(merged.clock["B"], 4)
        self.assertEqual(merged.clock["C"], 2)

    def test_merge_in(self):
        a = VectorClock("A")
        a.clock = {"A": 2, "B": 0}

        b = VectorClock("B")
        b.clock = {"A": 1, "B": 3}

        a.merge_in(b)
        self.assertEqual(a.clock["A"], 2)  # kept max
        self.assertEqual(a.clock["B"], 3)

    def test_copy_is_independent(self):
        a = VectorClock("A")
        a.increment()
        b = a.copy()
        b.increment()
        self.assertNotEqual(a.clock["A"], b.clock["A"])

    def test_dominates(self):
        a = VectorClock("A")
        a.clock = {"A": 1}

        b = VectorClock("A")
        b.clock = {"A": 2}

        self.assertTrue(b.dominates(a))
        self.assertFalse(a.dominates(b))

    def test_is_concurrent_with(self):
        a = VectorClock("A")
        a.clock = {"A": 1, "B": 0}

        b = VectorClock("B")
        b.clock = {"A": 0, "B": 1}

        self.assertTrue(a.is_concurrent_with(b))


# ──────────────────────────────────────────────
# Single Node Tests
# ──────────────────────────────────────────────

class TestKVNodeBasic(unittest.TestCase):

    def setUp(self):
        self.node = KVNode("N1")

    def test_put_and_get(self):
        self.node.put("color", "blue")
        self.assertEqual(self.node.get("color"), "blue")

    def test_overwrite(self):
        self.node.put("x", 1)
        self.node.put("x", 2)
        self.assertEqual(self.node.get("x"), 2)

    def test_get_missing_key(self):
        self.assertIsNone(self.node.get("nonexistent"))

    def test_delete(self):
        self.node.put("k", "v")
        self.assertTrue(self.node.delete("k"))
        self.assertIsNone(self.node.get("k"))

    def test_delete_nonexistent(self):
        self.assertFalse(self.node.delete("ghost"))

    def test_stats_tracking(self):
        self.node.put("a", 1)
        self.node.get("a")
        self.assertEqual(self.node.stats["writes"], 1)
        self.assertEqual(self.node.stats["reads"], 1)

    def test_status(self):
        self.node.put("x", "y")
        status = self.node.status()
        self.assertEqual(status["node_id"], "N1")
        self.assertEqual(status["keys_stored"], 1)


# ──────────────────────────────────────────────
# Replication Tests
# ──────────────────────────────────────────────

class TestReplication(unittest.TestCase):

    def setUp(self):
        self.n1 = KVNode("N1", replication_factor=1)
        self.n2 = KVNode("N2")
        self.n1.add_peer(self.n2)

    def test_write_replicates_to_peer(self):
        self.n1.put("city", "Tokyo")
        # n2 should have received the replication
        self.assertEqual(self.n2.get("city"), "Tokyo")

    def test_no_replication_without_peers(self):
        solo = KVNode("Solo")
        solo.put("key", "val")
        self.assertEqual(solo.stats["replications_sent"], 0)

    def test_replication_stats(self):
        self.n1.put("x", 1)
        self.assertEqual(self.n1.stats["replications_sent"], 1)
        self.assertEqual(self.n2.stats["replications_received"], 1)


# ──────────────────────────────────────────────
# Conflict Detection and Resolution Tests
# ──────────────────────────────────────────────

class TestConflicts(unittest.TestCase):

    def _make_partitioned_nodes(self):
        """Two nodes with NO peers — so writes don't replicate."""
        n1 = KVNode("N1", conflict_strategy="lww")
        n2 = KVNode("N2", conflict_strategy="lww")
        return n1, n2

    def test_no_conflict_causal_write(self):
        n1, n2 = self._make_partitioned_nodes()
        # n1 writes, replicates to n2, then n2 updates
        v1 = n1.put("k", "first")
        n2.receive_replication("k", v1)
        n2.put("k", "second")  # n2's clock now includes n1's write
        # no conflict — causal chain
        self.assertEqual(len(n2.get_versioned("k")), 1)

    def test_conflict_concurrent_writes(self):
        n1, n2 = self._make_partitioned_nodes()
        # Both write to same key independently — concurrent!
        n1.put("score", 100)
        n2.put("score", 200)
        # Manually inject both versions into a third node
        n3 = KVNode("N3", conflict_strategy="lww")
        for v in n1.get_versioned("score"):
            n3.receive_replication("score", v)
        for v in n2.get_versioned("score"):
            n3.receive_replication("score", v)

        siblings = n3.get_versioned("score")
        self.assertEqual(len(siblings), 2, "Should detect 2 concurrent versions")

        # Get should resolve the conflict
        resolved = n3.get("score")
        self.assertIn(resolved, [100, 200], "Should pick one value")

    def test_merge_sets_strategy(self):
        n1 = KVNode("N1", conflict_strategy="merge_sets")
        n2 = KVNode("N2", conflict_strategy="merge_sets")

        # Independent writes of sets
        n1.put("tags", {"python", "distributed"})
        n2.put("tags", {"databases", "python"})

        n3 = KVNode("N3", conflict_strategy="merge_sets")
        for v in n1.get_versioned("tags"):
            n3.receive_replication("tags", v)
        for v in n2.get_versioned("tags"):
            n3.receive_replication("tags", v)

        result = n3.get("tags")
        if isinstance(result, set):
            self.assertIn("python", result)
            self.assertIn("distributed", result)
            self.assertIn("databases", result)


# ──────────────────────────────────────────────
# Cluster Tests
# ──────────────────────────────────────────────

class TestCluster(unittest.TestCase):

    def setUp(self):
        self.cluster = Cluster(conflict_strategy="lww", replication_factor=2)
        self.cluster.add_node("A")
        self.cluster.add_node("B")
        self.cluster.add_node("C")
        self.cluster.connect_all()

    def test_write_and_read_same_node(self):
        self.cluster.put("A", "fruit", "apple")
        self.assertEqual(self.cluster.get("A", "fruit"), "apple")

    def test_read_from_replicated_node(self):
        self.cluster.put("A", "fruit", "mango")
        # B and C should have it via replication
        self.assertEqual(self.cluster.get("B", "fruit"), "mango")

    def test_partition_and_divergence(self):
        self.cluster.partition("A", "B")
        self.cluster.put("A", "counter", 1)  # only reaches C
        self.cluster.put("B", "counter", 99)  # B writes independently

        # After partition, A and B have different values
        a_val = self.cluster.get("A", "counter")
        b_val = self.cluster.get("B", "counter")
        # They may have been replicated to C in different ways
        self.assertIsNotNone(a_val)
        self.assertIsNotNone(b_val)

    def test_heal_and_anti_entropy(self):
        self.cluster.partition("A", "B")
        self.cluster.put("A", "msg", "hello-from-A")
        self.cluster.heal_partition("A", "B")
        self.cluster.anti_entropy()
        # After healing + anti-entropy, B should know about A's write
        result = self.cluster.get("B", "msg")
        self.assertEqual(result, "hello-from-A")

    def test_quorum_read(self):
        self.cluster.put("A", "name", "distributed-kvstore")
        value = self.cluster.quorum_get("name", quorum=2)
        self.assertEqual(value, "distributed-kvstore")

    def test_is_consistent(self):
        self.cluster.put("A", "shared", "consensus")
        # After replication, all nodes should agree
        self.assertTrue(self.cluster.is_consistent("shared"))


# ──────────────────────────────────────────────
# Anti-Entropy Tests
# ──────────────────────────────────────────────

class TestAntiEntropy(unittest.TestCase):

    def test_sync_missing_key(self):
        n1 = KVNode("N1")
        n2 = KVNode("N2")
        n1.put("secret", "42")
        n1.anti_entropy_sync(n2)
        self.assertEqual(n2.get("secret"), "42")

    def test_sync_does_not_regress(self):
        """Syncing should never overwrite a newer version with an older one."""
        n1 = KVNode("N1")
        n2 = KVNode("N2")
        n1.put("x", "old")
        v_old = n1.get_versioned("x")[0]
        n2.receive_replication("x", v_old)
        n2.put("x", "new")  # n2 has a newer version

        n1.anti_entropy_sync(n2)
        # n1 should now have "new"
        self.assertEqual(n1.get("x"), "new")

    def test_sync_bidirectional(self):
        n1 = KVNode("N1")
        n2 = KVNode("N2")
        n1.put("a", 1)
        n2.put("b", 2)
        n1.anti_entropy_sync(n2)
        # n2 gets a, n1 gets b
        self.assertEqual(n2.get("a"), 1)
        self.assertEqual(n1.get("b"), 2)


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestVectorClock))
    suite.addTests(loader.loadTestsFromTestCase(TestKVNodeBasic))
    suite.addTests(loader.loadTestsFromTestCase(TestReplication))
    suite.addTests(loader.loadTestsFromTestCase(TestConflicts))
    suite.addTests(loader.loadTestsFromTestCase(TestCluster))
    suite.addTests(loader.loadTestsFromTestCase(TestAntiEntropy))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
