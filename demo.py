"""
Demo: Distributed KV Store in Action
=====================================
This script walks through key distributed systems scenarios:
  1. Basic replication
  2. Network partition + divergence
  3. Healing + anti-entropy
  4. Concurrent writes + conflict resolution
  5. Vector clock inspection
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src import Cluster, VectorClock, ClockComparison

DIVIDER = "─" * 60


def section(title: str):
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)


def print_cluster_state(cluster: Cluster, key: str):
    print(f"\n  Cluster state for key={key!r}:")
    for node_id, node in cluster.nodes.items():
        val = node.get(key)
        versions = node.get_versioned(key)
        clocks = [str(v.clock) for v in versions]
        print(f"    [{node_id}]  value={val!r}  clocks={clocks}")


# ─────────────────────────────────────────────
# 1. Basic Three-Node Cluster
# ─────────────────────────────────────────────
section("1. Three-Node Cluster: Basic Replication")

cluster = Cluster(conflict_strategy="lww", replication_factor=2)
cluster.add_node("A")
cluster.add_node("B")
cluster.add_node("C")
cluster.connect_all()

print("\n  Writing 'username' = 'alice' to node A...")
v = cluster.put("A", "username", "alice")
print(f"  Written with clock: {v.clock}")

print_cluster_state(cluster, "username")


# ─────────────────────────────────────────────
# 2. Network Partition
# ─────────────────────────────────────────────
section("2. Network Partition: A ✗ B")

print("\n  Partitioning A from B...")
cluster.partition("A", "B")

print("  Writing 'score'=100 to A (reaches C only)")
cluster.put("A", "score", 100)

print("  Writing 'score'=200 to B (reaches C... but C already has A's version)")
cluster.put("B", "score", 200)

print_cluster_state(cluster, "score")


# ─────────────────────────────────────────────
# 3. Healing + Anti-Entropy
# ─────────────────────────────────────────────
section("3. Healing Partition + Anti-Entropy Gossip")

print("\n  Healing partition between A and B...")
cluster.heal_partition("A", "B")

print("\n  Running anti-entropy round...")
results = cluster.anti_entropy()
for node_id, (sent, received) in results.items():
    print(f"    [{node_id}] sent={sent}, received={received}")

print_cluster_state(cluster, "score")
print(f"\n  Is cluster consistent on 'score'? {cluster.is_consistent('score')}")


# ─────────────────────────────────────────────
# 4. Vector Clock Deep Dive
# ─────────────────────────────────────────────
section("4. Vector Clock Causality")

print("\n  Simulating causal chain:")
a = VectorClock("A")
a.increment()
print(f"    A writes:              {a}")

b = VectorClock("B")
b.merge_in(a)   # B receives A's message
b.increment()
print(f"    B receives A, writes:  {b}")

c = VectorClock("C")
c.merge_in(b)   # C receives B's message
c.increment()
print(f"    C receives B, writes:  {c}")

print(f"\n  Compare(A, B): {VectorClock.compare(a, b).value}")  # before
print(f"  Compare(B, C): {VectorClock.compare(b, c).value}")  # before
print(f"  Compare(A, C): {VectorClock.compare(a, c).value}")  # before

print("\n  Simulating concurrent writes (no communication):")
x = VectorClock("X")
x.increment()
print(f"    X writes independently: {x}")

y = VectorClock("Y")
y.increment()
print(f"    Y writes independently: {y}")

print(f"\n  Compare(X, Y): {VectorClock.compare(x, y).value}")  # concurrent!


# ─────────────────────────────────────────────
# 5. CRDT-style Set Merge
# ─────────────────────────────────────────────
section("5. CRDT-style Set Merge (conflict_strategy='merge_sets')")

crdt_cluster = Cluster(conflict_strategy="merge_sets", replication_factor=0)
crdt_cluster.add_node("P")
crdt_cluster.add_node("Q")

# Isolated writes — no replication yet
print("\n  P writes tags: {python, distributed}")
crdt_cluster.put("P", "tags", {"python", "distributed"})

print("  Q writes tags: {databases, python}")
crdt_cluster.put("Q", "tags", {"databases", "python"})

# Manually sync both into a third node
r = crdt_cluster.add_node("R")
r.conflict_strategy = "merge_sets"
p_node = crdt_cluster.get_node("P")
q_node = crdt_cluster.get_node("Q")

for v in p_node.get_versioned("tags"):
    r.receive_replication("tags", v)
for v in q_node.get_versioned("tags"):
    r.receive_replication("tags", v)

merged = r.get("tags")
print(f"\n  Merged tags at R: {merged}")


# ─────────────────────────────────────────────
# 6. Quorum Read
# ─────────────────────────────────────────────
section("6. Quorum Read (majority vote)")

qcluster = Cluster(replication_factor=2)
qcluster.add_node("X")
qcluster.add_node("Y")
qcluster.add_node("Z")
qcluster.connect_all()

qcluster.put("X", "config", "v2.1.0")

print(f"\n  Quorum read (2/3 nodes): {qcluster.quorum_get('config', quorum=2)!r}")
print(f"  Quorum read (3/3 nodes): {qcluster.quorum_get('config', quorum=3)!r}")


# ─────────────────────────────────────────────
print(f"\n{DIVIDER}")
print("  Demo complete!")
print(DIVIDER)
