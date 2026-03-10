"""
Cluster
=======
Manages a group of KVNode instances as a logical cluster.
Provides a high-level interface for:
  - Adding/removing nodes
  - Simulating network partitions
  - Running anti-entropy (gossip) across all nodes
  - Cluster-wide reads (quorum)
"""

import random
from typing import List, Optional, Dict, Any, Set, Tuple
from .node import KVNode, VersionedValue


class Cluster:
    """
    Coordinates a set of KVNode instances.

    Usage:
        cluster = Cluster()
        cluster.add_node("A")
        cluster.add_node("B")
        cluster.add_node("C")
        cluster.connect_all()

        cluster.put("A", "username", "alice")
        cluster.get("C", "username")   # reads from node C
        cluster.anti_entropy()          # sync all nodes
    """

    def __init__(self, conflict_strategy: str = "lww", replication_factor: int = 2):
        self.nodes: Dict[str, KVNode] = {}
        self.conflict_strategy = conflict_strategy
        self.replication_factor = replication_factor
        self._partitions: Set[Tuple[str, str]] = set()  # pairs that can't communicate

    def add_node(self, node_id: str) -> KVNode:
        """Create and register a new node in the cluster."""
        if node_id in self.nodes:
            raise ValueError(f"Node '{node_id}' already exists.")
        node = KVNode(
            node_id=node_id,
            conflict_strategy=self.conflict_strategy,
            replication_factor=self.replication_factor,
        )
        self.nodes[node_id] = node
        return node

    def get_node(self, node_id: str) -> KVNode:
        if node_id not in self.nodes:
            raise KeyError(f"Node '{node_id}' not found in cluster.")
        return self.nodes[node_id]

    def connect_all(self) -> None:
        """Wire all nodes as peers of each other (fully connected mesh)."""
        node_list = list(self.nodes.values())
        for i, node in enumerate(node_list):
            for other in node_list:
                if other.node_id != node.node_id:
                    node.add_peer(other)

    def put(self, node_id: str, key: str, value: Any) -> VersionedValue:
        """Write to a specific node."""
        return self.get_node(node_id).put(key, value)

    def get(self, node_id: str, key: str) -> Optional[Any]:
        """Read from a specific node."""
        return self.get_node(node_id).get(key)

    def quorum_get(self, key: str, quorum: int = None) -> Optional[Any]:
        """
        Read from a quorum of nodes and return the most causally recent value.
        Useful when you can't trust any single node.
        """
        if quorum is None:
            quorum = len(self.nodes) // 2 + 1

        sample = random.sample(list(self.nodes.values()), min(quorum, len(self.nodes)))
        candidates = []
        for node in sample:
            versions = node.get_versioned(key)
            candidates.extend(versions)

        if not candidates:
            return None

        # Find the most causally advanced version
        best = candidates[0]
        for v in candidates[1:]:
            from .vector_clock import VectorClock, ClockComparison
            if VectorClock.compare(v.clock, best.clock) == ClockComparison.AFTER:
                best = v

        return best.value

    def anti_entropy(self) -> Dict[str, Tuple[int, int]]:
        """
        Run a round of anti-entropy (gossip) between all node pairs.
        Each node syncs with a random peer.
        Returns a dict of {node_id: (sent, received)} per node.
        """
        results = {}
        node_list = list(self.nodes.values())

        for node in node_list:
            if node.peers:
                peer = random.choice(node.peers)
                sent, received = node.anti_entropy_sync(peer)
                results[node.node_id] = (sent, received)

        return results

    def partition(self, node_a: str, node_b: str) -> None:
        """
        Simulate a network partition: A and B can no longer communicate.
        (Removes each from the other's peer list.)
        """
        a = self.get_node(node_a)
        b = self.get_node(node_b)
        a.peers = [p for p in a.peers if p.node_id != node_b]
        b.peers = [p for p in b.peers if p.node_id != node_a]
        self._partitions.add((min(node_a, node_b), max(node_a, node_b)))
        print(f"[Cluster] Partition created between {node_a} <✗> {node_b}")

    def heal_partition(self, node_a: str, node_b: str) -> None:
        """Heal a network partition and re-add peers."""
        a = self.get_node(node_a)
        b = self.get_node(node_b)
        if b not in a.peers:
            a.peers.append(b)
        if a not in b.peers:
            b.peers.append(a)
        key = (min(node_a, node_b), max(node_a, node_b))
        self._partitions.discard(key)
        print(f"[Cluster] Partition healed between {node_a} <✓> {node_b}")

    def status(self) -> List[Dict[str, Any]]:
        """Return status of all nodes."""
        return [node.status() for node in self.nodes.values()]

    def is_consistent(self, key: str) -> bool:
        """Check if all nodes agree on the value for a key."""
        values = [node.get(key) for node in self.nodes.values()]
        return len(set(str(v) for v in values)) <= 1

    def __repr__(self):
        return f"Cluster(nodes={list(self.nodes.keys())}, strategy={self.conflict_strategy!r})"
