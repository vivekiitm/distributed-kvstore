"""
Distributed Key-Value Store Node
=================================
Each node in the cluster maintains its own data store and vector clock.
Nodes can replicate writes to peers and resolve conflicts using vector clocks.
"""

import time
import uuid
import threading
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from .vector_clock import VectorClock, ClockComparison
from .replication import ReplicationLog, WriteOperation


@dataclass
class VersionedValue:
    """A value with its associated vector clock (version)."""
    value: Any
    clock: VectorClock
    timestamp: float = field(default_factory=time.time)
    node_id: str = ""

    def __repr__(self):
        return f"VersionedValue(value={self.value!r}, clock={self.clock}, ts={self.timestamp:.3f})"


class ConflictResolver:
    """
    Strategies for resolving concurrent writes (causally unordered).
    """

    @staticmethod
    def last_write_wins(a: VersionedValue, b: VersionedValue) -> VersionedValue:
        """Resolve by wall-clock timestamp (simple but lossy)."""
        return a if a.timestamp >= b.timestamp else b

    @staticmethod
    def merge_sets(a: VersionedValue, b: VersionedValue) -> VersionedValue:
        """
        For set-typed values, merge them (CRDT-style).
        Falls back to last-write-wins for non-sets.
        """
        if isinstance(a.value, set) and isinstance(b.value, set):
            merged_clock = VectorClock.merge(a.clock, b.clock)
            return VersionedValue(
                value=a.value | b.value,
                clock=merged_clock,
                timestamp=max(a.timestamp, b.timestamp),
                node_id="merged"
            )
        return ConflictResolver.last_write_wins(a, b)

    @staticmethod
    def keep_all(a: VersionedValue, b: VersionedValue) -> List[VersionedValue]:
        """Keep both versions (multi-value register, like Dynamo)."""
        return [a, b]


class KVNode:
    """
    A single node in the distributed key-value store.

    Features:
    - Local reads and writes with vector clock versioning
    - Peer replication with configurable consistency
    - Conflict detection and resolution
    - Anti-entropy via full state sync
    """

    def __init__(
        self,
        node_id: str,
        peers: Optional[List["KVNode"]] = None,
        conflict_strategy: str = "lww",
        replication_factor: int = 2,
    ):
        self.node_id = node_id
        self.peers: List["KVNode"] = peers or []
        self.conflict_strategy = conflict_strategy
        self.replication_factor = replication_factor

        # Core storage: key -> list of VersionedValues (siblings on conflict)
        self._store: Dict[str, List[VersionedValue]] = defaultdict(list)
        self._clock = VectorClock(node_id)
        self._lock = threading.RLock()
        self.replication_log = ReplicationLog(node_id)

        # Stats
        self.stats = {
            "reads": 0,
            "writes": 0,
            "conflicts_detected": 0,
            "conflicts_resolved": 0,
            "replications_sent": 0,
            "replications_received": 0,
        }

    def put(self, key: str, value: Any) -> VersionedValue:
        """
        Write a key-value pair locally and replicate to peers.
        Returns the VersionedValue that was stored.
        """
        with self._lock:
            self._clock.increment()
            versioned = VersionedValue(
                value=value,
                clock=self._clock.copy(),
                node_id=self.node_id
            )
            self._apply_write(key, versioned)
            self.stats["writes"] += 1

            op = WriteOperation(
                key=key,
                versioned_value=versioned,
                origin_node=self.node_id,
                op_id=str(uuid.uuid4())
            )
            self.replication_log.append(op)

        # Replicate asynchronously to peers
        self._replicate_to_peers(key, versioned)
        return versioned

    def get(self, key: str) -> Optional[Any]:
        """
        Read a value. If there are conflicting versions, resolve them.
        Returns None if key not found.
        """
        with self._lock:
            self.stats["reads"] += 1
            siblings = self._store.get(key, [])

            if not siblings:
                return None
            if len(siblings) == 1:
                return siblings[0].value

            # Conflict: resolve and write back
            self.stats["conflicts_detected"] += 1
            resolved = self._resolve_conflict(siblings)
            self._store[key] = [resolved] if isinstance(resolved, VersionedValue) else resolved
            self.stats["conflicts_resolved"] += 1
            return resolved.value if isinstance(resolved, VersionedValue) else [v.value for v in resolved]

    def get_versioned(self, key: str) -> List[VersionedValue]:
        """Return all versions (siblings) for a key."""
        with self._lock:
            return list(self._store.get(key, []))

    def delete(self, key: str) -> bool:
        """Delete a key (tombstone not implemented for brevity)."""
        with self._lock:
            if key in self._store:
                del self._store[key]
                return True
            return False

    def receive_replication(self, key: str, versioned: VersionedValue) -> None:
        """
        Handle an incoming replicated write from a peer.
        Merges with local state using vector clock ordering.
        """
        with self._lock:
            self._clock.merge_in(versioned.clock)
            self._apply_write(key, versioned)
            self.stats["replications_received"] += 1

    def _apply_write(self, key: str, incoming: VersionedValue) -> None:
        """
        Apply a write, handling causality:
        - Discard if we already have a causally newer version
        - Replace if incoming is causally newer
        - Add as sibling if concurrent (conflict)
        """
        existing = self._store.get(key, [])
        survivors = []
        dominated = False

        for stored in existing:
            cmp = VectorClock.compare(incoming.clock, stored.clock)
            if cmp == ClockComparison.BEFORE:
                # Incoming is older — discard it
                survivors.append(stored)
                dominated = True
            elif cmp == ClockComparison.AFTER:
                # Incoming is newer — drop the old version
                pass
            else:
                # Concurrent — keep both as siblings
                survivors.append(stored)

        if not dominated:
            survivors.append(incoming)

        self._store[key] = survivors

    def _resolve_conflict(self, siblings: List[VersionedValue]) -> Any:
        """Apply the configured conflict resolution strategy."""
        if self.conflict_strategy == "lww":
            result = siblings[0]
            for s in siblings[1:]:
                result = ConflictResolver.last_write_wins(result, s)
            return result
        elif self.conflict_strategy == "merge_sets":
            result = siblings[0]
            for s in siblings[1:]:
                result = ConflictResolver.merge_sets(result, s)
            return result
        elif self.conflict_strategy == "multi_value":
            return siblings  # caller handles list
        return siblings[0]

    def _replicate_to_peers(self, key: str, versioned: VersionedValue) -> None:
        """Send write to up to `replication_factor` peers."""
        targets = self.peers[:self.replication_factor]
        for peer in targets:
            try:
                peer.receive_replication(key, versioned)
                self.stats["replications_sent"] += 1
            except Exception as e:
                print(f"[{self.node_id}] Replication to {peer.node_id} failed: {e}")

    def anti_entropy_sync(self, peer: "KVNode") -> Tuple[int, int]:
        """
        Full state sync with a peer (anti-entropy).
        Sends any keys the peer is missing or has older versions of.
        Returns (keys_sent, keys_received).
        """
        sent = 0
        received = 0

        with self._lock:
            local_snapshot = {k: list(v) for k, v in self._store.items()}

        peer_snapshot = peer.get_full_store()

        # Push newer local versions to peer
        for key, versions in local_snapshot.items():
            for v in versions:
                peer_versions = peer_snapshot.get(key, [])
                if not peer_versions or all(
                    VectorClock.compare(v.clock, pv.clock) == ClockComparison.AFTER
                    for pv in peer_versions
                ):
                    peer.receive_replication(key, v)
                    sent += 1

        # Pull newer peer versions locally
        for key, versions in peer_snapshot.items():
            for v in versions:
                local_versions = local_snapshot.get(key, [])
                if not local_versions or all(
                    VectorClock.compare(v.clock, lv.clock) == ClockComparison.AFTER
                    for lv in local_versions
                ):
                    self.receive_replication(key, v)
                    received += 1

        return sent, received

    def get_full_store(self) -> Dict[str, List[VersionedValue]]:
        with self._lock:
            return {k: list(v) for k, v in self._store.items()}

    def add_peer(self, peer: "KVNode") -> None:
        if peer not in self.peers and peer.node_id != self.node_id:
            self.peers.append(peer)

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "node_id": self.node_id,
                "keys_stored": len(self._store),
                "clock": dict(self._clock.clock),
                "peers": [p.node_id for p in self.peers],
                "stats": dict(self.stats),
            }

    def __repr__(self):
        return f"KVNode(id={self.node_id!r}, keys={len(self._store)}, peers={[p.node_id for p in self.peers]})"
