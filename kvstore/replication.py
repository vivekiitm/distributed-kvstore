"""
Replication Log
===============
Tracks all write operations performed on a node.
Useful for debugging, audit trails, and potential log-based replication.
"""

import time
from dataclasses import dataclass, field
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .node import VersionedValue


@dataclass
class WriteOperation:
    """Represents a single write event in the replication log."""
    key: str
    versioned_value: "VersionedValue"
    origin_node: str
    op_id: str
    timestamp: float = field(default_factory=time.time)
    replicated_to: List[str] = field(default_factory=list)

    def mark_replicated(self, node_id: str):
        self.replicated_to.append(node_id)

    def __repr__(self):
        return (
            f"WriteOp(id={self.op_id[:8]}, key={self.key!r}, "
            f"origin={self.origin_node}, ts={self.timestamp:.3f})"
        )


class ReplicationLog:
    """
    Append-only log of all write operations for a node.
    Provides filtering and basic inspection utilities.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._log: List[WriteOperation] = []

    def append(self, op: WriteOperation) -> None:
        self._log.append(op)

    def get_ops_for_key(self, key: str) -> List[WriteOperation]:
        return [op for op in self._log if op.key == key]

    def get_ops_since(self, since_ts: float) -> List[WriteOperation]:
        return [op for op in self._log if op.timestamp >= since_ts]

    def last_op(self) -> Optional[WriteOperation]:
        return self._log[-1] if self._log else None

    def summary(self) -> dict:
        keys = {op.key for op in self._log}
        return {
            "node_id": self.node_id,
            "total_ops": len(self._log),
            "unique_keys": len(keys),
            "keys": sorted(keys),
        }

    def __len__(self):
        return len(self._log)

    def __iter__(self):
        return iter(self._log)

    def __repr__(self):
        return f"ReplicationLog(node={self.node_id!r}, ops={len(self._log)})"
