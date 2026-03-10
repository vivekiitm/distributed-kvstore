"""
Vector Clocks
=============
A vector clock assigns a logical timestamp to each event in a distributed system.
By comparing two clocks, we can determine if one event CAUSED another,
or if they happened CONCURRENTLY (a conflict).

Key rules:
  - On local event:      clock[self] += 1
  - On send:             clock[self] += 1, attach clock to message
  - On receive:          clock = element-wise max(local, received), then clock[self] += 1
"""

from enum import Enum
from typing import Dict


class ClockComparison(Enum):
    BEFORE = "before"    # a happened before b (a caused b)
    AFTER = "after"      # a happened after b (b caused a)
    EQUAL = "equal"      # identical clocks
    CONCURRENT = "concurrent"  # neither caused the other → conflict!


class VectorClock:
    """
    Immutable-safe vector clock for tracking causality across nodes.

    Example:
        vc_a = VectorClock("A")
        vc_a.increment()          # A:1
        vc_b = VectorClock("B")
        vc_b.merge_in(vc_a)       # A:1, B:0
        vc_b.increment()          # A:1, B:1
        # Now A:1 BEFORE A:1,B:1 → B's event causally follows A's
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.clock: Dict[str, int] = {node_id: 0}

    def increment(self) -> "VectorClock":
        """Increment this node's own counter (local event or send)."""
        self.clock[self.node_id] = self.clock.get(self.node_id, 0) + 1
        return self

    def merge_in(self, other: "VectorClock") -> "VectorClock":
        """
        Merge another clock into this one (element-wise max).
        Called on receive, before incrementing own counter.
        """
        for node, ts in other.clock.items():
            self.clock[node] = max(self.clock.get(node, 0), ts)
        return self

    def copy(self) -> "VectorClock":
        """Return a deep copy of this clock."""
        vc = VectorClock(self.node_id)
        vc.clock = dict(self.clock)
        return vc

    @staticmethod
    def compare(a: "VectorClock", b: "VectorClock") -> ClockComparison:
        """
        Determine the causal relationship between two vector clocks.

        a <= b  iff  for all nodes n: a[n] <= b[n]
        a < b   iff  a <= b  AND  a != b

        If neither a<=b nor b<=a, they are CONCURRENT (conflict).
        """
        all_nodes = set(a.clock) | set(b.clock)

        a_leq_b = all(a.clock.get(n, 0) <= b.clock.get(n, 0) for n in all_nodes)
        b_leq_a = all(b.clock.get(n, 0) <= a.clock.get(n, 0) for n in all_nodes)

        if a_leq_b and b_leq_a:
            return ClockComparison.EQUAL
        elif a_leq_b:
            return ClockComparison.BEFORE
        elif b_leq_a:
            return ClockComparison.AFTER
        else:
            return ClockComparison.CONCURRENT

    @staticmethod
    def merge(a: "VectorClock", b: "VectorClock") -> "VectorClock":
        """Return a new clock that is the element-wise max of a and b."""
        all_nodes = set(a.clock) | set(b.clock)
        # Use first node_id arbitrarily for the merged clock's identity
        merged = VectorClock(a.node_id)
        merged.clock = {n: max(a.clock.get(n, 0), b.clock.get(n, 0)) for n in all_nodes}
        return merged

    def dominates(self, other: "VectorClock") -> bool:
        """Return True if self is strictly AFTER other (self causally follows other)."""
        return VectorClock.compare(self, other) == ClockComparison.AFTER

    def is_concurrent_with(self, other: "VectorClock") -> bool:
        """Return True if neither clock causally precedes the other."""
        return VectorClock.compare(self, other) == ClockComparison.CONCURRENT

    def __repr__(self):
        parts = ", ".join(f"{k}:{v}" for k, v in sorted(self.clock.items()))
        return f"VC({{{parts}}})"

    def __eq__(self, other):
        if not isinstance(other, VectorClock):
            return False
        return VectorClock.compare(self, other) == ClockComparison.EQUAL
