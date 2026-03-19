# distributed-kvstore

> A from-scratch distributed key-value store in pure Python — implementing vector clocks, eventual consistency, conflict resolution, and anti-entropy gossip. Inspired by [Amazon Dynamo](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf). Zero dependencies.

---

## Why This Exists

Most tutorials on distributed systems stay theoretical. This project is a hands-on implementation of the core ideas behind Dynamo-style databases — the kind of system that powers Amazon's shopping cart, Riak, and Cassandra's gossip layer. If you want to understand *how* eventual consistency actually works in code, this is a good place to start.

---

## Features

| Concept | Implementation |
|---|---|
| **Vector Clocks** | Causal ordering of events across nodes |
| **Eventual Consistency** | Nodes converge after network partitions are healed |
| **Conflict Detection** | Concurrent writes tracked as siblings |
| **Conflict Resolution** | LWW, CRDT set-merge, and multi-value strategies |
| **Anti-Entropy Gossip** | Peers sync full state in background |
| **Network Partitions** | Simulate and heal splits between nodes |
| **Quorum Reads** | Majority reads for freshness guarantees |
| **Replication Log** | Append-only write history per node |

---

## Architecture

```
┌────────────────────────────────────────────┐
│                  Cluster                    │
│  ┌──────┐     ┌──────┐     ┌──────┐        │
│  │ Node │────▶│ Node │────▶│ Node │        │
│  │  A   │◀────│  B   │◀────│  C   │        │
│  └──────┘     └──────┘     └──────┘        │
│     │              │            │           │
│  [VectorClock] [VectorClock] [VectorClock] │
│  [KV Store  ] [KV Store  ] [KV Store  ]   │
│  [Replic Log] [Replic Log] [Replic Log]   │
└────────────────────────────────────────────┘
```

Each node independently maintains its own key-value store and vector clock. Writes are replicated to N peers (configurable). Concurrent writes produce siblings; anti-entropy gossip ensures all nodes eventually converge.

---

## Project Structure

```
distributed-kvstore/
├── kvstore/
│   ├── vector_clock.py   # VectorClock + ClockComparison enum
│   ├── node.py           # KVNode — single store + peer replication
│   ├── replication.py    # WriteOperation + ReplicationLog
│   └── cluster.py        # Cluster — multi-node coordinator
├── server/               # (HTTP server layer)
├── scripts/              # Helper / demo scripts
├── demo.py               # Runnable walkthrough of all concepts
└── README.md
```

---

## Quick Start

No pip installs required — pure Python 3.7+.

```bash
git clone https://github.com/vivekiitm/distributed-kvstore.git
cd distributed-kvstore
python demo.py
```

The demo walks through:

- Basic 3-node write and replication
- Network partition → divergence → heal → convergence
- Vector clock causality trace
- CRDT-style set merge
- Quorum reads

---

## Usage Examples

### Vector Clocks

```python
from kvstore import VectorClock, ClockComparison

a = VectorClock("A")
a.increment()          # {A: 1}

b = VectorClock("B")
b.merge_in(a)          # {A: 1, B: 0}
b.increment()          # {A: 1, B: 1}

VectorClock.compare(a, b)  # → ClockComparison.BEFORE
```

Two clocks can be compared element-wise to determine whether one event *caused* the other or if they are *concurrent* (a conflict).

### Conflict Resolution

When concurrent writes are detected, `ConflictResolver` selects a winner based on the chosen strategy:

| Strategy | Behaviour |
|---|---|
| `lww` | Last-write-wins by wall-clock timestamp |
| `merge_sets` | Union of set values (CRDT G-Set style) |
| `multi_value` | Retain all siblings — like Riak's multi-value registers |

### Simulating Network Partitions

```python
cluster.partition("A", "B")    # A and B can no longer communicate

cluster.put("A", "x", 1)       # writes to A and C
cluster.put("B", "x", 99)      # writes to B — concurrent conflict!

cluster.heal_partition("A", "B")
cluster.anti_entropy()          # gossip syncs diverged state; nodes converge
```

---

## Running Tests

```bash
python -m pytest tests/ -v
# or directly:
python tests/test_all.py
```

The test suite covers 25+ scenarios including causal ordering, partition behaviour, conflict resolution strategies, and quorum correctness.

---

## Key Concepts Explained

### What is a Vector Clock?

A vector clock is a map of `{node_id: counter}`. Every time a node writes, it increments its own counter. When a write is replicated to another node, the receiving node merges the incoming clock with its own. This lets you determine the causal relationship between any two events:

- **A happened before B** — A's clock is strictly dominated by B's
- **B happened before A** — the reverse
- **Concurrent** — neither dominates; this is a conflict

### What is Eventual Consistency?

Nodes don't need to agree on a value at every instant. As long as they keep gossiping and no new writes arrive, they will *eventually* converge to the same state. This trades strong consistency for availability and partition tolerance (the AP side of CAP).

### What is Anti-Entropy?

Each node periodically picks a random peer and exchanges its full state. Any keys the peer is missing (or has older versions of) get updated. This is the self-healing mechanism that makes eventual consistency live up to its name.

---

## References

- [Amazon Dynamo Paper (2007)](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) — the original inspiration
- [Lamport Clocks (1978)](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) — foundational paper on logical clocks
- [Riak's multi-value registers](https://riak.com/posts/technical/vector-clocks-revisited/index.html) — practical application of sibling values
- [Designing Data-Intensive Applications](https://dataintensive.net/) — Kleppmann, Chapter 5 (Replication)

---

## License

MIT
