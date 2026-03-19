# distributed-kvstore

A distributed key-value store built in Python that demonstrates core concepts from **Dynamo-style systems** — vector clocks, eventual consistency, conflict resolution, and anti-entropy gossip.

## What This Demonstrates

| Concept | Where |
|---|---|
| **Vector Clocks** | `src/vector_clock.py` — causal ordering of events |
| **Eventual Consistency** | `src/cluster.py` — nodes converge after partitions |
| **Conflict Detection** | `src/node.py` — concurrent writes tracked as siblings |
| **Conflict Resolution** | `ConflictResolver` — LWW, set-merge (CRDT), multi-value |
| **Anti-Entropy Gossip** | `KVNode.anti_entropy_sync()` — peers sync full state |
| **Network Partitions** | `Cluster.partition()` / `heal_partition()` |
| **Quorum Reads** | `Cluster.quorum_get()` — majority read for freshness |
| **Replication Log** | `src/replication.py` — append-only write history |

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

Each node:
- Maintains its own key-value store and vector clock
- Replicates writes to N peers (configurable)
- Detects concurrent writes as *siblings* (conflicts)
- Can sync full state with a peer (anti-entropy)

## Quick Start

```bash
# No dependencies — pure Python 3.7+
python demo.py
```

**Expected output includes:**
- Basic 3-node write/replication
- Partition → divergence → heal → convergence
- Vector clock causality trace
- CRDT-style set merge
- Quorum reads

## Run Tests

```bash
python -m pytest tests/ -v
# or
python tests/test_all.py
```

## Core Concepts

### Vector Clocks

Each write is tagged with a vector clock: a map of `{node_id: counter}`.

```python
from src import VectorClock, ClockComparison

a = VectorClock("A")
a.increment()          # A:1

b = VectorClock("B")
b.merge_in(a)          # A:1, B:0
b.increment()          # A:1, B:1

VectorClock.compare(a, b)  # → ClockComparison.BEFORE
```

By comparing two clocks element-wise, we know if one *caused* the other or if they're *concurrent* (conflict).

### Conflict Resolution

When two concurrent writes are detected, `ConflictResolver` picks a winner:

| Strategy | Behavior |
|---|---|
| `lww` | Last-write-wins by wall-clock timestamp |
| `merge_sets` | Union of set values (CRDT G-Set style) |
| `multi_value` | Keep all siblings (like Riak) |

### Network Partitions

```python
cluster.partition("A", "B")   # A and B can't communicate
cluster.put("A", "x", 1)      # writes to A, C
cluster.put("B", "x", 99)     # writes to B (concurrent!)

cluster.heal_partition("A", "B")
cluster.anti_entropy()         # gossip syncs diverged state
```

## Project Structure

```
distributed-kvstore/
├── src/
│   ├── vector_clock.py   # VectorClock + ClockComparison
│   ├── node.py           # KVNode — single store + peer replication
│   ├── replication.py    # WriteOperation + ReplicationLog
│   └── cluster.py        # Cluster — multi-node coordinator
├── tests/
│   └── test_all.py       # 25+ unit tests
├── demo.py               # Runnable walkthrough
└── README.md
```

## References/Papers

- [Amazon Dynamo Paper (2007)](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Lamport Clocks (1978)](https://lamport.azurewebsites.net/pubs/time-clocks.pdf)
- [Riak's multi-value registers](https://riak.com/posts/technical/vector-clocks-revisited/index.html)

## License

MIT
