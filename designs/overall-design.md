# High-Level Architecture

## Goals Recap

- Handle huge task volumes (Double 11 scale)
- Thread-safe, low-latency
- Even load distribution considering task weight (1–10)
- Dynamic node add/remove with minimal task remapping
- Soft task affinity (not strict binding)
- High resource utilization without partitioning

## Architecture Overview

```
+---------------------+
| Central Task Source |
| (per 5s/10s/60s)    |
+----------+----------+
           |
           v
+------------------------+
| Task Ingress API       |
| - Validation           |
| - Batching             |
+----------+-------------+
           |
           v
+-------------------------------+
| Task Router Service           |
|                               |
| - HRW Preference Engine       |
| - Candidate Expander          |
| - Load-Aware Selector         |
+----------+--------------------+
           |
           v
+------------------------+
| Worker Node Pool       |
|                        |
| - Task Executor        |
| - Resource Metrics     |
+----------+-------------+
           |
           v
+------------------------+
| Monitoring & Metrics   |
| - Node load            |
| - Task latency         |
+------------------------+
```

# Core Design Principles

## 1. Two-Phase Scheduling Model

The system separates scheduling into two distinct phases:

### Phase 1: Deterministic Preference (HRW)

For each task type T and node Mi, compute:

```
score(T, Mi) = hash(T, Mi)
```

Sort all nodes by score (descending):

```
RankedNodes(T) = [M7, M23, M4, ...]
```

This produces a deterministic preference order.

This approach ensures:

- All schedulers agree on the same ordering
- Minimal disruption when nodes change
- No need for hash rings or virtual nodes

### Phase 2: Load-Aware Placement

- From preferred nodes, select based on real-time load:

```
projectedLoad = (currentLoad + taskWeight) / capacity
```

- Choose node with lowest projected load.

Ensures:

- Fair distribution
- Avoids hotspots
- Maximizes utilization

## 2. Soft Task Affinity

- Tasks of the same type share the same HRW ranking
- No fixed binding to a single node

Result:

- Strong locality when load is low
- Automatic spreading when load increases

## 3. Progressive Candidate Expansion

Instead of selecting a single node:

```
Top K → if no capacity → Top 2K → Top 3K → ...
```

Benefits:

- Prevents overload
- Adapts to workload skew
- Maintains affinity when possible

## 4. Global Shared Resource Pool

- All nodes belong to a single pool:

```
Nodes = {M1, M2, ..., MN}
```

- No per-type partitioning
  Avoids fragmentation and improves utilization

# Key Design Decisions

## HRW (Rendezvous Hashing)

- Each node gets a score per task type
- Highest score = highest preference
- Next highest = fallback

Properties:

- Uniform distribution across nodes
- Minimal remapping on node changes
- Simple and stateless (no ring, no tokens)

## Load-Aware Scheduling

- Capacity-aware admission control
- Weight-based scheduling (task weight 1–10)
- Prevents node overload

## Adaptive Scheduling

- Candidate set expands dynamically
- No fixed allocation per task type
- Handles skew automatically

# Component Diagram

```
+--------------------------------------------------+
|                  TaskRouter                      |
|--------------------------------------------------|
| + route(Task) : WorkerNode                      |
|--------------------------------------------------|
| Uses:                                           |
|  - HRWPreferenceEngine                          |
|  - CandidateSelector                            |
|  - LoadEvaluator                                |
+------------------------+-------------------------+
                         |
         +---------------+---------------+
         |                               |
+--------------------------+   +----------------------+
| HRWPreferenceEngine      |   | LoadEvaluator        |
|--------------------------|   |----------------------|
| + rank(taskType)         |   | + canAccept()        |
|                          |   | + projectedLoad()   |
+--------------------------+   +----------------------+
         |
+--------------------------+
| CandidateSelector        |
|--------------------------|
| + expand(K)              |
+--------------------------+
```

# Core Components & Responsibilities

## TaskRouter (Core Entry Point)

Responsibility

- Orchestrates scheduling decision

Logic

```
route(task):
  ranked = HRW.rank(task.type)

  for window in [K, 2K, 3K...]:
      candidates = ranked[0:window]

      feasible = filter(nodes with capacity)

      if feasible not empty:
          return node with min(projectedLoad)

  return fallback
```

## HRWPreferenceEngine

```
public interface HRWPreferenceEngine {
    List<WorkerNode> rank(String taskType);
}
```

- Stateless
- Deterministic
- No caching required

## CandidateSelector

```
public interface CandidateSelector {
    List<WorkerNode> expand(List<WorkerNode> ranked, int k);
}
```

- Controls progressive expansion
- Ensures adaptive scheduling

## LoadEvaluator

```
public interface LoadEvaluator {
    boolean canAccept(WorkerNode node, int weight);
    double projectedLoad(WorkerNode node, int weight);
}
```

## WorkerNode

```
public class WorkerNode {
    private final String nodeId;
    private final int capacity;
    private final AtomicInteger currentLoad;
}
```

# Concurrency & Thread Safety Strategy

| Component     | Strategy               | Description                                                                                         |
| ------------- | ---------------------- | --------------------------------------------------------------------------------------------------- |
| HRW ranking   | Stateless / lock-free  | Deterministic computation with no shared mutable state                                              |
| Load tracking | AtomicInteger          | Uses atomic operations for thread-safe counters without locks :contentReference[oaicite:0]{index=0} |
| Task routing  | Lock-free              | Avoids blocking in the hot path for high throughput                                                 |
| Node updates  | Copy-on-write snapshot | Immutable snapshots ensure safe concurrent reads                                                    |

# Dynamic Scaling & Minimal Migration

## Node Add

- Automatically participates in ranking
- Only affects tasks where it ranks highly

## Node Remove

- Tasks fall back to next-ranked node
- No global reshuffle required

HRW guarantees minimal disruption and smooth failover

# Key Properties Summary

| Property                 | Result |
| ------------------------ | ------ |
| Shared resource pool     | ✅ Yes |
| Soft type affinity       | ✅ Yes |
| Load-aware scheduling    | ✅ Yes |
| No resource partitioning | ✅ Yes |
| Handles workload skew    | ✅ Yes |
| Minimal disruption       | ✅ Yes |
| High utilization         | ✅ Yes |

# Summary

This design transforms task scheduling into a two-phase decision system:

## 1. Deterministic preference (HRW)

- Stable, consistent, minimal disruption

## 2. Dynamic load-aware selection

- Adaptive, fair, and efficient

By separating determinism from adaptability, the system achieves:

- Better scalability than static hashing
- Better stability than purely dynamic scheduling
- Simpler implementation than hash-ring-based designs
