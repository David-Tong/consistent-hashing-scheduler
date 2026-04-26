# High-Level Architecture

## Goals Recap

- Handle huge task volumes (Double 11 scale)
- Thread-safe, low-latency
- Even load distribution considering task weight (1–10)
- Dynamic node add/remove with minimal task remapping
- Task affinity: same task type → same machine when topology unchanged

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
+------------------------+
| Task Router Service    |
|                        |
| - Consistent Hash Ring |
| - Weighted Scheduling  |
| - Task Affinity Cache  |
+----------+-------------+
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

# Key Design Decisions

## Consistent Hashing with Virtual Nodes

- Each physical machine maps to multiple virtual nodes
- Supports:
  - Smooth load balancing
  - Minimal migration when nodes change

- Virtual node count can be capacity-based

## Weighted Task Scheduling

- Each task has a weight (1–10)
- Node capacity tracked as:

```
currentLoad + taskWeight <= nodeCapacity
```

## Task Affinity (Sticky Tasks)

- Same task type → same hash key
- Hash key example:

```
hash(taskType)
```

- Ensures consistency if ring unchanged

# Component Diagram

```
+--------------------------------------------------+
|                  TaskRouter                      |
|--------------------------------------------------|
| + route(Task) : Node                             |
| + rebalance()                                   |
|--------------------------------------------------|
|  Uses:                                           |
|   - ConsistentHashRing                           |
|   - LoadBalancer                                 |
|   - AffinityManager                              |
+------------------------+-------------------------+
                         |
         +---------------+---------------+
         |                               |
+---------------------+        +--------------------+
| ConsistentHashRing  |        | LoadBalancer       |
|---------------------|        |--------------------|
| + addNode()         |        | + selectNode()     |
| + removeNode()      |        | + updateLoad()     |
| + getNode(key)      |        +--------------------+
+---------------------+
         |
+---------------------+
| AffinityManager     |
|---------------------|
| + getPreferredNode()|
| + cacheMapping()    |
+---------------------+
```

# Core Components & Responsibilities

## TaskRouter (Core Entry Point)

- Responsibility
  - Main orchestration layer
  - Maps incoming tasks to worker nodes

- Key Logic
  - Try task affinity
  - Fallback to consistent hashing
  - Validate node capacity

```
public interface TaskRouter {
    WorkerNode route(Task task);
}
```

## ConsistentHashRing

- Responsibility
  - Maintain sorted hash ring
  - Minimal remapping when topology changes

- Design
  - TreeMap<Long, VirtualNode>
  - Thread-safe with ReadWriteLock

```
public interface ConsistentHashRing {
    void addNode(WorkerNode node);
    void removeNode(String nodeId);
    WorkerNode getNode(String hashKey);
}
```

## VirtualNode

```
public class VirtualNode {
    private final String virtualId;
    private final WorkerNode physicalNode;
}
```

- Number of virtual nodes ∝ machine capacity

## LoadBalancer (Weight-Aware)

- Responsibility
  - Ensure node capacity not exceeded
  - Balance weighted tasks

```
public interface LoadBalancer {
    boolean canAccept(WorkerNode node, int taskWeight);
    void onTaskAssigned(WorkerNode node, int taskWeight);
    void onTaskFinished(WorkerNode node, int taskWeight);
}
```

- Implementation Notes
  - Use AtomicInteger for current load
  - Avoid global locks

## AffinityManager

- Responsibility
  - Preserve task-type → node mapping
  - Improves cache locality and predictability

```
public interface AffinityManager {
    Optional<WorkerNode> getPreferredNode(String taskType);
    void bind(String taskType, WorkerNode node);
}
```

- Storage Options
  - Local ConcurrentHashMap
  - Optional Redis for multi-router consistency

## WorkerNode

```

public class WorkerNode {
    private final String nodeId;
    private final int capacity; // total weight allowed
    private final AtomicInteger currentLoad;
}
```

# API Design (Java-Friendly)

## Task Ingress API

```

public interface TaskIngressApi {
    void submit(Task task);
}
```

```

public class Task {
    private String taskId;
    private String taskType;
    private int weight; // 1-10
}
```

## Node Management API

```

public interface NodeRegistry {
    void registerNode(WorkerNode node);
    void deregisterNode(String nodeId);
    List<WorkerNode> listNodes();
}
```

# Concurrency & Thread Safety Strategy

The system is designed for **high-concurrency, read-heavy workloads**. Its concurrency model follows the principle of **lock-free or lightweight locks on the read path, and centralized control on the write path**.

## Concurrency Strategy Overview

| Component            | Strategy                    | Rationale                                                           |
| -------------------- | --------------------------- | ------------------------------------------------------------------- |
| Consistent Hash Ring | `ReadWriteLock`             | Node lookup is frequent (reads), topology changes are rare (writes) |
| Node Load Tracking   | `AtomicInteger`             | Lock-free, accurate tracking of current load                        |
| Task Routing         | Lock-free / CAS-based       | Avoids blocking on the hot routing path                             |
| Node Add/Remove      | Write lock + lazy rebalance | Limits the impact of topology changes                               |

## Key Design Considerations

- **ConsistentHashRing**
  - Uses `ReentrantReadWriteLock`
  - Read lock for node lookup operations (high concurrency, low latency)
  - Write lock only when adding or removing nodes (low frequency)

- **WorkerNode Load Tracking**
  - `AtomicInteger` is used to maintain `currentLoad`
  - Task assignment and completion update load via atomic operations
  - Eliminates lock contention under high concurrency

- **Task Routing Path**
  - Primarily stateless and read-only
  - Combines consistent hashing with local task-affinity caching
  - Avoids synchronized blocks in the routing hot path

- **Node Topology Changes**
  - Node addition and removal are considered control-plane operations
  - Protected by write locks on the hash ring
  - Only tasks in affected hash ranges are remapped (lazy rebalancing)

## Resulting Benefits

- Supports extremely high concurrency
- Avoids global locks and bottlenecks
- Minimizes performance impact during topology changes
- Aligns with production-grade Java concurrency best practices

# Dynamic Scaling & Minimal Migration

- Adding a Node
  - Create virtual nodes
  - Insert into hash ring
  - Only tasks in affected hash ranges migrate

- Removing a Node
  - Remove virtual nodes from the ring
  - Re-route tasks mapped to removed ranges only
  - Ensures O(k / n) migration instead of full reshuffle

# Extensibility

- Pluggable hash functions
- Alternative load metrics (CPU, memory, custom)
- Multi-cluster and region-aware routing
- Failure detection and retry mechanisms

# Summary

This design:

- Supports thousands of worker nodes
- Handles high concurrency safely
- Achieves balanced load distribution
- Preserves task affinity
- Minimizes task migration
- Is cleanly implementable in Java
- Suitable for production systems and system-design interviews
