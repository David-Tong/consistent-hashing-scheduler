# Algorithm Design Document

## Load‑Aware Task Scheduling with Type Affinity Using HRW

---

## 1. Overview

This document describes the design of a **distributed task scheduling algorithm** that achieves:

- **Soft task-type affinity** (tasks of the same type tend to run on similar machines),
- **Global load balancing** across a large shared resource pool,
- **High resource utilization** without static partitioning,
- **Stability under cluster changes** (machine add/remove).

The core idea is to combine **Rendezvous Hashing (HRW)** for _deterministic preference ordering_ with **dynamic, load-aware scheduling** for final placement.

---

## 2. Problem Statement

We consider a system with the following properties:

- A cluster of **N machines** (e.g., 1000 servers), forming a **single shared resource pool**.
- A stream of tasks, where each task has:
  - A **task type** (logical category).
  - A **resource cost** (e.g., integer 1–10).
- Each machine has limited capacity and a measurable current load.

### Key Challenges

1. **Type Affinity vs Load Balancing**  
   Tasks of the same type should prefer similar machines, but not at the cost of overloading them.

2. **Avoiding Resource Fragmentation**  
   The system must not create per-type resource pools or static partitions.

3. **Handling Load Skew**  
   Different task types may have vastly different arrival rates.

4. **Stability**  
   Machine membership changes should not cause large-scale remapping.

---

## 3. Design Goals

### Functional Goals

- Provide **stable but flexible task-type affinity**.
- Incorporate **task cost** and **machine load** into scheduling.
- Support **large-scale clusters** efficiently.

### Non-Goals

- Strong (hard) binding between task types and specific machines.
- Perfect locality at the expense of global fairness.
- Frequent reshuffling based on instantaneous load.

---

## 4. High-Level Approach

The scheduling decision is intentionally split into two layers:

- Deterministic Preference Generation (HRW)
- Dynamic Load-Aware Selection
  > HRW determines _which machines a task type prefers_.  
  > The scheduler decides _where the task actually runs_.

This separation is critical to balancing stability and adaptability.

---

## 5. Core Algorithm

### 5.1 Global Machine Pool

All machines belong to a single global set:

Machines = {M1, M2, ..., MN}

There are **no per-type machine pools**.

---

### 5.2 Type-Based Preference Ordering (HRW)

For a given task type `T`, compute a score for every machine:

score(T, Mi) = Hash(T, machine_id_i)

Machines are sorted by descending score to form a **deterministic ranking**:

RankedMachines(T) = [M7, M23, M4, M88, ...]

#### Properties of HRW in This Design

- Deterministic: Same type, same machine set → same ranking.
- Minimal disruption: Adding/removing a machine affects only a subset of types.
- No extra structures: No hash ring or virtual nodes required.

Importantly, this ranking represents **preference order**, not ownership.

---

### 5.3 Progressive Candidate Selection

Instead of selecting a fixed candidate set, the scheduler uses **adaptive expansion**:

1. Start with the top `K` machines in the ranking.
2. Attempt to place the task on a suitable machine in this set.
3. If no machine can accept the task:
   - Expand the window to top `2K`, then `3K`, etc.
4. Continue until placement succeeds or a system-level limit is reached.

This ensures:

- Light task types remain localized.
- Heavy task types automatically spread across more machines.

---

### 5.4 Load-Aware Final Placement

Within the current candidate window:

- Exclude machines without sufficient remaining capacity.
- Compute projected load for each candidate:

projected_load = (current_load + task_cost) / capacity

- Select the machine with the lowest projected load.

This step ensures fairness and avoids hotspots.

---

## 6. Load Balancing Analysis

### Why Static Candidate Sets Fail

If each task type were permanently limited to a fixed set of machines:

- High-volume types would overload their machines.
- Low-volume types would leave machines underutilized.

### Why This Design Works

- **Soft constraints**: Preference does not imply exclusivity.
- **Adaptive expansion**: Load pressure increases candidate scope.
- **Statistical overlap**: Different task types’ preference rankings overlap across the cluster.

As a result, unused capacity is naturally reclaimed by heavier workloads.

---

## 7. Handling Extreme Scenarios

### Hot Task Type (Burst Traffic)

- Initial preference machines saturate.
- Scheduler expands candidate window.
- Load distributes across more of the cluster.

### Cold Task Type

- Uses a small subset of machines.
- Does not reserve or block unused capacity.

### Machine Join / Leave

- HRW guarantees only limited reshuffling.
- No global rebalancing required.

---

## 8. Key Properties Summary

| Property                 | Result |
| ------------------------ | ------ |
| Shared resource pool     | ✅ Yes |
| Soft type affinity       | ✅ Yes |
| Load-aware scheduling    | ✅ Yes |
| Handles workload skew    | ✅ Yes |
| Minimal disruption       | ✅ Yes |
| Scales to large clusters | ✅ Yes |

---

## 9. Design Invariants (Must Be Explicit)

To avoid pathological behavior, the following rules must hold:

1. **Candidate sets are soft constraints**, never hard partitions.
2. **Load and capacity override type preference** when necessary.
3. **HRW ranking is stable and slow-changing**; real-time load must not directly reshuffle rankings aggressively.

---

## 10. Conclusion

This algorithm frames task scheduling as a **two-phase decision process**:

1. **Deterministic preference generation** using HRW to provide type affinity and stability.
2. **Dynamic, load-aware execution selection** to ensure fairness and high utilization.

By clearly separating determinism from adaptability, the system achieves a balance that neither pure hashing nor pure dynamic scheduling can provide on its own.
