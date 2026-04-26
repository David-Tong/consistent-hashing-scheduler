## Background

An Internet company Double 11 (Singles’ Day) must handle extremely large traffic volumes, and many non-business systems also experience peak loads during this time. One of these systems is the monitoring system. In order to successfully withstand this traffic surge, we need to scale up our cluster capacity. At the same time, we must design a distributed data-processing system.

A critical part of this system is the use of a **consistent hashing algorithm** to evenly distribute tasks across machines. Our central services generate a large number of identical tasks at fixed intervals (5s, 10s, or 60s).

## Programming Language

- **Java** is preferred.

## Requirements

- Support **high-concurrency** scenarios and ensure **thread safety**.
- Keep the **load among nodes as balanced as possible**.
- Each task requires different amounts of computing resources (which can be represented by values from **1 to 10**); this factor must be taken into account during task allocation.
- By default, there are **1,000 machines**. The system must support **dynamic addition and removal of machines**, while **minimizing task migration** when the machine count changes.
- When the set of machines remains unchanged, the **same type of task should be executed by the same machine as much as possible**.
