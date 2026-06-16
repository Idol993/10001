# Async Task Scheduler Engine 🔥

A high-performance, production-grade async task orchestration engine built with Python 3.9+. Designed for complex DAG (Directed Acyclic Graph) workflow execution with robust concurrency control.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        INTERFACE LAYER                          │
│  (Protocol Contracts)                                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  protocols.py: TaskNode, StorageBackend, SchedulerEngine│   │
│  └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│                        APPLICATION LAYER                        │
│  (Core Business Logic)                                        │
│  ┌───────────────────────┐  ┌─────────────────────────────┐   │
│  │  metaclass.py          │  │  algorithms.py                │   │
│  │  NodeMeta + Registry   │  │  BloomFilter                 │   │
│  │  Auto-registration     │  │  TopologicalSorter (Kahn's)  │   │
│  └───────────────────────┘  └─────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  engine.py: AsyncTaskEngine                              │   │
│  │  • Semaphore concurrency control                         │   │
│  │  • TaskGroupCompat structured concurrency                 │   │
│  │  • BloomFilter deduplication                             │   │
│  │  • Exponential backoff retry + timeout                   │   │
│  └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│                        DOMAIN LAYER                             │
│  (Entities & Value Objects)                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  entities.py: TaskIdentifier, TaskState, TaskGraph,      │   │
│  │              TaskStatus (Enum)                           │   │
│  └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│                      INFRASTRUCTURE LAYER                       │
│  (Technical Concerns)                                         │
│  ┌───────────────────────┐  ┌─────────────────────────────┐   │
│  │  storage.py            │  │  logger.py                    │   │
│  │  InMemoryStorage       │  │  JSONFormatter               │   │
│  └───────────────────────┘  └─────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Layered Architecture (Clean Architecture)

| Layer | Responsibility | Dependencies |
|-------|---------------|-------------|
| **Interface** | Defines contracts (Protocols) | None (abstract) |
| **Application** | Business logic, algorithms, orchestration | Domain, Interface |
| **Domain** | Core entities and value objects | None (pure) |
| **Infrastructure** | Technical implementations | Interface, Domain |

---

## ✨ Core Features

### 1. 🔮 Metaclass-Based Auto-Registration (`NodeMeta`)
Every task node class is automatically registered into a global `NodeRegistry` upon definition. The metaclass validates:
- Required class variables (`identifier`, `description`, `max_retries`)
- Identifier type safety (must be `TaskIdentifier`)
- Duplicate detection (prevents naming conflicts)
- Instance caching (singleton pattern per node class)

```python
class MyTask(BaseTaskNode):
    identifier = TaskIdentifier(name="my_task", version="1.0.0")
    description = "My custom task"
    max_retries = 3
    
    async def execute(self, context):
        return {"result": 42}
# Auto-registered! No manual registration needed.
```

### 2. 🧮 Advanced Algorithms

#### BloomFilter (Probabilistic Deduplication)
- O(1) membership testing with configurable false-positive rate
- Double hashing technique for optimal distribution
- Memory efficiency: ~1 byte per item at 1% FPR

#### TopologicalSorter (DAG Resolution)
- **Kahn's Algorithm**: O(V + E) time complexity
- Cycle detection with clear error reporting
- Level-based execution planning for maximum parallelism
- Handles diamond-shaped dependency graphs correctly

### 3. ⚡ Concurrent Execution Engine

- **`asyncio.Semaphore`**: Bounds concurrent execution to avoid resource exhaustion
- **`asyncio.TaskGroup`** (Python 3.11+) / **`_TaskGroupCompat`** (3.9+): Structured concurrency with automatic error propagation
- **`asyncio.Event`**: Graceful cancellation coordination
- **Exponential backoff**: Smart retry with delay scaling (1s, 2s, 4s, ...)
- **Timeout**: Per-task configurable via `asyncio.wait_for`

### 4. 🎯 Advanced Type Annotations

- `Protocol` classes for structural typing
- `TypeVar` generics on `BloomFilter[T]`
- `ClassVar` for class-level metadata
- `frozen=True` dataclasses for immutable value objects

---

## 🚀 Quick Start

### Installation
```bash
pip install -r async_task_engine/requirements.txt
```

### Basic Usage
```python
import asyncio
from async_task_engine.application.engine import AsyncTaskEngine, EngineConfig
from async_task_engine.application.metaclass import BaseTaskNode
from async_task_engine.domain.entities import TaskGraph, TaskIdentifier

# 1. Define task nodes (auto-registered via metaclass)
class FetchData(BaseTaskNode):
    identifier = TaskIdentifier(name="fetch_data")
    description = "Fetch data from API"
    max_retries = 2
    
    async def execute(self, context):
        await asyncio.sleep(0.1)
        return {"raw_data": [1, 2, 3]}

class ProcessData(BaseTaskNode):
    identifier = TaskIdentifier(name="process_data")
    description = "Process fetched data"
    
    async def execute(self, context):
        raw = context.get("raw_data", [])
        return [x * 2 for x in raw]

# 2. Build DAG
graph = TaskGraph()
fetch_id = TaskIdentifier(name="fetch_data")
process_id = TaskIdentifier(name="process_data")
graph.add_node(fetch_id)
graph.add_node(process_id)
graph.add_dependency(process_id, fetch_id)  # process_data depends on fetch_data

# 3. Execute
async def main():
    engine = AsyncTaskEngine(EngineConfig(max_concurrent_tasks=10))
    results = await engine.run(graph, context={"raw_data": None})
    for task_id, state in results.items():
        print(f"{task_id}: {state.status.value} -> {state.result}")

asyncio.run(main())
```

---

## 📊 Performance Benchmarks

Test: 30 tasks across 3 levels (10 tasks per level), 0.02s sleep per task.

| Concurrency Level | Execution Time | Speedup vs Serial |
|-------------------|---------------|-------------------|
| **100** (unlimited) | **0.068s** | **9.31x** |
| **2** (limited) | 0.321s | 1.97x |
| **1** (serial) | 0.633s | 1.00x (baseline) |

> Run your own benchmark: `python async_task_engine/benchmark.py`

### Performance Optimization Strategies

1. **Semaphore Tuning**: Set `max_concurrent_tasks` based on your I/O throughput. For network tasks, 50-200 is optimal.
2. **BloomFilter**: Prevents redundant execution in idempotent workflows. Configure based on expected task volume.
3. **Level-Based Execution**: The topological sorter groups tasks into execution levels, maximizing parallelism while respecting dependencies.
4. **Instance Caching**: Metaclass caches node instances, avoiding repeated initialization overhead.
5. **Async Context Passing**: Shared context dict avoids serialization costs between tasks.

---

## 🧪 Testing

```bash
# Run all tests
pytest async_task_engine/tests/ -v

# Run specific test suites
pytest async_task_engine/tests/test_algorithms.py -v
pytest async_task_engine/tests/test_engine.py -v
```

### Test Coverage

| Test Suite | Coverage |
|------------|----------|
| `test_metaclass.py` | Auto-registration, instance caching, validation, duplicate detection |
| `test_algorithms.py` | BloomFilter (operations, FP rate, parameters), TopologicalSorter (DAG, cycles, levels) |
| `test_engine.py` | Concurrency, failure propagation, timeouts, cancellation, deduplication, semaphore limits |

---

## 📁 Project Structure

```
async_task_engine/
├── __init__.py
├── benchmark.py                      # Performance benchmarks
├── requirements.txt                  # Dependencies
├── domain/                           # Layer 1: Core entities
│   ├── __init__.py
│   └── entities.py                   # TaskIdentifier, TaskState, TaskGraph
├── interface/                        # Layer 2: Contracts (Protocols)
│   ├── __init__.py
│   └── protocols.py                  # TaskNode, StorageBackend protocols
├── application/                      # Layer 3: Business logic
│   ├── __init__.py
│   ├── metaclass.py                  # NodeMeta + NodeRegistry
│   ├── algorithms.py                 # BloomFilter + TopologicalSorter
│   └── engine.py                     # AsyncTaskEngine core
├── infrastructure/                   # Layer 4: Technical impls
│   ├── __init__.py
│   ├── storage.py                    # InMemoryStorage with TTL
│   └── logger.py                     # Structured JSON logging
└── tests/                            # Test suite
    ├── __init__.py
    ├── sample_nodes.py               # Test task node definitions
    ├── test_metaclass.py
    ├── test_algorithms.py
    └── test_engine.py
```

---

## 🛠️ Tech Stack

| Feature | Implementation | Version Required |
|---------|---------------|------------------|
| Concurrency | `asyncio.Semaphore` + `TaskGroup` | Python 3.9+ |
| Metaprogramming | `type` metaclass with `__init_subclass__` pattern | Python 3.9+ |
| Type System | `typing.Protocol`, `TypeVar`, `ClassVar`, dataclasses | Python 3.9+ |
| Hashing | SHA-256 + MD5 (double hashing for BloomFilter) | - |
| Testing | `pytest`, `pytest-asyncio` | - |

---

## 🔮 Future Extensions

- [ ] **RedisStorage**: Distributed state persistence across workers
- [ ] **Priority-based scheduling**: Urgency-weighted task execution
- [ ] **Dead letter queue**: Automatic quarantine of persistently failing tasks
- [ ] **Prometheus metrics**: Built-in observability for monitoring
- [ ] **Worker pool**: Multi-process execution via `multiprocessing.Queue`
- [ ] **Plugin system**: Dynamic loading of task node packages via `importlib`

---

## 📄 License

MIT License. Use freely in production environments.
