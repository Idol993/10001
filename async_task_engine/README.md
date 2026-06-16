# Async Task Scheduler Engine 🔥

A high-performance, **distributed** async task orchestration engine built with Python 3.9+. Designed for complex DAG (Directed Acyclic Graph) workflow execution with true cross-process/cross-node distributed execution via a TCP-based message broker.

---

## 🏗️ Architecture Overview

### Distributed Architecture (Cross-Process / Cross-Node)

```
┌─────────────────────┐     TCP Socket      ┌──────────────────────────────┐
│  Master Process     │◄──────────────────►│                              │
│  (submit DAG)       │                     │   TCP Broker (msg_broker)   │
└─────────────────────┘                     │   ┌────────────────────┐   │
                                            │   │  Task Queue (FIFO) │   │
┌─────────────────────┐     TCP Socket      │   └────────────────────┘   │
│  Worker Process 0   │◄──────────────────►│   ┌────────────────────┐   │
│  (execute task)     │                     │   │  Result Queue       │   │
└─────────────────────┘                     │   └────────────────────┘   │
                                            │   ┌────────────────────┐   │
┌─────────────────────┐     TCP Socket      │   │  State Store (KV)  │   │
│  Worker Process N   │◄──────────────────►│   └────────────────────┘   │
│  (execute task)     │                     │                              │
└─────────────────────┘                     └──────────────────────────────┘

  Each Worker is an INDEPENDENT Python process:
  - Separate memory space
  - Separate Python interpreter
  - Communicates ONLY via JSON-Line over TCP
  - Can run on a different machine entirely
```

### Clean Architecture (Layered)

```
┌─────────────────────────────────────────────────────────────────┐
│                       INTERFACE LAYER                          │
│  protocols.py + distributed.py (Protocol Contracts)            │
├─────────────────────────────────────────────────────────────────┤
│                       APPLICATION LAYER                        │
│  metaclass.py + algorithms.py + engine.py + distributed.py    │
├─────────────────────────────────────────────────────────────────┤
│                       DOMAIN LAYER                             │
│  entities.py (TaskIdentifier, TaskState, TaskGraph)            │
├─────────────────────────────────────────────────────────────────┤
│                       INFRASTRUCTURE LAYER                    │
│  message_queue.py (TCPBrokerServer + TCPMessageQueue)           │
│  storage.py + logger.py                                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- **No external dependencies** (zero-dependency TCP broker)

### 3 Ways to Run

#### Method 1: Single command demo (recommended first)
```bash
PYTHONPATH=. python3 async_task_engine/main.py
```
This starts everything automatically: Broker + 3 Worker subprocesses + Master

#### Method 2: Separate processes (real distributed mode)
```bash
# Terminal 1: Start broker
python -m async_task_engine.cli.broker --host 0.0.0.0 --port 9527

# Terminal 2: Start a worker (can be on a different machine)
python -m async_task_engine.cli.worker --worker-id w1 --broker-host 192.168.1.100

# Terminal 3: Start another worker
python -m async_task_engine.cli.worker --worker-id w2 --broker-host 192.168.1.100

# Terminal 4: Submit DAG from master
python -m async_task_engine.cli.master --broker-host 192.168.1.100
```

#### Method 3: From Python code
```python
import asyncio
from async_task_engine.application.distributed import Master
from async_task_engine.domain.entities import TaskGraph, TaskIdentifier
from async_task_engine.infrastructure.message_queue import TCPBrokerServer

async def main():
    # Start broker
    broker = TCPBrokerServer("127.0.0.1", 9527)
    broker_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.3)

    # Build DAG
    g = TaskGraph()
    fetch = TaskIdentifier(name="fetch_user_profile")
    process = TaskIdentifier(name="fetch_user_orders")
    g.add_node(fetch)
    g.add_node(process)
    g.add_dependency(process, fetch)

    # Submit via master
    master = Master("127.0.0.1", 9527)
    await master.connect()
    results = await master.submit_graph(g)

    await master.shutdown()
    await broker.stop()
```

---

## 🧪 Testing

```bash
# Run all 27 tests
pytest async_task_engine/tests/ -v

# Test breakdown:
# 10 tests: BloomFilter + TopologicalSorter algorithms
# 7 tests: Local async engine
# 6 tests: Distributed TCP broker + Master-Worker cross-process
# 4 tests: Metaclass auto-registration
```

### Key Distributed Tests

| Test | Description |
|------|-------------|
| `test_tcp_broker_queue_operations` | TCP enqueue/dequeue/size/clear |
| `test_tcp_broker_kv_store` | TCP key-value state store |
| `test_master_worker_via_tcp` | Master and Worker communicate via TCP |
| `test_cross_process_worker_via_subprocess` | Worker in REAL subprocess via `subprocess.Popen` |
| `test_multiple_workers_tcp` | Multiple workers competing for tasks via TCP |
| `test_distributed_failure_propagation_tcp` | Failure propagation across TCP-distributed workers |

---

## 📁 Project Structure

```
async_task_engine/
├── main.py                          # Unified demo (spawns Worker subprocesses)
├── benchmark.py                     # Performance benchmarks
├── cli/                             # Separate CLI entry points
│   ├── broker.py                    # TCP Broker server (standalone)
│   ├── worker.py                    # Worker CLI (standalone process)
│   ├── master.py                    # Master CLI (standalone process)
│   └── demo_nodes.py                # Shared demo task nodes
├── domain/
│   └── entities.py                  # Core domain entities
├── interface/
│   ├── protocols.py                  # Core protocols
│   └── distributed.py               # Distributed protocols
├── application/
│   ├── metaclass.py                 # NodeMeta + NodeRegistry
│   ├── algorithms.py                # BloomFilter + TopologicalSorter
│   ├── engine.py                    # Local async engine
│   └── distributed.py               # Master + Worker classes
├── infrastructure/
│   ├── message_queue.py             # TCPBrokerServer + TCPMessageQueue
│   ├── storage.py                   # InMemoryStorage
│   └── logger.py                    # Structured JSON logging
└── tests/
    ├── test_algorithms.py
    ├── test_engine.py
    ├── test_distributed.py          # Cross-process distributed tests
    └── test_metaclass.py
```

---

## 🔑 Core Features

1. **Metaclass Auto-Registration**: Define a class with `BaseTaskNode` → auto-registered
2. **BloomFilter Deduplication**: O(1) cross-dispatch deduplication
3. **Topological Sort**: O(V+E) DAG resolution via Kahn's Algorithm
4. **TCP Broker**: Zero-dependency cross-process message passing
5. **Master-Worker**: True distributed execution via TCP sockets
6. **Graceful Cancellation**: SIGINT/SIGTERM handling
7. **Retry Logic**: Exponential backoff per task
8. **Timeout**: Configurable per-task execution timeout

---

## 🔮 Future Extensions

- [ ] Redis-based `RedisMessageQueue` (drop-in replacement)
- [ ] Worker heartbeat and automatic re-registration
- [ ] Persistent result storage (Redis/PostgreSQL)
- [ ] Priority-based task scheduling
- [ ] Prometheus metrics for observability
- [ ] Multi-cluster federation

---

## 📄 License

MIT License.
