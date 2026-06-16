"""
Tests: Distributed Engine (Master-Worker via TCP Broker)
"""
import asyncio
import subprocess
import sys
import time
import os

import pytest

from async_task_engine.application.distributed import Master, Worker
from async_task_engine.application.metaclass import BaseTaskNode
from async_task_engine.domain.entities import (
    DistributedTaskPayload,
    TaskGraph,
    TaskIdentifier,
    TaskResultPayload,
    TaskStatus,
)
from async_task_engine.infrastructure.message_queue import (
    TCPBrokerServer,
    TCPMessageQueue,
)


# ── Test Task Nodes ──
class DistTaskFast(BaseTaskNode):
    identifier = TaskIdentifier(name="dist_fast_task", version="1.0.0")
    description = "Fast task"
    max_retries = 0

    async def execute(self, context):
        await asyncio.sleep(0.01)
        return {"data": "fast_result"}


class DistTaskSlow(BaseTaskNode):
    identifier = TaskIdentifier(name="dist_slow_task", version="1.0.0")
    description = "Slow task"
    max_retries = 0

    async def execute(self, context):
        await asyncio.sleep(0.05)
        return {"data": "slow_result"}


class DistTaskDep(BaseTaskNode):
    identifier = TaskIdentifier(name="dist_dep_task", version="1.0.0")
    description = "Dependent task"
    max_retries = 0

    async def execute(self, context):
        await asyncio.sleep(0.01)
        upstream = context.get("dist_fast_task", {})
        return {"processed": upstream.get("data", "unknown")}


class DistTaskFail(BaseTaskNode):
    identifier = TaskIdentifier(name="dist_fail_task", version="1.0.0")
    description = "Failing task"
    max_retries = 0

    async def execute(self, context):
        raise ValueError("Intentional failure")


# ── Fixtures ──
@pytest.fixture
async def tcp_broker():
    """Start a TCP broker for testing."""
    broker = TCPBrokerServer(host="127.0.0.1", port=9528)
    task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.2)
    yield broker
    await broker.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.fixture
def simple_dag():
    g = TaskGraph()
    fast = TaskIdentifier(name="dist_fast_task")
    slow = TaskIdentifier(name="dist_slow_task")
    dep = TaskIdentifier(name="dist_dep_task")
    g.add_node(fast)
    g.add_node(slow)
    g.add_node(dep)
    g.add_dependency(dep, fast)
    return g


# ── Tests ──
@pytest.mark.asyncio
async def test_tcp_broker_queue_operations(tcp_broker):
    """Test 1: TCP broker enqueue/dequeue/size operations."""
    client = TCPMessageQueue("127.0.0.1", 9528)
    await client.connect()

    # Enqueue messages
    await client.enqueue("test_q", {"key": "value1"})
    await client.enqueue("test_q", {"key": "value2"})
    size = await client.size("test_q")
    assert size == 2

    # FIFO dequeue
    msg1 = await client.dequeue("test_q", timeout=1.0)
    assert msg1 == {"key": "value1"}

    msg2 = await client.dequeue("test_q", timeout=1.0)
    assert msg2 == {"key": "value2"}

    # Empty queue returns None
    msg3 = await client.dequeue("test_q", timeout=0.1)
    assert msg3 is None

    await client.disconnect()


@pytest.mark.asyncio
async def test_tcp_broker_kv_store(tcp_broker):
    """Test 2: TCP broker key-value store."""
    client = TCPMessageQueue("127.0.0.1", 9528)
    await client.connect()

    await client.set_key("state:test", {"value": 42})
    result = await client.get_key("state:test")
    assert result == {"value": 42}

    # Nonexistent key
    missing = await client.get_key("state:nonexistent")
    assert missing is None

    await client.disconnect()


@pytest.mark.asyncio
async def test_master_worker_via_tcp(tcp_broker, simple_dag):
    """Test 3: Master and Worker communicate via TCP broker (in-process but TCP-based)."""
    # Start worker (connects via TCP)
    worker = Worker(worker_id="test_w1", broker_host="127.0.0.1", broker_port=9528)
    worker_task = asyncio.create_task(worker.start())
    await asyncio.sleep(0.3)

    # Start master (connects via TCP)
    master = Master(broker_host="127.0.0.1", broker_port=9528)
    await master.connect()

    results = await master.submit_graph(simple_dag)

    assert len(results) == 3
    for r in results.values():
        assert r.status == TaskStatus.SUCCESS

    # Validate dependency result was merged
    dep_result = results[TaskIdentifier(name="dist_dep_task")]
    assert dep_result.result == {"processed": "fast_result"}
    assert dep_result.worker_id == "test_w1"

    await master.shutdown()
    await worker.stop()
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_cross_process_worker_via_subprocess(tcp_broker):
    """Test 4: Worker runs in a REAL subprocess and communicates via TCP."""
    python_exe = sys.executable
    proc = subprocess.Popen(
        [
            python_exe, "-m", "async_task_engine.cli.worker",
            "--worker-id", "subprocess_w",
            "--broker-host", "127.0.0.1",
            "--broker-port", "9528",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "PYTHONPATH": os.getcwd()},
    )

    # Wait for subprocess worker to connect
    await asyncio.sleep(1.0)

    master = Master(broker_host="127.0.0.1", broker_port=9528)
    await master.connect()

    g = TaskGraph()
    g.add_node(TaskIdentifier(name="dist_fast_task"))

    results = await master.submit_graph(g)
    task_result = results[TaskIdentifier(name="dist_fast_task")]

    assert task_result.status == TaskStatus.SUCCESS
    assert task_result.worker_id == "subprocess_w"
    assert task_result.result == {"data": "fast_result"}

    await master.shutdown()
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.mark.asyncio
async def test_multiple_workers_tcp(tcp_broker):
    """Test 5: Multiple workers compete for tasks via TCP broker."""
    workers = []
    worker_tasks = []
    for i in range(3):
        w = Worker(
            worker_id=f"w{i}",
            broker_host="127.0.0.1",
            broker_port=9528,
        )
        workers.append(w)
        worker_tasks.append(asyncio.create_task(w.start()))

    await asyncio.sleep(0.5)

    master = Master(broker_host="127.0.0.1", broker_port=9528)
    await master.connect()

    g = TaskGraph()
    g.add_node(TaskIdentifier(name="dist_fast_task"))
    g.add_node(TaskIdentifier(name="dist_slow_task"))
    g.add_node(TaskIdentifier(name="dist_fail_task"))

    results = await master.submit_graph(g)

    # fast and slow should succeed, fail should fail
    assert results[TaskIdentifier(name="dist_fast_task")].status == TaskStatus.SUCCESS
    assert results[TaskIdentifier(name="dist_slow_task")].status == TaskStatus.SUCCESS
    assert results[TaskIdentifier(name="dist_fail_task")].status == TaskStatus.FAILED

    await master.shutdown()
    for w, t in zip(workers, worker_tasks):
        await w.stop()
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_distributed_failure_propagation_tcp(tcp_broker):
    """Test 6: Failures propagate correctly in distributed mode via TCP."""
    w = Worker(worker_id="fail_w", broker_host="127.0.0.1", broker_port=9528)
    wt = asyncio.create_task(w.start())
    await asyncio.sleep(0.3)

    master = Master(
        broker_host="127.0.0.1",
        broker_port=9528,
        continue_on_failure=False,
    )
    await master.connect()

    g = TaskGraph()
    fail = TaskIdentifier(name="dist_fail_task")
    dep = TaskIdentifier(name="dist_dep_task")
    g.add_node(fail)
    g.add_node(dep)
    g.add_dependency(dep, fail)

    results = await master.submit_graph(g)

    assert results[fail].status == TaskStatus.FAILED
    assert results[dep].status == TaskStatus.CANCELLED

    await master.shutdown()
    await w.stop()
    wt.cancel()
    try:
        await wt
    except asyncio.CancelledError:
        pass
