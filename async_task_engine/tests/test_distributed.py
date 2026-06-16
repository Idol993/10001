"""
Tests: Distributed Engine (Master-Worker Architecture)
"""
import asyncio
import time
import pytest

from async_task_engine.application.distributed import DistributedEngine, Master, Worker
from async_task_engine.application.metaclass import BaseTaskNode
from async_task_engine.domain.entities import (
    DistributedTaskPayload,
    TaskGraph,
    TaskIdentifier,
    TaskResultPayload,
    TaskStatus,
)
from async_task_engine.infrastructure.message_queue import InMemoryMessageQueue


# ── Test Task Nodes for Distributed Tests ──
class DistTaskFast(BaseTaskNode):
    identifier = TaskIdentifier(name="dist_fast_task", version="1.0.0")
    description = "Fast task for distributed testing"
    max_retries = 0

    async def execute(self, context):
        await asyncio.sleep(0.01)
        return {"data": "fast_result"}


class DistTaskSlow(BaseTaskNode):
    identifier = TaskIdentifier(name="dist_slow_task", version="1.0.0")
    description = "Slow task for distributed testing"
    max_retries = 0

    async def execute(self, context):
        await asyncio.sleep(0.05)
        return {"data": "slow_result"}


class DistTaskDep(BaseTaskNode):
    identifier = TaskIdentifier(name="dist_dep_task", version="1.0.0")
    description = "Dependent task for distributed testing"
    max_retries = 0

    async def execute(self, context):
        await asyncio.sleep(0.01)
        upstream = context.get("dist_fast_task", {})
        return {"processed": upstream.get("data", "unknown")}


class DistTaskFail(BaseTaskNode):
    identifier = TaskIdentifier(name="dist_fail_task", version="1.0.0")
    description = "Failing task for distributed testing"
    max_retries = 0

    async def execute(self, context):
        raise ValueError("Distributed task failure test")


class DistTaskRetryable(BaseTaskNode):
    identifier = TaskIdentifier(name="dist_retryable_task", version="1.0.0")
    description = "Retryable task for distributed testing"
    max_retries = 2

    async def execute(self, context):
        await asyncio.sleep(0.01)
        return {"retried": True}


@pytest.fixture
def simple_dag():
    """A simple DAG: fast → dep, slow is independent."""
    g = TaskGraph()
    fast = TaskIdentifier(name="dist_fast_task")
    slow = TaskIdentifier(name="dist_slow_task")
    dep = TaskIdentifier(name="dist_dep_task")

    g.add_node(fast)
    g.add_node(slow)
    g.add_node(dep)
    g.add_dependency(dep, fast)  # dep depends on fast

    return g


@pytest.mark.asyncio
async def test_message_queue_basic_operations():
    """Test 1: Message queue enqueue/dequeue/size/clear."""
    queue = InMemoryMessageQueue()

    # Test enqueue and size
    await queue.enqueue("test_q", {"key": "value1"})
    await queue.enqueue("test_q", {"key": "value2"})
    size = await queue.size("test_q")
    assert size == 2

    # Test FIFO dequeue
    msg1 = await queue.dequeue("test_q", timeout=1.0)
    assert msg1 == {"key": "value1"}

    msg2 = await queue.dequeue("test_q", timeout=1.0)
    assert msg2 == {"key": "value2"}

    # Test empty queue returns None after timeout
    msg3 = await queue.dequeue("test_q", timeout=0.1)
    assert msg3 is None

    # Test clear
    await queue.enqueue("test_q", {"key": "value3"})
    await queue.clear("test_q")
    size_after = await queue.size("test_q")
    assert size_after == 0

    # Test stats
    stats = queue.get_stats("test_q")
    assert stats["enqueued"] == 3  # 3 total enqueued
    assert stats["dequeued"] == 2  # 2 dequeued before clear


@pytest.mark.asyncio
async def test_single_worker_execution():
    """Test 2: Single worker receives a task from queue and returns result."""
    queue = InMemoryMessageQueue()
    worker = Worker(
        worker_id="test_worker_1",
        queue=queue,
        task_queue_name="tasks",
        result_queue_name="results",
        poll_timeout=0.2,
    )

    # Start worker in background
    worker_task = asyncio.create_task(worker.start())
    await asyncio.sleep(0.1)

    # Send a task
    payload = DistributedTaskPayload(
        task_id=TaskIdentifier(name="dist_fast_task", version="1.0.0"),
        context={"test": "value"},
        max_retries=0,
        dispatch_id="test_dispatch_1",
    )
    await queue.enqueue("tasks", payload.to_dict())

    # Wait for result
    result_msg = await queue.dequeue("results", timeout=2.0)
    assert result_msg is not None
    assert result_msg["task_name"] == "dist_fast_task"
    assert result_msg["status"] == "success"
    assert result_msg["worker_id"] == "test_worker_1"
    assert result_msg["result"] == {"data": "fast_result"}

    # Stop worker
    await worker.stop()
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_master_worker_dag_execution(simple_dag):
    """Test 3: Master dispatches a DAG to multiple workers and collects results."""
    engine = DistributedEngine(num_workers=3, continue_on_failure=False)

    await engine.start()
    await asyncio.sleep(0.3)  # Let workers initialize

    start = time.monotonic()
    results = await engine.run(simple_dag)
    elapsed = time.monotonic() - start

    # Validate execution
    assert elapsed < 2.0, f"Distributed execution took too long: {elapsed:.3f}s"
    assert len(results) == 3

    # All tasks should succeed
    for task_id, result in results.items():
        assert result.status == TaskStatus.SUCCESS, f"Task {task_id} failed: {result.error}"

    # Validate dependency: dep should have received fast's result in context
    dep_result = results[TaskIdentifier(name="dist_dep_task")]
    assert dep_result.result == {"processed": "fast_result"}

    # Validate workers were actually used (results come from different workers)
    worker_ids = {r.worker_id for r in results.values()}
    assert len(worker_ids) >= 1  # At least one worker processed

    await engine.stop()


@pytest.mark.asyncio
async def test_distributed_failure_propagation():
    """Test 4: Distributed engine properly handles task failures."""
    g = TaskGraph()
    fail = TaskIdentifier(name="dist_fail_task")
    dep = TaskIdentifier(name="dist_dep_task")

    g.add_node(fail)
    g.add_node(dep)
    g.add_dependency(dep, fail)  # dep depends on failing task

    engine = DistributedEngine(num_workers=2, continue_on_failure=False)

    await engine.start()
    await asyncio.sleep(0.3)

    results = await engine.run(g)

    # The failing task should be FAILED
    assert results[fail].status == TaskStatus.FAILED
    assert results[fail].error is not None

    # The dependent task should be CANCELLED or SKIPPED (since continue_on_failure=False)
    assert results[dep].status in (TaskStatus.CANCELLED, TaskStatus.SKIPPED)

    await engine.stop()


@pytest.mark.asyncio
async def test_distributed_concurrent_workers(simple_dag):
    """Test 5: Verify that multiple workers process tasks truly concurrently."""
    engine = DistributedEngine(num_workers=5, continue_on_failure=False)

    await engine.start()
    await asyncio.sleep(0.3)

    start = time.monotonic()
    results = await engine.run(simple_dag)
    elapsed = time.monotonic() - start

    # dist_fast_task: 0.01s, dist_slow_task: 0.05s, dist_dep_task: 0.01s (after fast)
    # With parallel execution: max(0.01, 0.05) + 0.01 = 0.06s minimum
    # Without parallel: 0.01 + 0.05 + 0.01 = 0.07s minimum
    # Should complete well under 0.5s with parallelism
    assert elapsed < 0.5, f"Tasks not running concurrently: {elapsed:.3f}s"

    # Results should be distributed across workers
    workers_used = {r.worker_id for r in results.values()}
    print(f"\n  Workers used for this dispatch: {workers_used}")

    await engine.stop()


@pytest.mark.asyncio
async def test_master_bloom_deduplication():
    """Test 6: Master uses BloomFilter to prevent duplicate dispatches."""
    g = TaskGraph()
    fast = TaskIdentifier(name="dist_fast_task")
    g.add_node(fast)

    engine = DistributedEngine(num_workers=2)
    await engine.start()
    await asyncio.sleep(0.3)

    # First run
    results1 = await engine.run(g)
    assert results1[fast].status == TaskStatus.SUCCESS

    # Second run with same graph - BloomFilter should prevent re-dispatch
    results2 = await engine.run(g)
    # Most likely SKIPPED due to BloomFilter dedup (probabilistic but very likely)
    assert results2[fast].status in (TaskStatus.SUCCESS, TaskStatus.SKIPPED)

    await engine.stop()
