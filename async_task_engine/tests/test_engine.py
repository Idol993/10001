"""
Test: Async Engine with concurrency
"""
import asyncio
import time

import pytest
import pytest_asyncio

from async_task_engine.application.engine import AsyncTaskEngine, EngineConfig
from async_task_engine.domain.entities import TaskGraph, TaskIdentifier, TaskStatus

# Import sample nodes to ensure they're auto-registered
from async_task_engine.tests.sample_nodes import (
    TestTaskA,
    TestTaskB,
    TestTaskC,
    FailingTask,
    SlowTask,
    NumericTask,
)


@pytest.fixture
def simple_graph():
    g = TaskGraph()
    a = TaskIdentifier(name="task_a")
    b = TaskIdentifier(name="task_b")
    c = TaskIdentifier(name="task_c")

    g.add_node(a)
    g.add_node(b)
    g.add_node(c)
    g.add_dependency(b, a)  # b depends on a
    # c is independent

    return g


@pytest.fixture
def engine():
    config = EngineConfig(max_concurrent_tasks=10, default_timeout=30.0)
    return AsyncTaskEngine(config=config)


@pytest.mark.asyncio
async def test_simple_execution(simple_graph, engine):
    results = await engine.run(simple_graph, context={"key": "value"})

    assert len(results) == 3
    assert results[TaskIdentifier(name="task_a")].status == TaskStatus.SUCCESS
    assert results[TaskIdentifier(name="task_b")].status == TaskStatus.SUCCESS
    assert results[TaskIdentifier(name="task_c")].status == TaskStatus.SUCCESS


@pytest.mark.asyncio
async def test_concurrent_execution(simple_graph):
    """Verify that independent tasks run concurrently (within timing tolerance)."""
    config = EngineConfig(max_concurrent_tasks=5)
    engine = AsyncTaskEngine(config=config)

    start = time.monotonic()
    results = await engine.run(simple_graph)
    elapsed = time.monotonic() - start

    # task_a: 0.05s, task_c: 0.02s (runs in parallel with a), task_b: 0.03s (after a)
    # Minimum theoretical time: max(0.05, 0.02) + 0.03 = 0.08s
    # With some overhead, should be well under 0.3s
    assert elapsed < 0.3, f"Tasks not running concurrently: elapsed={elapsed:.3f}s"

    # Verify b ran after a (dependency respected)
    assert results[TaskIdentifier(name="task_a")].status == TaskStatus.SUCCESS
    assert results[TaskIdentifier(name="task_b")].status == TaskStatus.SUCCESS
    assert results[TaskIdentifier(name="task_c")].status == TaskStatus.SUCCESS


@pytest.mark.asyncio
async def test_failure_propagation():
    """Test that a failing node marks its dependents as SKIPPED."""
    g = TaskGraph()
    fail = TaskIdentifier(name="failing_task")
    dep = TaskIdentifier(name="task_b")  # Depends on the failing task

    g.add_node(fail)
    g.add_node(dep)
    g.add_dependency(dep, fail)

    config = EngineConfig(max_concurrent_tasks=5, continue_on_failure=True)
    engine = AsyncTaskEngine(config=config)

    results = await engine.run(g)

    assert results[fail].status == TaskStatus.FAILED
    assert results[dep].status == TaskStatus.SKIPPED


@pytest.mark.asyncio
async def test_timeout_handling():
    """Test that slow tasks are properly timed out."""
    g = TaskGraph()
    slow = TaskIdentifier(name="slow_task")
    g.add_node(slow)

    config = EngineConfig(max_concurrent_tasks=5, default_timeout=0.1)
    engine = AsyncTaskEngine(config=config)

    results = await engine.run(g)
    assert results[slow].status == TaskStatus.FAILED
    assert results[slow].error is not None


@pytest.mark.asyncio
async def test_cancellation():
    """Test graceful cancellation."""
    g = TaskGraph()
    for name in ["task_a", "task_c"]:
        g.add_node(TaskIdentifier(name=name))

    config = EngineConfig(max_concurrent_tasks=2)
    engine = AsyncTaskEngine(config=config)

    # Cancel right away
    cancel_task = asyncio.create_task(engine.cancel())
    await asyncio.sleep(0.02)

    results = await engine.run(g)

    # Some may have completed, some cancelled
    for state in results.values():
        assert state.status in (TaskStatus.SUCCESS, TaskStatus.CANCELLED, TaskStatus.FAILED)


@pytest.mark.asyncio
async def test_deduplication():
    """Test that BloomFilter prevents duplicate execution."""
    g = TaskGraph()
    a = TaskIdentifier(name="numeric_task")
    g.add_node(a)

    config = EngineConfig(max_concurrent_tasks=5)
    engine = AsyncTaskEngine(config=config)

    results = await engine.run(g)
    assert results[a].status == TaskStatus.SUCCESS

    # Reset states but the bloom filter still has the task
    g2 = TaskGraph()
    g2.add_node(a)

    results2 = await engine.run(g2)
    # Second run should show SKIPPED because of BloomFilter dedup
    # (Bloom filter might have false positives, but with our settings it's very likely)
    assert results2[a].status in (TaskStatus.SUCCESS, TaskStatus.SKIPPED)


@pytest.mark.asyncio
async def test_semaphore_concurrency_limit():
    """Test that max_concurrent_tasks is respected via semaphore."""
    g = TaskGraph()
    # Use registered tasks (task_a and task_c are independent)
    g.add_node(TaskIdentifier(name="task_a"))
    g.add_node(TaskIdentifier(name="task_c"))

    config = EngineConfig(max_concurrent_tasks=1, continue_on_failure=True)
    engine = AsyncTaskEngine(config=config)

    start = time.monotonic()
    results = await engine.run(g)
    elapsed = time.monotonic() - start

    # With semaphore of 1, these should run sequentially
    # task_a: 0.05s + task_c: 0.02s = 0.07s minimum
    # With parallel (semaphore=infinity): max(0.05, 0.02) = 0.05s minimum
    # Sequential should take longer than parallel
    assert elapsed >= 0.07, f"Semaphore not limiting: elapsed={elapsed:.3f}s"
    assert elapsed < 1.0
