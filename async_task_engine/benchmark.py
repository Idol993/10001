"""
Performance Benchmark
Compares serial execution vs. concurrent execution vs. engine execution.
"""
from __future__ import annotations

import asyncio
import time
import sys

sys.path.insert(0, ".")

from async_task_engine.application.algorithms import TopologicalSorter
from async_task_engine.application.engine import AsyncTaskEngine, EngineConfig
from async_task_engine.application.metaclass import BaseTaskNode
from async_task_engine.domain.entities import TaskGraph, TaskIdentifier


def make_dag(n_tasks: int, depth: int = 3) -> TaskGraph:
    """Create a layered DAG with specified width per level."""
    g = TaskGraph()
    levels: list[list[TaskIdentifier]] = []

    tasks_per_level = max(1, n_tasks // depth)
    total = 0
    for lvl in range(depth):
        level: list[TaskIdentifier] = []
        for i in range(tasks_per_level):
            tid = TaskIdentifier(name=f"l{lvl}_t{i}")
            g.add_node(tid)
            level.append(tid)
            total += 1
        levels.append(level)

    # Connect levels: each node in level k depends on one node in level k-1
    for lvl in range(1, len(levels)):
        for node_idx, node in enumerate(levels[lvl]):
            dep_idx = node_idx % len(levels[lvl - 1])
            g.add_dependency(node, levels[lvl - 1][dep_idx])

    return g


# Dynamically generate benchmark task nodes
def generate_benchmark_tasks(n_tasks: int) -> None:
    """Dynamically register task nodes for benchmarking."""
    for lvl in range(3):
        for i in range(max(1, n_tasks // 3)):
            name = f"l{lvl}_t{i}"
            tid = TaskIdentifier(name=name)

            # Create a dynamic subclass
            type(
                f"Bench_{name}",
                (BaseTaskNode,),
                {
                    "identifier": tid,
                    "description": f"Benchmark task {name}",
                    "max_retries": 0,
                    "execute": lambda self, context, _sleep=0.02: _do_bench_work(context, _sleep),
                },
            )


async def _do_bench_work(context, sleep_time: float) -> int:
    """Simulate async work."""
    await asyncio.sleep(sleep_time)
    return 42


async def run_serial(tasks: list, dependencies: dict) -> None:
    """Run tasks serially (one after another)."""
    completed: set = set()
    for task_id in tasks:
        await asyncio.sleep(0.02)
        completed.add(task_id)


async def run_benchmark(n_tasks: int = 30) -> None:
    """Run a comprehensive benchmark."""
    print(f"\n{'='*60}")
    print(f"  Async Task Engine Performance Benchmark")
    print(f"  Tasks: {n_tasks} across 3 levels")
    print(f"{'='*60}\n")

    generate_benchmark_tasks(n_tasks)

    g = make_dag(n_tasks)
    levels = TopologicalSorter.get_execution_levels(g)
    print(f"Graph: {len(g.nodes)} nodes, {len(levels)} levels")
    for i, level in enumerate(levels):
        print(f"  Level {i}: {len(level)} tasks")

    # Test 1: Engine with high concurrency
    print(f"\n--- Test 1: Engine (concurrency=100) ---")
    config_fast = EngineConfig(max_concurrent_tasks=100)
    engine_fast = AsyncTaskEngine(config=config_fast)
    start = time.monotonic()
    results_fast = await engine_fast.run(g)
    elapsed_fast = time.monotonic() - start
    success_fast = sum(1 for s in results_fast.values() if s.status.value == "success")
    print(f"  Time: {elapsed_fast:.4f}s")
    print(f"  Success: {success_fast}/{len(results_fast)}")

    # Test 2: Engine with limited concurrency
    print(f"\n--- Test 2: Engine (concurrency=2) ---")
    config_limited = EngineConfig(max_concurrent_tasks=2)
    engine_limited = AsyncTaskEngine(config=config_limited)
    start = time.monotonic()
    results_limited = await engine_limited.run(g)
    elapsed_limited = time.monotonic() - start
    success_limited = sum(1 for s in results_limited.values() if s.status.value == "success")
    print(f"  Time: {elapsed_limited:.4f}s")
    print(f"  Success: {success_limited}/{len(results_limited)}")

    # Test 3: Engine with concurrency=1 (essentially serial)
    print(f"\n--- Test 3: Engine (concurrency=1, serial) ---")
    config_serial = EngineConfig(max_concurrent_tasks=1)
    engine_serial = AsyncTaskEngine(config=config_serial)
    start = time.monotonic()
    results_serial = await engine_serial.run(g)
    elapsed_serial = time.monotonic() - start
    success_serial = sum(1 for s in results_serial.values() if s.status.value == "success")
    print(f"  Time: {elapsed_serial:.4f}s")
    print(f"  Success: {success_serial}/{len(results_serial)}")

    # Summary
    print(f"\n{'='*60}")
    print(f"  BENCHMARK SUMMARY")
    print(f"{'='*60}")
    print(f"  Concurrency=100: {elapsed_fast:.4f}s")
    print(f"  Concurrency=2:   {elapsed_limited:.4f}s")
    print(f"  Concurrency=1:   {elapsed_serial:.4f}s")
    print(f"\n  Speedup (100 vs 1x): {elapsed_serial/max(elapsed_fast,0.001):.2f}x")
    print(f"  Speedup (2 vs 1):   {elapsed_serial/max(elapsed_limited,0.001):.2f}x")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(run_benchmark(30))
