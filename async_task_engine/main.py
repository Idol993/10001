#!/usr/bin/env python3
"""
main.py - Unified demo launcher for the distributed task scheduler.

This script demonstrates TRUE cross-process distributed execution:
1. Starts a TCP Broker in-process (for demo)
2. Spawns N Worker subprocesses via subprocess.Popen (each a REAL separate Python process)
3. Runs Master to submit DAG tasks via TCP to the broker
4. Collects and displays results
5. Cleans up all processes

Each Worker process is truly independent - it has its own memory space,
imports its own Python interpreter, and communicates ONLY via TCP sockets
to the shared broker.
"""
from __future__ import annotations

import asyncio
import subprocess
import sys
import time
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from async_task_engine.application.distributed import Master
from async_task_engine.domain.entities import TaskGraph, TaskIdentifier
from async_task_engine.infrastructure.logger import setup_logging
from async_task_engine.infrastructure.message_queue import TCPBrokerServer

# Import demo nodes to trigger metaclass registration
import async_task_engine.cli.demo_nodes  # noqa: F401


BROKER_HOST = "127.0.0.1"
BROKER_PORT = 9527


def build_demo_graph() -> TaskGraph:
    """Build a multi-level DAG for distributed execution."""
    g = TaskGraph()
    names = [
        "fetch_user_profile", "fetch_user_orders", "fetch_product_catalog",
        "validate_orders", "generate_report",
    ]
    nodes = {name: TaskIdentifier(name=name) for name in names}
    for nid in nodes.values():
        g.add_node(nid)
    g.add_dependency(nodes["fetch_user_orders"], nodes["fetch_user_profile"])
    g.add_dependency(nodes["validate_orders"], nodes["fetch_user_orders"])
    g.add_dependency(nodes["validate_orders"], nodes["fetch_product_catalog"])
    g.add_dependency(nodes["generate_report"], nodes["validate_orders"])
    return g


async def run_demo(num_workers: int = 3) -> None:
    """Run the full cross-process distributed demo."""
    broker = TCPBrokerServer(host=BROKER_HOST, port=BROKER_PORT)
    broker_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.3)

    python_exe = sys.executable
    workers: list[subprocess.Popen] = []

    print(f"\n{'='*60}")
    print(f"  Spawning {num_workers} Worker subprocesses...")
    print(f"{'='*60}")

    for i in range(num_workers):
        worker_id = f"worker_{i}"
        proc = subprocess.Popen(
            [
                python_exe, "-m", "async_task_engine.cli.worker",
                "--worker-id", worker_id,
                "--broker-host", BROKER_HOST,
                "--broker-port", str(BROKER_PORT),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "PYTHONPATH": os.getcwd()},
        )
        workers.append(proc)
        print(f"  Started {worker_id} (PID: {proc.pid})")

    await asyncio.sleep(0.5)

    graph = build_demo_graph()
    master = Master(broker_host=BROKER_HOST, broker_port=BROKER_PORT)
    await master.connect()

    print(f"\n{'='*60}")
    print(f"  Submitting DAG to distributed workers...")
    print(f"{'='*60}")

    start = time.monotonic()
    results = await master.submit_graph(graph)
    elapsed = time.monotonic() - start

    print(f"\n{'='*60}")
    print(f"  Results ({elapsed:.3f}s)")
    print(f"{'='*60}")

    for task_id, result in sorted(results.items(), key=lambda x: str(x[0])):
        icon = {"success": "✅", "failed": "❌", "skipped": "⏭️", "cancelled": "🚫"}.get(
            result.status.value, "❓"
        )
        data = f"{result.result}"[:50] if result.result else str(result.error or "")[:50]
        print(f"  {icon} {task_id}: {result.status.value} | "
              f"process={result.worker_id} | {data}")

    success_count = sum(1 for r in results.values() if r.status.value == "success")
    print(f"\n  ✅ {success_count}/{len(results)} tasks succeeded")

    print(f"\n{'='*60}")
    print(f"  Terminating Worker subprocesses...")
    print(f"{'='*60}")
    for proc in workers:
        proc.terminate()
        try:
            stdout, stderr = proc.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    await master.shutdown()
    await broker.stop()
    broker_task.cancel()
    try:
        await broker_task
    except asyncio.CancelledError:
        pass

    print(f"\n  ✅ Cleanup complete. All {num_workers} worker processes terminated.")
    print(f"{'='*60}\n")


async def main() -> None:
    setup_logging(level=40, json_format=False)
    print("\n" + "🚀" * 3 + "  Distributed Task Scheduler  " + "🚀" * 3)
    print("  True cross-process execution via TCP broker")
    await run_demo(num_workers=3)
    print("✅ Distributed demo completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
