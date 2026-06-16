#!/usr/bin/env python3
"""
Master CLI - Submit a task graph for distributed execution via the broker.

Usage:
    python -m async_task_engine.cli.master --broker-host 127.0.0.1
"""
import argparse
import asyncio
import sys
import time

sys.path.insert(0, ".")

from async_task_engine.domain.entities import TaskGraph, TaskIdentifier
from async_task_engine.application.distributed import Master
from async_task_engine.infrastructure.logger import setup_logging


# Import task nodes so they auto-register
import async_task_engine.tests.sample_nodes  # noqa: F401


def build_demo_graph() -> TaskGraph:
    """Build a demo DAG for distributed execution."""
    g = TaskGraph()

    nodes = {
        name: TaskIdentifier(name=name)
        for name in [
            "fetch_user_profile",
            "fetch_user_orders",
            "fetch_product_catalog",
            "validate_orders",
            "generate_report",
        ]
    }

    for nid in nodes.values():
        g.add_node(nid)

    g.add_dependency(nodes["fetch_user_orders"], nodes["fetch_user_profile"])
    g.add_dependency(nodes["validate_orders"], nodes["fetch_user_orders"])
    g.add_dependency(nodes["validate_orders"], nodes["fetch_product_catalog"])
    g.add_dependency(nodes["generate_report"], nodes["validate_orders"])

    return g


async def run_master(broker_host: str, broker_port: int) -> None:
    master = Master(broker_host=broker_host, broker_port=broker_port)
    await master.connect()

    graph = build_demo_graph()
    print(f"\n[Master] Submitting demo DAG ({len(graph.nodes)} nodes) to broker at {broker_host}:{broker_port}")
    print(f"[Master] Waiting for worker results...\n")

    start = time.monotonic()
    results = await master.submit_graph(graph)
    elapsed = time.monotonic() - start

    print(f"\n[Master] Results collected in {elapsed:.3f}s")
    print("[Master] Task results:")
    print("  " + "-" * 60)

    for task_id, result in sorted(results.items(), key=lambda x: str(x[0])):
        icon = {
            "success": "✅", "failed": "❌", "skipped": "⏭️",
            "cancelled": "🚫", "running": "🔄", "pending": "⏳",
        }.get(result.status.value, "❓")
        result_str = (
            f"{result.result}"[:50] if result.result else str(result.error or "")[:50]
        )
        print(
            f"  {icon} {task_id}: {result.status.value} | "
            f"worker={result.worker_id} | {result_str}"
        )

    print("  " + "-" * 60)

    success_count = sum(1 for r in results.values() if r.status.value == "success")
    print(f"  ✅ Success: {success_count}/{len(results)} tasks")

    await master.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit task graph to distributed engine")
    parser.add_argument("--broker-host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--broker-port", type=int, default=9527, help="Broker port")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    setup_logging(level=20 if args.verbose else 30, json_format=False)
    asyncio.run(run_master(args.broker_host, args.broker_port))


if __name__ == "__main__":
    main()
