"""
Main Entry Point
Demonstrates the distributed task scheduler engine with a complete DAG workflow.
"""
from __future__ import annotations

import asyncio
import sys
import time

from async_task_engine.application.distributed import DistributedEngine
from async_task_engine.application.engine import AsyncTaskEngine, EngineConfig
from async_task_engine.application.metaclass import BaseTaskNode
from async_task_engine.domain.entities import TaskGraph, TaskIdentifier, TaskStatus
from async_task_engine.infrastructure.logger import setup_logging


# ── Demo Task Nodes ──
class FetchUserProfile(BaseTaskNode):
    identifier = TaskIdentifier(name="fetch_user_profile", version="1.0.0")
    description = "Fetches user profile from database"
    max_retries = 2

    async def execute(self, context):
        await asyncio.sleep(0.05)
        return {"user_id": 1, "name": "Alice", "email": "alice@example.com"}


class FetchUserOrders(BaseTaskNode):
    identifier = TaskIdentifier(name="fetch_user_orders", version="1.0.0")
    description = "Fetches user orders from API"
    max_retries = 2

    async def execute(self, context):
        await asyncio.sleep(0.08)
        profile = context.get("fetch_user_profile", {})
        return [
            {"order_id": "A1", "amount": 100.0, "user": profile.get("name")},
            {"order_id": "A2", "amount": 250.0, "user": profile.get("name")},
        ]


class FetchProductCatalog(BaseTaskNode):
    identifier = TaskIdentifier(name="fetch_product_catalog", version="1.0.0")
    description = "Fetches product catalog (independent branch)"
    max_retries = 0

    async def execute(self, context):
        await asyncio.sleep(0.04)
        return [
            {"product_id": "P1", "name": "Widget", "price": 9.99},
            {"product_id": "P2", "name": "Gadget", "price": 24.99},
        ]


class ValidateOrders(BaseTaskNode):
    identifier = TaskIdentifier(name="validate_orders", version="1.0.0")
    description = "Validates orders against catalog"
    max_retries = 1

    async def execute(self, context):
        await asyncio.sleep(0.03)
        orders = context.get("fetch_user_orders", [])
        catalog = context.get("fetch_product_catalog", [])
        catalog_ids = {p["product_id"] for p in catalog}
        return [
            {**o, "valid": True, "products_checked": len(catalog_ids)}
            for o in orders
        ]


class GenerateReport(BaseTaskNode):
    identifier = TaskIdentifier(name="generate_report", version="1.0.0")
    description = "Generates final report from validated orders"
    max_retries = 0

    async def execute(self, context):
        await asyncio.sleep(0.02)
        validated = context.get("validate_orders", [])
        total = sum(o["amount"] for o in validated)
        return {
            "report_id": "R001",
            "total_orders": len(validated),
            "total_amount": total,
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }


# ── Build Demo DAG ──
def build_demo_graph() -> TaskGraph:
    g = TaskGraph()

    # Define all nodes
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

    # Define dependencies
    g.add_dependency(nodes["fetch_user_orders"], nodes["fetch_user_profile"])
    g.add_dependency(nodes["validate_orders"], nodes["fetch_user_orders"])
    g.add_dependency(nodes["validate_orders"], nodes["fetch_product_catalog"])
    g.add_dependency(nodes["generate_report"], nodes["validate_orders"])

    return g


# ── Demo Functions ──
async def run_local_demo() -> None:
    """Run the task graph with the local (non-distributed) engine."""
    print("\n" + "=" * 60)
    print("  LOCAL ENGINE DEMO")
    print("=" * 60)

    graph = build_demo_graph()
    config = EngineConfig(max_concurrent_tasks=10, default_timeout=10.0)
    engine = AsyncTaskEngine(config=config)

    start = time.monotonic()
    results = await engine.run(graph)
    elapsed = time.monotonic() - start

    print(f"\n  Execution completed in {elapsed:.3f}s\n")
    print("  Results:")
    print("  " + "-" * 50)
    for task_id, state in sorted(results.items(), key=lambda x: str(x[0])):
        status_icon = {"success": "✅", "failed": "❌", "skipped": "⏭️",
                       "cancelled": "🚫", "running": "🔄", "pending": "⏳"}.get(
            state.status.value, "❓"
        )
        result_str = f"{state.result}"[:60] if state.result else str(state.error)[:60]
        print(f"  {status_icon} {task_id}: {state.status.value} | {result_str}")

    print("=" * 60 + "\n")


async def run_distributed_demo(num_workers: int = 3) -> None:
    """Run the task graph with the distributed master-worker engine."""
    print("\n" + "=" * 60)
    print(f"  DISTRIBUTED ENGINE DEMO ({num_workers} workers)")
    print("=" * 60)

    graph = build_demo_graph()
    engine = DistributedEngine(num_workers=num_workers, continue_on_failure=True)

    # Start worker pool
    await engine.start()
    await asyncio.sleep(0.3)  # Allow workers to initialize

    start = time.monotonic()
    results = await engine.run(graph)
    elapsed = time.monotonic() - start

    print(f"\n  Distributed execution completed in {elapsed:.3f}s\n")
    print("  Results (from distributed workers):")
    print("  " + "-" * 50)
    for task_id, result in sorted(results.items(), key=lambda x: str(x[0])):
        status_icon = {"success": "✅", "failed": "❌", "skipped": "⏭️",
                       "cancelled": "🚫"}.get(result.status.value, "❓")
        result_str = f"{result.result}"[:60] if result.result else str(result.error or "")[:60]
        print(f"  {status_icon} {task_id}: {result.status.value} | "
              f"worker={result.worker_id} | {result_str}")

    print("=" * 60 + "\n")

    await engine.stop()


async def main() -> None:
    setup_logging(level=40, json_format=False)

    print("\n" + "🚀" * 3 + "  Async Task Scheduler Engine Demo  " + "🚀" * 3)

    # Demo 1: Local execution
    await run_local_demo()

    # Demo 2: Distributed execution with 3 workers
    await run_distributed_demo(num_workers=3)

    print("\n✅ All demos completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
