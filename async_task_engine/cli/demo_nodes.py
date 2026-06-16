"""
Demo task nodes for the distributed scheduler.
These are imported by both main.py and worker CLI via the metaclass auto-registration.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict

from async_task_engine.application.metaclass import BaseTaskNode, NodeRegistry
from async_task_engine.domain.entities import TaskIdentifier


class FetchUserProfile(BaseTaskNode):
    identifier = TaskIdentifier(name="fetch_user_profile", version="1.0.0")
    description = "Fetches user profile from database"
    max_retries = 2

    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.05)
        return {"user_id": 1, "name": "Alice", "email": "alice@example.com"}


class FetchUserOrders(BaseTaskNode):
    identifier = TaskIdentifier(name="fetch_user_orders", version="1.0.0")
    description = "Fetches user orders from API"
    max_retries = 2

    async def execute(self, context: Dict[str, Any]) -> list:
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

    async def execute(self, context: Dict[str, Any]) -> list:
        await asyncio.sleep(0.04)
        return [
            {"product_id": "P1", "name": "Widget", "price": 9.99},
            {"product_id": "P2", "name": "Gadget", "price": 24.99},
        ]


class ValidateOrders(BaseTaskNode):
    identifier = TaskIdentifier(name="validate_orders", version="1.0.0")
    description = "Validates orders against catalog"
    max_retries = 1

    async def execute(self, context: Dict[str, Any]) -> list:
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

    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        import time as _time
        await asyncio.sleep(0.02)
        validated = context.get("validate_orders", [])
        total = sum(o["amount"] for o in validated)
        return {
            "report_id": "R001",
            "total_orders": len(validated),
            "total_amount": total,
            "generated_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
