"""
Test fixtures and sample task nodes for testing.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict

from async_task_engine.application.metaclass import BaseTaskNode, NodeRegistry
from async_task_engine.domain.entities import TaskIdentifier


class TestTaskA(BaseTaskNode):
    identifier = TaskIdentifier(name="task_a", version="1.0.0")
    description = "Test task A - fetches data"
    max_retries = 2

    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.05)
        result = {"data": f"result_from_a_{id(self)}"}
        context["task_a_result"] = result
        return result


class TestTaskB(BaseTaskNode):
    identifier = TaskIdentifier(name="task_b", version="1.0.0")
    description = "Test task B - processes data"
    max_retries = 1

    async def execute(self, context: Dict[str, Any]) -> str:
        await asyncio.sleep(0.03)
        data = context.get("task_a_result", {})
        return f"processed: {data}"


class TestTaskC(BaseTaskNode):
    identifier = TaskIdentifier(name="task_c", version="1.0.0")
    description = "Test task C - writes output"
    max_retries = 0

    async def execute(self, context: Dict[str, Any]) -> str:
        await asyncio.sleep(0.02)
        return "output_c"


class FailingTask(BaseTaskNode):
    identifier = TaskIdentifier(name="failing_task", version="1.0.0")
    description = "A task that always fails"
    max_retries = 0

    async def execute(self, context: Dict[str, Any]) -> Any:
        raise ValueError("Intentional failure for testing")


class SlowTask(BaseTaskNode):
    identifier = TaskIdentifier(name="slow_task", version="1.0.0")
    description = "A slow task for timeout testing"
    max_retries = 0

    async def execute(self, context: Dict[str, Any]) -> Any:
        await asyncio.sleep(60.0)  # Never complete in tests
        return "should_not_reach"


class NumericTask(BaseTaskNode):
    identifier = TaskIdentifier(name="numeric_task", version="1.0.0")
    description = "Returns a computed number"
    max_retries = 0

    async def execute(self, context: Dict[str, Any]) -> int:
        await asyncio.sleep(0.01)
        return 42


# Auto-registration happens at class definition via NodeMeta metaclass
