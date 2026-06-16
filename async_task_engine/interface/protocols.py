"""
Interface Layer: Protocol definitions (abstract contracts).
Defines contracts for task nodes, storage backends, and schedulers.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, ClassVar, Dict, Optional, Protocol, Type, runtime_checkable

from async_task_engine.domain.entities import TaskGraph, TaskIdentifier, TaskState


@runtime_checkable
class TaskNode(Protocol):
    """Protocol that all task nodes must implement."""
    identifier: ClassVar[TaskIdentifier]
    description: ClassVar[str]
    max_retries: ClassVar[int]

    async def execute(self, context: Dict[str, Any]) -> Any:
        """Execute the task logic."""
        ...


@runtime_checkable
class StorageBackend(Protocol):
    """Protocol for task execution persistence."""

    async def save_state(self, task_id: TaskIdentifier, state: TaskState) -> None:
        ...

    async def load_state(self, task_id: TaskIdentifier) -> Optional[TaskState]:
        ...


@runtime_checkable
class SchedulerEngine(Protocol):
    """Protocol for the scheduler orchestration engine."""

    async def run(self, graph: TaskGraph, context: Dict[str, Any] = None) -> Dict[TaskIdentifier, TaskState]:
        ...

    async def cancel(self) -> None:
        ...
