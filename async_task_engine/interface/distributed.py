"""
Interface: Distributed Message Queue Protocols
Defines contracts for message-based communication between Master and Worker.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, List, Optional, Protocol, runtime_checkable

from async_task_engine.domain.entities import DistributedTaskPayload, TaskResultPayload


@runtime_checkable
class MessageQueue(Protocol):
    """Protocol for a distributed message queue."""

    async def enqueue(self, queue_name: str, payload: Dict[str, Any]) -> None:
        ...

    async def dequeue(self, queue_name: str, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        ...

    async def size(self, queue_name: str) -> int:
        ...

    async def clear(self, queue_name: str) -> None:
        ...


@runtime_checkable
class WorkerNode(Protocol):
    """Protocol for a Worker that executes tasks."""

    worker_id: str

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    async def execute_task(self, payload: DistributedTaskPayload) -> TaskResultPayload:
        ...


@runtime_checkable
class MasterNode(Protocol):
    """Protocol for a Master that dispatches tasks."""

    async def submit_graph(self, graph_dict: Dict[str, Any]) -> str:
        ...

    async def get_results(self, dispatch_id: str) -> Dict[str, Any]:
        ...

    async def shutdown(self) -> None:
        ...
