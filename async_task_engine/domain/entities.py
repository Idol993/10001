"""
Domain Entities
Core data structures and type definitions for the task engine.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, FrozenSet, List, Optional, Set


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class TaskIdentifier:
    name: str
    version: str = "1.0.0"

    def __post_init__(self) -> None:
        if not self.name or not self.name.replace("_", "").isalnum():
            raise ValueError(f"Invalid task name: {self.name!r}")
        if not self.version or not self.version.replace(".", "").isdigit():
            raise ValueError(f"Invalid version: {self.version!r}")

    def __str__(self) -> str:
        return f"{self.name}@{self.version}"


@dataclass
class TaskState:
    status: TaskStatus = TaskStatus.PENDING
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration: float = 0.0
    error: Optional[BaseException] = None
    result: Any = None
    retry_count: int = 0

    def mark_running(self) -> None:
        self.status = TaskStatus.RUNNING
        self.start_time = time.monotonic()

    def mark_success(self, result: Any = None) -> None:
        self.status = TaskStatus.SUCCESS
        self.end_time = time.monotonic()
        self.duration = (self.end_time - self.start_time) if self.start_time else 0.0
        self.result = result

    def mark_failed(self, error: BaseException) -> None:
        self.status = TaskStatus.FAILED
        self.end_time = time.monotonic()
        self.duration = (self.end_time - self.start_time) if self.start_time else 0.0
        self.error = error


@dataclass
class TaskGraph:
    nodes: Set[TaskIdentifier] = field(default_factory=set)
    dependencies: Dict[TaskIdentifier, FrozenSet[TaskIdentifier]] = field(default_factory=dict)

    def add_node(self, node: TaskIdentifier) -> None:
        self.nodes.add(node)
        if node not in self.dependencies:
            self.dependencies[node] = frozenset()

    def add_dependency(self, node: TaskIdentifier, depends_on: TaskIdentifier) -> None:
        self.add_node(node)
        self.add_node(depends_on)
        current = self.dependencies.get(node, frozenset())
        self.dependencies[node] = frozenset(current | {depends_on})

    def get_dependents(self, node: TaskIdentifier) -> List[TaskIdentifier]:
        return [n for n in self.nodes if node in self.dependencies.get(n, frozenset())]


@dataclass
class DistributedTaskPayload:
    """Payload sent from Master to Worker for task execution."""
    task_id: TaskIdentifier
    context: Dict[str, Any] = field(default_factory=dict)
    task_graph_nodes: List[TaskIdentifier] = field(default_factory=list)
    max_retries: int = 0
    attempt: int = 0
    dispatch_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": str(self.task_id),
            "task_name": self.task_id.name,
            "task_version": self.task_id.version,
            "context": self.context,
            "max_retries": self.max_retries,
            "attempt": self.attempt,
            "dispatch_id": self.dispatch_id,
        }


@dataclass
class TaskResultPayload:
    """Result payload sent from Worker back to Master."""
    task_id: TaskIdentifier
    status: TaskStatus
    result: Any = None
    error: Optional[str] = None
    duration: float = 0.0
    worker_id: str = ""
    dispatch_id: str = ""
    succeeded: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": str(self.task_id),
            "task_name": self.task_id.name,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "duration": self.duration,
            "worker_id": self.worker_id,
            "dispatch_id": self.dispatch_id,
            "succeeded": self.succeeded,
        }
