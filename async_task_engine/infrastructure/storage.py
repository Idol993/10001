"""
Infrastructure: In-memory storage backend for task states.
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional

from async_task_engine.domain.entities import TaskIdentifier, TaskState, TaskStatus
from async_task_engine.interface.protocols import StorageBackend


class InMemoryStorage(StorageBackend):
    """Thread-safe in-memory storage with optional TTL-based cleanup."""

    def __init__(self, ttl_seconds: float = 3600.0) -> None:
        self._store: Dict[TaskIdentifier, tuple[TaskState, float]] = {}
        self._ttl = ttl_seconds

    async def save_state(self, task_id: TaskIdentifier, state: TaskState) -> None:
        now = time.monotonic()
        self._store[task_id] = (state, now)

    async def load_state(self, task_id: TaskIdentifier) -> Optional[TaskState]:
        entry = self._store.get(task_id)
        if entry is None:
            return None
        state, stored_at = entry
        if (time.monotonic() - stored_at) > self._ttl:
            del self._store[task_id]
            return None
        return state

    def clear_expired(self) -> int:
        """Remove expired entries and return count of removed items."""
        now = time.monotonic()
        expired = [k for k, (_, ts) in self._store.items() if (now - ts) > self._ttl]
        for k in expired:
            del self._store[k]
        return len(expired)

    async def get_all(self) -> Dict[TaskIdentifier, TaskState]:
        return {k: v for k, (v, _) in self._store.items()}
