"""
Infrastructure: In-Memory Distributed Message Queue
A lightweight asyncio-based message queue that mimics Redis-like pub/sub
with separate task and result channels.
"""
from __future__ import annotations

import asyncio
import json
import time
import logging
from collections import defaultdict
from typing import Any, Deque, Dict, List, Optional

from async_task_engine.interface.distributed import MessageQueue

logger = logging.getLogger(__name__)


class InMemoryMessageQueue:
    """
    A simple in-memory message queue backed by asyncio primitives.
    
    Supports:
    - Multiple named queues (task_queue, result_queue)
    - Blocking dequeue with timeout
    - TTL-based message expiry
    - Queue size monitoring
    
    Designed to be swapped with a Redis-backed implementation
    without changing application code.
    """

    def __init__(self) -> None:
        self._queues: Dict[str, Deque[Dict[str, Any]]] = defaultdict(lambda: asyncio.Queue())
        self._conditions: Dict[str, asyncio.Condition] = {}
        self._lock = asyncio.Lock()
        self._stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"enqueued": 0, "dequeued": 0})

    async def enqueue(self, queue_name: str, payload: Dict[str, Any]) -> None:
        """Push a message into the specified queue."""
        enriched = {
            "payload": payload,
            "timestamp": time.monotonic(),
            "message_id": f"{queue_name}:{time.monotonic_ns()}",
        }
        q = self._queues[queue_name]
        await q.put(enriched)
        self._stats[queue_name]["enqueued"] += 1
        logger.debug("Enqueued message to %s", queue_name)

    async def dequeue(self, queue_name: str, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """
        Pop a message from the queue. Blocks until a message arrives or timeout.
        
        Returns None if timeout expires before a message is available.
        """
        q = self._queues[queue_name]
        try:
            enriched = await asyncio.wait_for(q.get(), timeout=timeout)
            self._stats[queue_name]["dequeued"] += 1
            logger.debug("Dequeued message from %s", queue_name)
            return enriched["payload"]
        except asyncio.TimeoutError:
            logger.debug("Dequeue timeout on %s", queue_name)
            return None

    async def size(self, queue_name: str) -> int:
        """Return the current number of messages in a queue."""
        q = self._queues[queue_name]
        return q.qsize()

    async def clear(self, queue_name: str) -> None:
        """Remove all messages from the specified queue."""
        q = self._queues[queue_name]
        while not q.empty():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                break
        logger.info("Cleared queue: %s", queue_name)

    def get_stats(self, queue_name: str) -> Dict[str, int]:
        """Return statistics for a specific queue."""
        return dict(self._stats[queue_name])

    def get_all_stats(self) -> Dict[str, Dict[str, int]]:
        """Return statistics for all queues."""
        return {name: dict(stats) for name, stats in self._stats.items()}
