"""
Distributed Master-Worker Engine
Implements a Master-Worker architecture where:
- Master: partitions the DAG into execution levels and dispatches tasks to workers
- Workers: receive tasks from queue, execute them, and report results back

This enables true horizontal scaling: workers can run on separate processes/machines
and communicate via the shared message queue.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Set

from async_task_engine.application.algorithms import BloomFilter, TopologicalSorter
from async_task_engine.application.metaclass import BaseTaskNode, NodeRegistry
from async_task_engine.domain.entities import (
    DistributedTaskPayload,
    TaskGraph,
    TaskIdentifier,
    TaskResultPayload,
    TaskStatus,
)
from async_task_engine.interface.distributed import MasterNode, WorkerNode
from async_task_engine.infrastructure.message_queue import InMemoryMessageQueue

logger = logging.getLogger(__name__)


class Worker:
    """
    Worker node: consumes task payloads from queue and executes them.
    
    Lifecycle:
    1. Start → begins listening on task queue
    2. On message → looks up registered node class → executes with context
    3. Reports result back via result queue
    4. Supports graceful shutdown
    """

    def __init__(
        self,
        worker_id: str,
        queue: InMemoryMessageQueue,
        task_queue_name: str,
        result_queue_name: str,
        poll_timeout: float = 1.0,
    ) -> None:
        self.worker_id = worker_id
        self._queue = queue
        self._task_queue = task_queue_name
        self._result_queue = result_queue_name
        self._poll_timeout = poll_timeout
        self._running = False
        self._task = asyncio.current_task()
        self._stats = {"processed": 0, "successful": 0, "failed": 0}

    async def start(self) -> None:
        """Start the worker loop. Runs until stop() is called."""
        self._running = True
        logger.info("Worker %s started and listening on %s", self.worker_id, self._task_queue)

        while self._running:
            message = await self._queue.dequeue(self._task_queue, timeout=self._poll_timeout)
            if message is None:
                continue

            try:
                payload = DistributedTaskPayload(
                    task_id=TaskIdentifier(
                        name=message["task_name"],
                        version=message.get("task_version", "1.0.0"),
                    ),
                    context=message.get("context", {}),
                    max_retries=message.get("max_retries", 0),
                    attempt=message.get("attempt", 0),
                    dispatch_id=message.get("dispatch_id", ""),
                )

                result = await self.execute_task(payload)
                await self._queue.enqueue(
                    self._result_queue,
                    result.to_dict(),
                )
            except Exception as e:
                logger.error("Worker %s encountered error: %s", self.worker_id, e)

    async def stop(self) -> None:
        """Gracefully stop the worker."""
        self._running = False
        logger.info("Worker %s stopped. Stats: %s", self.worker_id, self._stats)

    async def execute_task(self, payload: DistributedTaskPayload) -> TaskResultPayload:
        """Execute a single task and return the result."""
        self._stats["processed"] += 1
        task_id = payload.task_id

        node_type = NodeRegistry.get_node(task_id)
        if node_type is None:
            self._stats["failed"] += 1
            return TaskResultPayload(
                task_id=task_id,
                status=TaskStatus.FAILED,
                error=f"No registered node for {task_id}",
                worker_id=self.worker_id,
                dispatch_id=payload.dispatch_id,
                succeeded=False,
            )

        instance = node_type()
        max_retries = getattr(instance, "max_retries", payload.max_retries)
        last_error: Optional[BaseException] = None
        start = time.monotonic()

        for attempt in range(max_retries + 1):
            try:
                result = await asyncio.wait_for(
                    instance.execute(payload.context),
                    timeout=30.0,
                )
                duration = time.monotonic() - start
                self._stats["successful"] += 1
                return TaskResultPayload(
                    task_id=task_id,
                    status=TaskStatus.SUCCESS,
                    result=result,
                    duration=duration,
                    worker_id=self.worker_id,
                    dispatch_id=payload.dispatch_id,
                    succeeded=True,
                )
            except Exception as e:
                last_error = e
                logger.warning(
                    "Worker %s: task %s attempt %d failed: %s",
                    self.worker_id, task_id, attempt + 1, e,
                )
                if attempt < max_retries:
                    await asyncio.sleep(min(2 ** attempt, 10))

        duration = time.monotonic() - start
        self._stats["failed"] += 1
        return TaskResultPayload(
            task_id=task_id,
            status=TaskStatus.FAILED,
            error=str(last_error),
            duration=duration,
            worker_id=self.worker_id,
            dispatch_id=payload.dispatch_id,
            succeeded=False,
        )


class Master:
    """
    Master node: orchestrates distributed DAG execution.
    
    Strategy:
    1. Receive a task graph
    2. Topologically sort into levels
    3. Dispatch level-by-level: enqueue all tasks in current level
    4. Wait for results (with timeout)
    5. On success → merge results into context, advance to next level
    6. On failure → skip dependents or abort based on config
    """

    def __init__(
        self,
        queue: InMemoryMessageQueue,
        task_queue_name: str = "tasks",
        result_queue_name: str = "results",
        continue_on_failure: bool = False,
        level_timeout: float = 120.0,
    ) -> None:
        self._queue = queue
        self._task_queue = task_queue_name
        self._result_queue = result_queue_name
        self._continue_on_failure = continue_on_failure
        self._level_timeout = level_timeout
        self._context: Dict[str, Any] = {}
        self._running = False
        self._dispatch_id_counter = 0

    async def submit_graph(self, graph: TaskGraph) -> Dict[TaskIdentifier, TaskResultPayload]:
        """
        Submit a full DAG for distributed execution.
        
        Returns mapping of task identifier to result payload.
        """
        self._context = {}
        self._dispatch_id_counter += 1
        dispatch_id = f"dispatch_{self._dispatch_id_counter}_{uuid.uuid4().hex[:8]}"

        logger.info("Master: starting dispatch %s with %d nodes", dispatch_id, len(graph.nodes))

        # Phase 1: compute execution plan
        levels = TopologicalSorter.get_execution_levels(graph)
        results: Dict[TaskIdentifier, TaskResultPayload] = {}
        bloom = BloomFilter[TaskIdentifier](expected_items=len(graph.nodes) * 2)

        # Phase 2: execute level by level
        for level_idx, level in enumerate(levels):
            logger.info("Master: dispatching level %d (%d tasks)", level_idx, len(level))

            level_payloads: Dict[TaskIdentifier, DistributedTaskPayload] = {}
            for node in level:
                if node in bloom:
                    logger.debug("Master: skipping already-dispatched %s", node)
                    results[node] = TaskResultPayload(
                        task_id=node,
                        status=TaskStatus.SKIPPED,
                        worker_id="master",
                        dispatch_id=dispatch_id,
                    )
                    continue

                payload = DistributedTaskPayload(
                    task_id=node,
                    context=self._context,
                    max_retries=getattr(
                        NodeRegistry.get_node(node),
                        "max_retries",
                        0,
                    ) if NodeRegistry.get_node(node) else 0,
                    dispatch_id=dispatch_id,
                )
                level_payloads[node] = payload
                await self._queue.enqueue(self._task_queue, payload.to_dict())
                bloom.add(node)

            if not level_payloads:
                continue

            # Phase 3: collect results for this level
            collected: Dict[TaskIdentifier, TaskResultPayload] = {}
            expected = set(level_payloads.keys())
            deadline = time.monotonic() + self._level_timeout

            while collected.keys() != expected and time.monotonic() < deadline:
                result_msg = await self._queue.dequeue(self._result_queue, timeout=1.0)
                if result_msg is None:
                    continue

                task_id = TaskIdentifier(
                    name=result_msg["task_name"],
                    version=result_msg.get("task_version", "1.0.0"),
                )
                result = TaskResultPayload(
                    task_id=task_id,
                    status=TaskStatus(result_msg["status"]),
                    result=result_msg.get("result"),
                    error=result_msg.get("error"),
                    duration=result_msg.get("duration", 0.0),
                    worker_id=result_msg.get("worker_id", "unknown"),
                    dispatch_id=result_msg.get("dispatch_id", ""),
                    succeeded=result_msg.get("succeeded", False),
                )

                # Only count results for this dispatch
                if result.dispatch_id == dispatch_id and task_id in expected:
                    collected[task_id] = result
                    results[task_id] = result

                    if result.succeeded and result.result is not None:
                        # Merge result into context for downstream tasks
                        self._context[task_id.name] = result.result

            # Phase 4: handle failures
            failed = [
                tid for tid, r in collected.items()
                if r.status == TaskStatus.FAILED
            ]
            if failed:
                logger.warning("Master: level %d had failures: %s", level_idx, failed)
                if not self._continue_on_failure:
                    logger.error("Master: aborting dispatch %s due to failure", dispatch_id)
                    # Mark remaining as cancelled
                    for node in graph.nodes:
                        if node not in results:
                            results[node] = TaskResultPayload(
                                task_id=node,
                                status=TaskStatus.CANCELLED,
                                worker_id="master",
                                dispatch_id=dispatch_id,
                            )
                    break

        logger.info(
            "Master: dispatch %s complete. Results: %s",
            dispatch_id,
            {str(k): v.status.value for k, v in results.items()},
        )
        return results

    async def shutdown(self) -> None:
        self._running = False
        logger.info("Master: shut down")


class DistributedEngine:
    """
    High-level distributed execution engine.
    
    Encapsulates Master + Worker coordination and provides
    a simple API for submitting task graphs to a worker pool.
    """

    def __init__(
        self,
        num_workers: int = 3,
        continue_on_failure: bool = False,
    ) -> None:
        self._queue = InMemoryMessageQueue()
        self._master = Master(
            queue=self._queue,
            continue_on_failure=continue_on_failure,
        )
        self._workers: List[Worker] = []
        self._worker_tasks: List[asyncio.Task] = []
        self._num_workers = num_workers

    async def start(self) -> None:
        """Start the worker pool."""
        self._workers = [
            Worker(
                worker_id=f"worker_{i}",
                queue=self._queue,
                task_queue_name="tasks",
                result_queue_name="results",
                poll_timeout=0.5,
            )
            for i in range(self._num_workers)
        ]
        self._worker_tasks = [
            asyncio.create_task(w.start()) for w in self._workers
        ]
        logger.info("DistributedEngine: started %d workers", self._num_workers)

    async def run(self, graph: TaskGraph) -> Dict[TaskIdentifier, TaskResultPayload]:
        """Submit a graph and wait for all results."""
        return await self._master.submit_graph(graph)

    async def stop(self) -> None:
        """Gracefully stop all workers and the master."""
        for worker in self._workers:
            await worker.stop()
        for task in self._worker_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await self._master.shutdown()
        logger.info("DistributedEngine: stopped")
