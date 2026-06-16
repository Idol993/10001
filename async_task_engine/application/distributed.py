"""
Distributed Master-Worker Engine
True cross-process distributed execution using TCP-based message broker.

Usage:
    # Start broker (separate terminal / process)
    python -m async_task_engine.cli.broker

    # Start workers (separate processes)
    python -m async_task_engine.cli.worker --worker-id w1
    python -m async_task_engine.cli.worker --worker-id w2
    python -m async_task_engine.cli.worker --worker-id w3

    # Submit task graph from master
    python -m async_task_engine.cli.master
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
from async_task_engine.infrastructure.message_queue import TCPMessageQueue

logger = logging.getLogger(__name__)


class Worker:
    """
    Worker node: connects to TCP broker and executes tasks.
    
    Can run as a standalone process (via CLI) or embedded in tests.
    """

    def __init__(
        self,
        worker_id: str,
        broker_host: str = "127.0.0.1",
        broker_port: int = 9527,
        task_queue_name: str = "tasks",
        result_queue_name: str = "results",
        poll_timeout: float = 1.0,
    ) -> None:
        self.worker_id = worker_id
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._task_queue = task_queue_name
        self._result_queue = result_queue_name
        self._poll_timeout = poll_timeout
        self._queue: Optional[TCPMessageQueue] = None
        self._running = False
        self._stats = {"processed": 0, "successful": 0, "failed": 0}

    async def start(self) -> None:
        """Connect to broker and start listening for tasks."""
        self._queue = TCPMessageQueue(self._broker_host, self._broker_port)
        await self._queue.connect()
        self._running = True
        logger.info(
            "Worker %s connected to broker at %s:%s",
            self.worker_id, self._broker_host, self._broker_port,
        )
        print(f"[Worker {self.worker_id}] Connected to broker, waiting for tasks...")

        while self._running:
            try:
                message = await self._queue.dequeue(self._task_queue, timeout=self._poll_timeout)
                if message is None:
                    continue

                result = await self.execute_task_payload(message)
                await self._queue.enqueue(self._result_queue, result.to_dict())
                print(f"[Worker {self.worker_id}] Completed {result.task_id}: {result.status.value}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Worker %s error: %s", self.worker_id, e)

    async def stop(self) -> None:
        """Gracefully disconnect from broker."""
        self._running = False
        if self._queue:
            try:
                await self._queue.disconnect()
            except Exception:
                pass
        logger.info("Worker %s stopped. Stats: %s", self.worker_id, self._stats)
        print(f"[Worker {self.worker_id}] Stopped. Processed: {self._stats}")

    async def execute_task_payload(self, message: Dict[str, Any]) -> TaskResultPayload:
        """Deserialize message and execute the task."""
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
        return await self.execute_task(payload)

    async def execute_task(self, payload: DistributedTaskPayload) -> TaskResultPayload:
        """Execute a task with retry logic."""
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
    Master node: connects to TCP broker and orchestrates DAG execution.
    
    Dispatches tasks level-by-level, collects results from distributed workers.
    """

    def __init__(
        self,
        broker_host: str = "127.0.0.1",
        broker_port: int = 9527,
        task_queue_name: str = "tasks",
        result_queue_name: str = "results",
        continue_on_failure: bool = False,
        level_timeout: float = 120.0,
    ) -> None:
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._task_queue = task_queue_name
        self._result_queue = result_queue_name
        self._continue_on_failure = continue_on_failure
        self._level_timeout = level_timeout
        self._queue: Optional[TCPMessageQueue] = None
        self._context: Dict[str, Any] = {}
        self._running = False
        self._dispatch_id_counter = 0

    async def connect(self) -> None:
        """Connect to the TCP broker."""
        self._queue = TCPMessageQueue(self._broker_host, self._broker_port)
        await self._queue.connect()
        self._running = True
        logger.info("Master connected to broker at %s:%s", self._broker_host, self._broker_port)

    async def submit_graph(self, graph: TaskGraph) -> Dict[TaskIdentifier, TaskResultPayload]:
        """Submit a DAG for distributed execution via the broker."""
        if not self._queue:
            raise RuntimeError("Master not connected to broker. Call connect() first.")

        self._context = {}
        self._dispatch_id_counter += 1
        dispatch_id = f"dispatch_{self._dispatch_id_counter}_{uuid.uuid4().hex[:8]}"

        logger.info("Master: starting dispatch %s with %d nodes", dispatch_id, len(graph.nodes))
        print(f"\n[Master] Dispatch {dispatch_id}: {len(graph.nodes)} nodes")

        levels = TopologicalSorter.get_execution_levels(graph)
        results: Dict[TaskIdentifier, TaskResultPayload] = {}
        bloom = BloomFilter[TaskIdentifier](expected_items=len(graph.nodes) * 2)

        for level_idx, level in enumerate(levels):
            print(f"[Master] Level {level_idx}: dispatching {len(level)} tasks...")

            level_payloads: Dict[TaskIdentifier, DistributedTaskPayload] = {}
            for node in level:
                if node in bloom:
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
                        NodeRegistry.get_node(node), "max_retries", 0
                    ) if NodeRegistry.get_node(node) else 0,
                    dispatch_id=dispatch_id,
                )
                level_payloads[node] = payload
                await self._queue.enqueue(self._task_queue, payload.to_dict())
                bloom.add(node)

            if not level_payloads:
                continue

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

                if result.dispatch_id == dispatch_id and task_id in expected:
                    collected[task_id] = result
                    results[task_id] = result

                    if result.succeeded and result.result is not None:
                        self._context[task_id.name] = result.result

            failed = [tid for tid, r in collected.items() if r.status == TaskStatus.FAILED]
            if failed:
                logger.warning("Master: level %d had failures: %s", level_idx, failed)
                if not self._continue_on_failure:
                    logger.error("Master: aborting due to failure")
                    for node in graph.nodes:
                        if node not in results:
                            results[node] = TaskResultPayload(
                                task_id=node,
                                status=TaskStatus.CANCELLED,
                                worker_id="master",
                                dispatch_id=dispatch_id,
                            )
                    break

            print(f"[Master] Level {level_idx}: {len(collected)}/{len(expected)} completed")

        return results

    async def shutdown(self) -> None:
        """Disconnect from the broker."""
        self._running = False
        if self._queue:
            try:
                await self._queue.disconnect()
            except Exception:
                pass
        logger.info("Master: disconnected from broker")


class DistributedEngine:
    """
    High-level API for distributed execution.
    
    Can be used in two modes:
    1. In-process (for tests): embeds broker + worker pool
    2. Real distributed: master connects to external broker, workers are separate processes
    """

    def __init__(
        self,
        broker_host: str = "127.0.0.1",
        broker_port: int = 9527,
        continue_on_failure: bool = False,
    ) -> None:
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._master = Master(
            broker_host=broker_host,
            broker_port=broker_port,
            continue_on_failure=continue_on_failure,
        )

    async def connect(self) -> None:
        """Connect the master to the broker."""
        await self._master.connect()

    async def run(self, graph: TaskGraph) -> Dict[TaskIdentifier, TaskResultPayload]:
        """Submit a graph and wait for results from distributed workers."""
        return await self._master.submit_graph(graph)

    async def stop(self) -> None:
        """Disconnect the master."""
        await self._master.shutdown()
