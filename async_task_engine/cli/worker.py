#!/usr/bin/env python3
"""
Worker CLI - Start a Worker process that connects to the broker.
Worker processes can run on different machines and share work via the broker.

Usage:
    python -m async_task_engine.cli.worker --worker-id w1
    python -m async_task_engine.cli.worker --worker-id w2 --broker-host 192.168.1.100
"""
import argparse
import asyncio
import signal
import sys

sys.path.insert(0, ".")

from async_task_engine.infrastructure.logger import setup_logging
from async_task_engine.application.distributed import Worker

# Import task nodes so they auto-register via metaclass
# These use metaclass to auto-register on import
import async_task_engine.tests.sample_nodes  # noqa: F401
import async_task_engine.tests.test_distributed  # noqa: F401
import async_task_engine.cli.demo_nodes  # noqa: F401 - demo task nodes


async def run_worker(worker_id: str, broker_host: str, broker_port: int) -> None:
    worker = Worker(
        worker_id=worker_id,
        broker_host=broker_host,
        broker_port=broker_port,
    )

    stop_event = asyncio.Event()

    def _handle_signal(sig, frame):
        print(f"\n[Worker {worker_id}] Received signal, shutting down...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    worker_task = asyncio.create_task(worker.start())

    try:
        await stop_event.wait()
    finally:
        await worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Start a worker process")
    parser.add_argument("--worker-id", required=True, help="Unique worker identifier")
    parser.add_argument("--broker-host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--broker-port", type=int, default=9527, help="Broker port")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    setup_logging(level=20 if args.verbose else 30, json_format=False)
    asyncio.run(run_worker(args.worker_id, args.broker_host, args.broker_port))


if __name__ == "__main__":
    main()
