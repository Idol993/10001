#!/usr/bin/env python3
"""
Broker CLI - Start the TCP-based message broker server.
This must be running before Master and Worker processes can communicate.

Usage:
    python -m async_task_engine.cli.broker --host 127.0.0.1 --port 9527
"""
import argparse
import asyncio
import signal
import sys

sys.path.insert(0, ".")

from async_task_engine.infrastructure.logger import setup_logging
from async_task_engine.infrastructure.message_queue import TCPBrokerServer


async def run_broker(host: str, port: int) -> None:
    broker = TCPBrokerServer(host=host, port=port)

    # Graceful shutdown on SIGINT/SIGTERM
    stop_event = asyncio.Event()

    def _handle_signal(sig, frame):
        print("\n[Broker] Received signal, shutting down...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass  # Windows doesn't support signal handlers

    print(f"[Broker] Starting TCP broker on {host}:{port}")
    print(f"[Broker] Press Ctrl+C to stop")

    server_task = asyncio.create_task(broker.start())

    try:
        await stop_event.wait()
    finally:
        print("[Broker] Stopping...")
        await broker.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass
        print("[Broker] Stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the distributed task broker")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address")
    parser.add_argument("--port", type=int, default=9527, help="Bind port")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    setup_logging(level=20 if args.verbose else 30, json_format=False)
    asyncio.run(run_broker(args.host, args.port))


if __name__ == "__main__":
    main()
