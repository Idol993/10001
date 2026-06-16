"""
Infrastructure: TCP-Based Distributed Message Broker
A zero-dependency distributed message broker that enables true cross-process
and cross-node communication. Replaces Redis with a lightweight asyncio TCP server.

Architecture:
  ┌──────────────┐     TCP Socket      ┌──────────────────┐
  │  Master      │◄────────────────────►│                  │
  └──────────────┘                      │   TCP Broker     │
                                        │   (msg_broker)   │
  ┌──────────────┐     TCP Socket      │                  │
  │  Worker_0    │◄────────────────────►│   - Task Queue   │
  └──────────────┘                      │   - Result Queue  │
                                        │   - State Store  │
  ┌──────────────┐     TCP Socket      │   - Pub/Sub      │
  │  Worker_N    │◄────────────────────►│                  │
  └──────────────┘                      └──────────────────┘

Message Protocol (JSON-Line over TCP):
  {"cmd": "ENQUEUE", "queue": "tasks", "payload": {...}}
  {"cmd": "DEQUEUE", "queue": "tasks", "timeout": 5.0}
  {"cmd": "PING"}
  {"cmd": "SET", "key": "state:abc", "value": {...}}
  {"cmd": "GET", "key": "state:abc"}
  {"cmd": "SIZE", "queue": "tasks"}
  {"cmd": "CLEAR", "queue": "tasks"}
  {"cmd": "CLOSE"}
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from typing import Any, Deque, Dict, List, Optional

from async_task_engine.interface.distributed import MessageQueue

logger = logging.getLogger(__name__)

ENQUEUE = "ENQUEUE"
DEQUEUE = "DEQUEUE"
SIZE = "SIZE"
CLEAR = "CLEAR"
SET = "SET"
GET = "GET"
PING = "PING"
CLOSE = "CLOSE"


class TCPBrokerServer:
    """
    A TCP-based message broker that runs as a standalone server.
    
    Features:
    - Multiple named FIFO queues (tasks, results, etc.)
    - Blocking dequeue with configurable timeout
    - Key-value state store for sharing execution state
    - Statistics tracking
    - Graceful shutdown
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 9527) -> None:
        self._host = host
        self._port = port
        self._queues: Dict[str, Deque[Dict[str, Any]]] = defaultdict(lambda: asyncio.Queue())
        self._conditions: Dict[str, asyncio.Condition] = {}
        self._kv_store: Dict[str, Any] = {}
        self._stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"enqueued": 0, "dequeued": 0})
        self._server: Optional[asyncio.Server] = None
        self._running = False

    async def start(self) -> None:
        """Start the TCP broker server."""
        self._server = await asyncio.start_server(
            self._handle_client,
            self._host,
            self._port,
        )
        self._running = True
        logger.info("TCP Broker started on %s:%s", self._host, self._port)

        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        """Stop the broker server."""
        self._running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        logger.info("TCP Broker stopped")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle a single client connection."""
        peer = writer.transport.get_extra_info("peername", ("unknown", 0))
        logger.debug("Client connected: %s", peer)

        try:
            while True:
                line_bytes = await reader.readline()
                if not line_bytes:
                    break

                line = line_bytes.decode("utf-8").strip()
                if not line:
                    continue

                try:
                    request = json.loads(line)
                except json.JSONDecodeError:
                    await self._send_response(writer, {"status": "error", "message": "Invalid JSON"})
                    continue

                response = await self._dispatch(request)
                await self._send_response(writer, response)

        except (ConnectionResetError, BrokenPipeError, EOFError):
            pass
        finally:
            logger.debug("Client disconnected: %s", peer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _send_response(self, writer: asyncio.StreamWriter, response: Dict[str, Any]) -> None:
        """Send a JSON response back to the client."""
        line = json.dumps(response, default=str) + "\n"
        writer.write(line.encode("utf-8"))
        await writer.drain()

    async def _dispatch(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch a command to the appropriate handler."""
        cmd = request.get("cmd", "").upper()

        if cmd == ENQUEUE:
            return await self._handle_enqueue(request)
        elif cmd == DEQUEUE:
            return await self._handle_dequeue(request)
        elif cmd == SIZE:
            return await self._handle_size(request)
        elif cmd == CLEAR:
            return await self._handle_clear(request)
        elif cmd == SET:
            return self._handle_set(request)
        elif cmd == GET:
            return self._handle_get(request)
        elif cmd == PING:
            return {"status": "ok", "pong": True}
        elif cmd == CLOSE:
            self._running = False
            return {"status": "ok"}
        else:
            return {"status": "error", "message": f"Unknown command: {cmd}"}

    async def _handle_enqueue(self, request: Dict[str, Any]) -> Dict[str, Any]:
        queue_name = request["queue"]
        payload = request.get("payload", {})
        q = self._queues[queue_name]
        enriched = {
            "payload": payload,
            "timestamp": time.monotonic(),
            "message_id": f"{queue_name}:{time.monotonic_ns()}",
        }
        await q.put(enriched)
        self._stats[queue_name]["enqueued"] += 1
        return {"status": "ok", "queue": queue_name}

    async def _handle_dequeue(self, request: Dict[str, Any]) -> Dict[str, Any]:
        queue_name = request["queue"]
        timeout = request.get("timeout", 30.0)
        q = self._queues[queue_name]
        try:
            enriched = await asyncio.wait_for(q.get(), timeout=timeout)
            self._stats[queue_name]["dequeued"] += 1
            return {"status": "ok", "message": enriched["payload"]}
        except asyncio.TimeoutError:
            return {"status": "timeout"}

    async def _handle_size(self, request: Dict[str, Any]) -> Dict[str, Any]:
        queue_name = request["queue"]
        q = self._queues[queue_name]
        return {"status": "ok", "size": q.qsize()}

    async def _handle_clear(self, request: Dict[str, Any]) -> Dict[str, Any]:
        queue_name = request["queue"]
        q = self._queues[queue_name]
        while not q.empty():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                break
        return {"status": "ok", "queue": queue_name}

    def _handle_set(self, request: Dict[str, Any]) -> Dict[str, Any]:
        key = request["key"]
        value = request.get("value")
        self._kv_store[key] = value
        return {"status": "ok"}

    def _handle_get(self, request: Dict[str, Any]) -> Dict[str, Any]:
        key = request["key"]
        value = self._kv_store.get(key)
        if value is None:
            return {"status": "ok", "value": None, "exists": False}
        return {"status": "ok", "value": value, "exists": True}

    def get_stats(self) -> Dict[str, Dict[str, int]]:
        return {name: dict(stats) for name, stats in self._stats.items()}


class TCPMessageQueue(MessageQueue):
    """
    Client for connecting to the TCPBrokerServer.
    
    Implements the MessageQueue protocol using asyncio TCP streams.
    This allows Master and Worker processes (potentially on different machines)
    to communicate via a shared broker.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 9527) -> None:
        self._host = host
        self._port = port
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Connect to the TCP broker server."""
        if self._connected:
            return
        self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        self._connected = True
        logger.debug("Connected to broker at %s:%s", self._host, self._port)

    async def disconnect(self) -> None:
        """Disconnect from the broker server."""
        if self._writer:
            try:
                await self._send({"cmd": CLOSE})
            except Exception:
                pass
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
        self._connected = False
        logger.debug("Disconnected from broker")

    async def _send(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send a request and receive a response."""
        if not self._connected or not self._writer or not self._reader:
            raise ConnectionError("Not connected to broker")

        async with self._lock:
            line = json.dumps(request, default=str) + "\n"
            self._writer.write(line.encode("utf-8"))
            await self._writer.drain()

            response_bytes = await self._reader.readline()
            if not response_bytes:
                raise ConnectionError("Connection closed by broker")
            return json.loads(response_bytes.decode("utf-8"))

    async def enqueue(self, queue_name: str, payload: Dict[str, Any]) -> None:
        """Push a message into the specified queue."""
        response = await self._send({
            "cmd": ENQUEUE,
            "queue": queue_name,
            "payload": payload,
        })
        if response["status"] != "ok":
            raise RuntimeError(f"Enqueue failed: {response}")

    async def dequeue(self, queue_name: str, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """Pop a message from the queue. Returns None on timeout."""
        response = await self._send({
            "cmd": DEQUEUE,
            "queue": queue_name,
            "timeout": timeout,
        })
        if response["status"] == "ok":
            return response["message"]
        return None

    async def size(self, queue_name: str) -> int:
        """Return the number of messages in a queue."""
        response = await self._send({
            "cmd": SIZE,
            "queue": queue_name,
        })
        return response.get("size", 0)

    async def clear(self, queue_name: str) -> None:
        """Remove all messages from a queue."""
        await self._send({
            "cmd": CLEAR,
            "queue": queue_name,
        })

    async def set_key(self, key: str, value: Any) -> None:
        """Set a key-value pair in the shared state store."""
        await self._send({
            "cmd": SET,
            "key": key,
            "value": value,
        })

    async def get_key(self, key: str) -> Any:
        """Get a value from the shared state store."""
        response = await self._send({
            "cmd": GET,
            "key": key,
        })
        return response.get("value")
