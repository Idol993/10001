"""
Advanced Algorithms Module
Provides high-performance data structures and algorithms:
- BloomFilter: space-efficient probabilistic duplicate detection
- TopologicalSort: DAG dependency resolution using Kahn's algorithm
"""
from __future__ import annotations

import hashlib
import math
import struct
from typing import Any, Dict, FrozenSet, Generic, Iterable, Iterator, List, Set, TypeVar

from async_task_engine.domain.entities import TaskGraph, TaskIdentifier

T = TypeVar("T")


class BloomFilter(Generic[T]):
    """
    A space-efficient probabilistic data structure for membership testing.
    
    Advantages:
    - O(1) lookups and insertions
    - Extremely memory efficient
    - Allows a configurable false-positive rate
    
    Application in engine: Efficient deduplication of task execution requests
    across distributed nodes.
    """

    def __init__(self, expected_items: int = 10000, false_positive_rate: float = 0.01) -> None:
        if expected_items <= 0:
            raise ValueError("expected_items must be positive")
        if not 0 < false_positive_rate < 1:
            raise ValueError("false_positive_rate must be between 0 and 1 exclusive")

        # Optimal bit array size: m = -n * ln(p) / (ln(2))^2
        self._bit_size: int = max(8, int(-expected_items * math.log(false_positive_rate) / (math.log(2) ** 2)))
        # Optimal number of hash functions: k = (m/n) * ln(2)
        self._hash_count: int = max(1, int((self._bit_size / expected_items) * math.log(2)))
        self._bitmap: bytearray = bytearray((self._bit_size + 7) // 8)
        self._count: int = 0
        self._expected_items = expected_items

    def _hashes(self, item: T) -> List[int]:
        """Generate k hash values for an item using double hashing."""
        key = str(item).encode("utf-8")
        h1 = int.from_bytes(hashlib.sha256(key).digest()[:8], "big")
        h2 = int.from_bytes(hashlib.md5(key).digest()[:8], "big")

        positions: List[int] = []
        for i in range(self._hash_count):
            pos = (h1 + i * h2) % self._bit_size
            positions.append(pos)
        return positions

    def add(self, item: T) -> None:
        """Add an item to the Bloom filter."""
        for pos in self._hashes(item):
            self._bitmap[pos // 8] |= (1 << (pos % 8))
        self._count += 1

    def __contains__(self, item: object) -> bool:
        """Test if item might be in the filter (False positive possible)."""
        if not isinstance(item, T.__args__[0] if hasattr(T, "__args__") else object):
            return False
        try:
            typed_item = item  # type: ignore[assignment]
            for pos in self._hashes(typed_item):
                if not (self._bitmap[pos // 8] & (1 << (pos % 8))):
                    return False
            return True
        except Exception:
            return False

    def __len__(self) -> int:
        return self._count

    @property
    def capacity(self) -> int:
        return self._bit_size

    def false_positive_probability(self) -> float:
        """Estimate current false positive probability."""
        if self._count == 0:
            return 0.0
        return (1 - math.exp(-self._hash_count * self._count / self._bit_size)) ** self._hash_count


class TopologicalSorter:
    """
    Topological Sort using Kahn's Algorithm (BFS-based).
    
    Time Complexity: O(V + E)
    Space Complexity: O(V + E)
    
    Features:
    - Cycle detection with clear error reporting
    - Priority-based ordering for ties (optional)
    - Supports partial execution planning
    
    Critical for: Determining correct execution order of tasks in a DAG.
    """

    @staticmethod
    def sort(graph: TaskGraph) -> List[TaskIdentifier]:
        """
        Perform topological sort on the task graph.
        
        Returns a valid execution order where dependencies are always
        scheduled before their dependents.
        
        Raises:
            ValueError: If a cycle is detected in the dependency graph.
        """
        # Build adjacency list (dependent -> dependencies) and reverse
        in_degree: Dict[TaskIdentifier, int] = {node: 0 for node in graph.nodes}
        forward_edges: Dict[TaskIdentifier, Set[TaskIdentifier]] = {
            node: set() for node in graph.nodes
        }

        for node, deps in graph.dependencies.items():
            for dep in deps:
                if dep not in graph.nodes:
                    graph.add_node(dep)
                    in_degree[dep] = 0
                    forward_edges.setdefault(dep, set())
                forward_edges[dep].add(node)
                in_degree[node] = in_degree.get(node, 0) + 1

        # Kahn's algorithm: start with nodes having zero in-degree
        queue: List[TaskIdentifier] = [n for n, d in in_degree.items() if d == 0]
        queue.sort(key=lambda x: str(x))
        result: List[TaskIdentifier] = []

        while queue:
            node = queue.pop(0)
            result.append(node)

            for neighbor in sorted(forward_edges.get(node, set()), key=lambda x: str(x)):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
                    queue.sort(key=lambda x: str(x))

        if len(result) != len(graph.nodes):
            # Find cycle
            remaining = graph.nodes - set(result)
            cycle_nodes = ", ".join(str(n) for n in remaining)
            raise ValueError(f"Circular dependency detected among tasks: {cycle_nodes}")

        return result

    @staticmethod
    def get_execution_levels(graph: TaskGraph) -> List[List[TaskIdentifier]]:
        """
        Group tasks by execution level (tasks that can run concurrently).
        
        Level 0: Tasks with no dependencies
        Level K: Tasks whose all dependencies are in levels 0..K-1
        
        This enables maximum parallelism in async execution.
        """
        topo_order = TopologicalSorter.sort(graph)
        level: Dict[TaskIdentifier, int] = {}

        for node in topo_order:
            deps = graph.dependencies.get(node, frozenset())
            if not deps:
                level[node] = 0
            else:
                level[node] = max(level.get(d, 0) for d in deps) + 1

        max_level = max(level.values()) if level else 0
        levels: List[List[TaskIdentifier]] = [[] for _ in range(max_level + 1)]

        for node, lvl in level.items():
            levels[lvl].append(node)

        for lvl in levels:
            lvl.sort(key=lambda x: str(x))

        return levels
