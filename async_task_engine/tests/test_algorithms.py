"""
Test: Advanced Algorithms (BloomFilter, TopologicalSort)
"""
import pytest
import time

from async_task_engine.application.algorithms import BloomFilter, TopologicalSorter
from async_task_engine.domain.entities import TaskGraph, TaskIdentifier


class TestBloomFilter:
    def test_add_and_maybe_contains(self):
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        bf.add("item1")
        bf.add("item2")
        bf.add("item3")

        # These should definitely be present (no false negatives)
        assert "item1" in bf
        assert "item2" in bf
        assert "item3" in bf

        # These might or might not be present (could be false positives)
        # But Bloom filters guarantee no false negatives
        # So non-existent items might return True (FP) or False

    def test_count_tracks_inserts(self):
        bf = BloomFilter(expected_items=10)
        for i in range(10):
            bf.add(f"item_{i}")
        assert len(bf) == 10

    def test_parameters(self):
        bf = BloomFilter(expected_items=10000, false_positive_rate=0.01)
        assert bf.capacity > 0
        assert bf.false_positive_probability() >= 0.0

    def test_invalid_parameters(self):
        with pytest.raises(ValueError):
            BloomFilter(expected_items=0)
        with pytest.raises(ValueError):
            BloomFilter(false_positive_rate=0.0)
        with pytest.raises(ValueError):
            BloomFilter(false_positive_rate=1.5)

    def test_false_positive_rate_within_bounds(self):
        """Fill the filter and verify FP rate stays within reasonable bounds."""
        bf = BloomFilter(expected_items=1000, false_positive_rate=0.05)
        for i in range(1000):
            bf.add(f"key_{i}")

        false_positives = sum(1 for i in range(1000) if f"key_{i}_nonexistent" in bf)
        # Should be roughly <= 5% (with some tolerance for small tests)
        assert false_positives <= 150, f"False positive rate too high: {false_positives}/1000"


class TestTopologicalSorter:
    def test_simple_dag(self):
        g = TaskGraph()
        a = TaskIdentifier(name="a")
        b = TaskIdentifier(name="b")
        c = TaskIdentifier(name="c")

        g.add_node(a)
        g.add_node(b)
        g.add_node(c)
        g.add_dependency(b, a)  # b depends on a
        g.add_dependency(c, b)  # c depends on b

        order = TopologicalSorter.sort(g)
        assert order.index(a) < order.index(b)
        assert order.index(b) < order.index(c)

    def test_independent_nodes(self):
        g = TaskGraph()
        for i in range(5):
            g.add_node(TaskIdentifier(name=f"node_{i}"))

        order = TopologicalSorter.sort(g)
        assert len(order) == 5

    def test_cycle_detection(self):
        g = TaskGraph()
        a = TaskIdentifier(name="a")
        b = TaskIdentifier(name="b")
        c = TaskIdentifier(name="c")

        g.add_dependency(a, b)
        g.add_dependency(b, c)
        g.add_dependency(c, a)  # Creates cycle

        with pytest.raises(ValueError, match="Circular dependency"):
            TopologicalSorter.sort(g)

    def test_execution_levels(self):
        g = TaskGraph()
        a = TaskIdentifier(name="a")
        b = TaskIdentifier(name="b")
        c = TaskIdentifier(name="c")
        d = TaskIdentifier(name="d")

        g.add_node(a)
        g.add_node(b)
        g.add_node(c)
        g.add_node(d)
        g.add_dependency(b, a)
        g.add_dependency(c, a)
        g.add_dependency(d, b)

        levels = TopologicalSorter.get_execution_levels(g)
        assert len(levels) >= 2

        # First level should contain nodes with no dependencies
        assert a in levels[0]
        # b and c should be after a
        for level in levels[1:]:
            if b in level or c in level:
                assert a in levels[0]

    def test_complex_dag_levels(self):
        g = TaskGraph()
        ids = {name: TaskIdentifier(name=name) for name in ["fetch", "validate", "transform", "aggregate", "output"]}

        g.add_node(ids["fetch"])
        g.add_node(ids["validate"])
        g.add_node(ids["transform"])
        g.add_node(ids["aggregate"])
        g.add_node(ids["output"])

        g.add_dependency(ids["validate"], ids["fetch"])
        g.add_dependency(ids["transform"], ids["validate"])
        g.add_dependency(ids["aggregate"], ids["transform"])
        g.add_dependency(ids["output"], ids["aggregate"])

        levels = TopologicalSorter.get_execution_levels(g)
        # Each level should have exactly 1 node (sequential chain)
        assert len(levels) == 5
        for level in levels:
            assert len(level) == 1

        # Verify order
        flat = [node for level in levels for node in level]
        assert flat == [ids["fetch"], ids["validate"], ids["transform"], ids["aggregate"], ids["output"]]
