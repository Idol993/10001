"""
Test: Metaclass & Node Registry
"""
import pytest

from async_task_engine.application.metaclass import BaseTaskNode, NodeRegistry
from async_task_engine.domain.entities import TaskIdentifier


class _TestNode(BaseTaskNode):
    identifier = TaskIdentifier(name="metaclass_test", version="1.0.0")
    description = "A test node"
    max_retries = 0

    async def execute(self, context):
        return "ok"


class TestNodeMeta:
    def test_auto_registration(self):
        node = NodeRegistry.get_node(TaskIdentifier(name="metaclass_test", version="1.0.0"))
        assert node is not None
        assert node.__name__ == "_TestNode"

    def test_instance_caching(self):
        a = _TestNode()
        b = _TestNode()
        assert a is b

    def test_missing_identifier_raises(self):
        with pytest.raises(TypeError):
            class BadNode(BaseTaskNode):
                pass

    def test_duplicate_identifier_raises(self):
        with pytest.raises((ValueError, TypeError)):
            class DupNode(BaseTaskNode):
                identifier = TaskIdentifier(name="metaclass_test", version="1.0.0")
                description = "duplicate"
                max_retries = 0
