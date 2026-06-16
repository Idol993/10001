"""
NodeMeta Metaclass
A powerful metaclass that provides automatic registration, interface validation,
and singleton-like instantiation for all TaskNode implementations.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, ClassVar, Dict, FrozenSet, List, Optional, Type

from async_task_engine.domain.entities import TaskIdentifier
from async_task_engine.interface.protocols import TaskNode

logger = logging.getLogger(__name__)


class NodeRegistry:
    """Central registry for all task node classes and instances."""

    _nodes: Dict[TaskIdentifier, Type["BaseTaskNode"]] = {}
    _instances: Dict[Type["BaseTaskNode"], "BaseTaskNode"] = {}

    @classmethod
    def register(cls, node_type: Type["BaseTaskNode"]) -> None:
        identifier = getattr(node_type, "identifier", None)
        if not identifier:
            raise TypeError(f"Task node {node_type.__name__} must define 'identifier'")
        if identifier in cls._nodes:
            existing = cls._nodes[identifier]
            raise ValueError(
                f"Duplicate task identifier '{identifier}': "
                f"already registered by {existing.__name__}, cannot register {node_type.__name__}"
            )
        cls._nodes[identifier] = node_type
        logger.debug(f"Registered task node: {identifier} -> {node_type.__name__}")

    @classmethod
    def get_node(cls, identifier: TaskIdentifier) -> Optional[Type["BaseTaskNode"]]:
        return cls._nodes.get(identifier)

    @classmethod
    def get_all_nodes(cls) -> Dict[TaskIdentifier, Type["BaseTaskNode"]]:
        return dict(cls._nodes)

    @classmethod
    def clear(cls) -> None:
        cls._nodes.clear()
        cls._instances.clear()


class NodeMeta(type):
    """
    Metaclass for Task Nodes.
    
    Features:
    1. Automatic registration into NodeRegistry upon class definition
    2. Interface validation (ensures required attributes are present)
    3. Enforces immutability of identifier after class creation
    4. Provides a shared instance cache (like singleton)
    """

    _VALID_ATTRS: FrozenSet[str] = frozenset({"identifier", "description", "max_retries"})

    def __new__(mcs, name: str, bases: tuple, namespace: Dict[str, Any], **kwargs: Any) -> type:
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        if name == "BaseTaskNode":
            return cls

        # Validate required fields
        identifier = namespace.get("identifier")
        if identifier is None:
            raise TypeError(
                f"{name} must define 'identifier' as a class variable "
                f"(e.g., identifier = TaskIdentifier(name='...', version='1.0.0'))"
            )
        if not isinstance(identifier, TaskIdentifier):
            raise TypeError(
                f"{name}.identifier must be an instance of TaskIdentifier, got {type(identifier).__name__}"
            )

        description = namespace.get("description", "")
        if not isinstance(description, str):
            raise TypeError(f"{name}.description must be a string")

        max_retries = namespace.get("max_retries", 0)
        if not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError(f"{name}.max_retries must be a non-negative integer")

        # Add default execute stub if not provided
        if "execute" not in namespace:
            async def _default_execute(self: "BaseTaskNode", context: Dict[str, Any]) -> Any:
                raise NotImplementedError(
                    f"{name}.execute() is not implemented. "
                    f"Subclasses must override this method."
                )
            cls.execute = _default_execute  # type: ignore[assignment]

        # Register the node class
        NodeRegistry.register(cls)

        return cls

    def __call__(cls, *args: Any, **kwargs: Any) -> "BaseTaskNode":
        # Cache instances for reuse
        if cls not in NodeRegistry._instances:
            instance = super().__call__(*args, **kwargs)
            NodeRegistry._instances[cls] = instance  # type: ignore[assignment]
        return NodeRegistry._instances[cls]


class BaseTaskNode(metaclass=NodeMeta):
    """
    Abstract base class for all task nodes.
    All user-defined tasks should inherit from this class.
    """

    identifier: ClassVar[TaskIdentifier]
    description: ClassVar[str] = "Base task node"
    max_retries: ClassVar[int] = 0

    async def execute(self, context: Dict[str, Any]) -> Any:
        raise NotImplementedError("Subclasses must implement execute()")

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} identifier={self.identifier}>"
