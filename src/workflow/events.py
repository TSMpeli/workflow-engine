"""The three events the engine understands. They are plain immutable data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Union


@dataclass(frozen=True)
class InstanceStarted:
    NAME: ClassVar[str] = "instance.started"


@dataclass(frozen=True)
class TaskClosed:
    """A user closed a task. `value` is only meaningful for tasks that carry a
    data point; it is typed `object` on purpose so malformed input (None, True,
    "YES", ...) can reach the engine and be rejected there."""

    NAME: ClassVar[str] = "task.closed"
    task_id: str
    value: object = None


@dataclass(frozen=True)
class SystemReset:
    NAME: ClassVar[str] = "system.reset"


Event = Union[InstanceStarted, TaskClosed, SystemReset]
