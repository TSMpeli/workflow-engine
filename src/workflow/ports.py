"""The three interfaces the engine depends on. Each has an in-memory
implementation in memory.py, and each could be replaced independently."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from .events import Event

INFO = "INFO"
ERROR = "ERROR"


class EventBus(Protocol):
    def publish(self, event: Event) -> None: ...

    def subscribe(self, handler: Callable[[Event], None]) -> None: ...


class StateStore(Protocol):
    """State of every task plus recorded data points. Only the engine writes."""

    def get_status(self, task_id: str) -> str: ...

    def set_status(self, task_id: str, status: str) -> None: ...

    def snapshot(self) -> dict[str, str]:
        """Copy of {task_id: status} for all tasks, in flow order."""
        ...

    def get_data_point(self, task_id: str) -> str | None: ...

    def set_data_point(self, task_id: str, value: str) -> None: ...

    def data_points(self) -> dict[str, str]: ...

    def get_instance_status(self) -> str: ...

    def set_instance_status(self, status: str) -> None: ...

    def begin_instance(self) -> None:
        """Atomically wipe everything and mark a new order as running."""
        ...

    def reset(self) -> None:
        """Wipe everything: all tasks pending, no data points, instance idle."""
        ...


@dataclass(frozen=True)
class LogEntry:
    seq: int
    time: float  # from the injected clock
    level: str
    kind: str  # "event" | "update" | "rejection" | "instance" | "rate_limit" | "reset"
    message: str
    data: dict[str, Any] = field(default_factory=dict)


class LogSystem(Protocol):
    def log(self, level: str, kind: str, message: str, data: dict[str, Any] | None = None) -> LogEntry: ...

    def entries(self) -> list[LogEntry]: ...
