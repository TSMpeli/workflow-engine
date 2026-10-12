"""In-memory implementations of the event bus, state store and log system."""

from __future__ import annotations

import threading
from typing import Any, Callable

from .clock import Clock
from .events import Event
from .flow import (
    INSTANCE_IDLE,
    INSTANCE_RUNNING,
    INSTANCE_STATUSES,
    PENDING,
    TASK_IDS,
    TASK_STATUSES,
)
from .ports import LogEntry


class InMemoryEventBus:
    """Synchronous bus. Dispatch is serialised by a lock, so handlers are
    never invoked concurrently even when several threads publish at once."""

    def __init__(self) -> None:
        self._handlers: list[Callable[[Event], None]] = []
        self._lock = threading.RLock()

    def subscribe(self, handler: Callable[[Event], None]) -> None:
        with self._lock:
            self._handlers.append(handler)

    def publish(self, event: Event) -> None:
        with self._lock:
            for handler in list(self._handlers):
                handler(event)


class InMemoryStateStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._wipe()

    def _wipe(self) -> None:
        self._status = {task_id: PENDING for task_id in TASK_IDS}
        self._data: dict[str, str] = {}
        self._instance = INSTANCE_IDLE

    def get_status(self, task_id: str) -> str:
        with self._lock:
            return self._status[task_id]

    def set_status(self, task_id: str, status: str) -> None:
        if task_id not in self._status:
            raise KeyError(task_id)
        if status not in TASK_STATUSES:
            raise ValueError(f"invalid task status {status!r}")
        with self._lock:
            self._status[task_id] = status

    def snapshot(self) -> dict[str, str]:
        with self._lock:
            return dict(self._status)

    def get_data_point(self, task_id: str) -> str | None:
        with self._lock:
            return self._data.get(task_id)

    def set_data_point(self, task_id: str, value: str) -> None:
        with self._lock:
            self._data[task_id] = value

    def data_points(self) -> dict[str, str]:
        with self._lock:
            return dict(self._data)

    def get_instance_status(self) -> str:
        with self._lock:
            return self._instance

    def set_instance_status(self, status: str) -> None:
        if status not in INSTANCE_STATUSES:
            raise ValueError(f"invalid instance status {status!r}")
        with self._lock:
            self._instance = status

    def begin_instance(self) -> None:
        with self._lock:
            self._wipe()
            self._instance = INSTANCE_RUNNING

    def reset(self) -> None:
        with self._lock:
            self._wipe()


class InMemoryLogSystem:
    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: list[LogEntry] = []

    def log(
        self,
        level: str,
        kind: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> LogEntry:
        with self._lock:
            entry = LogEntry(
                seq=len(self._entries) + 1,
                time=self._clock.now(),
                level=level,
                kind=kind,
                message=message,
                data=dict(data or {}),
            )
            self._entries.append(entry)
            return entry

    def entries(self) -> list[LogEntry]:
        with self._lock:
            return list(self._entries)
