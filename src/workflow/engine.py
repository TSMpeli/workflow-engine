"""The engine: the only consumer of the event bus and the only writer of state.

Model ("pump"):

    inbox    events waiting to be handled, in arrival order
    pending  planned task updates waiting to be applied, in order

pump() repeats:
    1. if there is a pending update: apply it if the rate limiter has room,
       otherwise stop and report how long to wait
    2. else if there is an event: validate it against the CURRENT state and
       plan its updates into `pending`
    3. else stop

So events are handled strictly one at a time, and an event is judged only
after every update of the previous event has been applied. The core never
sleeps. Whoever drives it (a test with a fake clock, or runtime.Runtime with
real time) calls pump() again when time has passed.

system.reset is the exception: it is handled immediately and drops both
queues, so nothing from before a reset can be applied afterwards.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass

from .events import Event, InstanceStarted, SystemReset, TaskClosed
from .flow import (
    AVAILABLE,
    CLOSED,
    INSTANCE_CANCELLED,
    INSTANCE_COMPLETED,
    INSTANCE_RUNNING,
    Update,
    plan,
    validate_close,
)
from .ports import ERROR, INFO, EventBus, LogSystem, StateStore
from .ratelimit import RollingWindowLimiter


@dataclass(frozen=True)
class Backlog:
    updates: int  # planned updates not yet applied
    events: int  # events not yet handled
    seconds: float  # until the next update can be applied (0 if none waiting)


def _describe(event: object) -> tuple[str, str, dict]:
    """(event name, short text, structured data) for logging."""
    name = getattr(event, "NAME", type(event).__name__)
    if isinstance(event, TaskClosed):
        data = {"type": name, "task_id": event.task_id, "value": event.value}
        return name, f"{name} {event.task_id} value={event.value!r}", data
    return name, name, {"type": name}


class Engine:
    def __init__(
        self,
        bus: EventBus,
        store: StateStore,
        log: LogSystem,
        limiter: RollingWindowLimiter,
    ) -> None:
        self._store = store
        self._log = log
        self._limiter = limiter
        self._lock = threading.RLock()
        self._inbox: deque[Event] = deque()
        self._pending: deque[Update] = deque()
        self._wait_logged = False
        bus.subscribe(self.handle)

    # ---- input -------------------------------------------------------

    def handle(self, event: Event) -> None:
        """Bus subscriber. Logs the event, queues it, and pumps."""
        with self._lock:
            _, text, data = _describe(event)
            self._log.log(INFO, "event", text, data)
            if isinstance(event, SystemReset):
                self._reset()
                return
            self._inbox.append(event)
            self.pump()

    def pump(self) -> float | None:
        """Do as much work as the rate limit allows.

        Returns None when there is nothing left to do, otherwise the number of
        seconds to wait before calling pump() again.
        """
        with self._lock:
            while True:
                if self._pending:
                    if not self._limiter.try_acquire():
                        wait = self._limiter.seconds_until_ready()
                        if not self._wait_logged:
                            self._wait_logged = True
                            self._log.log(
                                INFO,
                                "rate_limit",
                                f"rate limit reached, {len(self._pending)} update(s) waiting "
                                f"(next in {wait:.1f}s)",
                                {"waiting": len(self._pending), "seconds": wait},
                            )
                        return wait
                    self._apply(self._pending.popleft())
                elif self._inbox:
                    self._process(self._inbox.popleft())
                else:
                    return None

    # ---- observation (for front ends and tests) ----------------------

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    @property
    def inbox_count(self) -> int:
        with self._lock:
            return len(self._inbox)

    def backlog(self) -> Backlog:
        with self._lock:
            seconds = self._limiter.seconds_until_ready() if self._pending else 0.0
            return Backlog(len(self._pending), len(self._inbox), seconds)

    # ---- internals ---------------------------------------------------

    def _process(self, event: Event) -> None:
        if isinstance(event, InstanceStarted):
            if self._store.get_instance_status() == INSTANCE_RUNNING:
                self._reject(event, "an order is already running")
                return
            self._store.begin_instance()
            self._log.log(INFO, "instance", "order started", {"status": INSTANCE_RUNNING})
            self._pending.append(Update("T1", AVAILABLE))
        elif isinstance(event, TaskClosed):
            snapshot = self._store.snapshot()
            reason = validate_close(snapshot, event.task_id, event.value)
            if reason is not None:
                self._reject(event, reason)
                return
            value = event.value if isinstance(event.value, str) else None
            self._pending.extend(plan(snapshot, event.task_id, value))
        else:
            self._reject(event, "unknown event type")

    def _apply(self, update: Update) -> None:
        previous = self._store.get_status(update.task_id)
        self._store.set_status(update.task_id, update.status)
        if update.value is not None:
            self._store.set_data_point(update.task_id, update.value)
        self._wait_logged = False
        self._log.log(
            INFO,
            "update",
            f"{update.task_id}: {previous} -> {update.status}",
            {
                "task_id": update.task_id,
                "from_status": previous,
                "status": update.status,
                "value": update.value,
            },
        )
        # The order is finished only once an end task is actually CLOSED.
        # (T3 is cancelled when the contract is signed, and T14 is cancelled
        # when the contract is refused: neither of those ends the order.)
        if update.status == CLOSED:
            if update.task_id == "T3":
                self._finish(INSTANCE_CANCELLED)
            elif update.task_id == "T14":
                self._finish(INSTANCE_COMPLETED)

    def _finish(self, status: str) -> None:
        self._store.set_instance_status(status)
        self._log.log(INFO, "instance", f"order {status}", {"status": status})

    def _reject(self, event: object, reason: str) -> None:
        name, _, data = _describe(event)
        self._log.log(ERROR, "rejection", f"{name} rejected: {reason}", {**data, "reason": reason})

    def _reset(self) -> None:
        dropped_updates = len(self._pending)
        dropped_events = len(self._inbox)
        self._pending.clear()
        self._inbox.clear()
        self._limiter.clear()
        self._wait_logged = False
        self._store.reset()
        self._log.log(
            INFO,
            "reset",
            f"state wiped, {dropped_updates} waiting update(s) and "
            f"{dropped_events} queued event(s) discarded, rate-limit window cleared",
            {"dropped_updates": dropped_updates, "dropped_events": dropped_events},
        )
