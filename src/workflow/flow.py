"""The locked order-fulfillment flow.

The process is expressed as data (task names, data points, end tasks) plus two
pure functions:

    validate_close(statuses, task_id, value) -> error message or None
    plan(statuses, task_id, value)           -> ordered list of Updates

Neither touches a store, a clock or a queue. The engine decides WHEN an update
is applied; this module decides WHICH updates an accepted event causes.
"""

from __future__ import annotations

from dataclasses import dataclass

# Task statuses
PENDING = "pending"
AVAILABLE = "available"
CLOSED = "closed"
CANCELLED = "cancelled"
TASK_STATUSES = (PENDING, AVAILABLE, CLOSED, CANCELLED)

# Order (instance) statuses
INSTANCE_IDLE = "idle"
INSTANCE_RUNNING = "running"
INSTANCE_COMPLETED = "completed"
INSTANCE_CANCELLED = "cancelled"
INSTANCE_STATUSES = (
    INSTANCE_IDLE,
    INSTANCE_RUNNING,
    INSTANCE_COMPLETED,
    INSTANCE_CANCELLED,
)

YES = "yes"
NO = "no"

TASKS: dict[str, str] = {
    "T1": "Receive order",
    "T2": "Send contract",
    "T3": "Cancel order",
    "T4": "Record billing details",
    "T5": "Schedule site visit",
    "T6": "Check stock",
    "T7": "Verify billing details",
    "T8": "Assign technician",
    "T9": "Purchase missing items",
    "T10": "Receive items",
    "T11": "Assemble and configure equipment",
    "T12": "Ship equipment",
    "T13": "Perform installation",
    "T14": "Confirm completion",
    "T15": "Schedule follow-up visit",
}
TASK_IDS: tuple[str, ...] = tuple(TASKS)

# Tasks that carry a yes/no data point, and the name of that data point.
DATA_POINTS: dict[str, str] = {
    "T2": "contract_signed",
    "T6": "all_items_in_stock",
    "T13": "full_installation",
}

# End tasks are closed by the engine, never by the user.
END_TASKS = frozenset({"T3", "T14"})

# Plain "when X closes, Y becomes available" edges.
_SUCCESSOR = {
    "T1": "T2",
    "T4": "T7",
    "T5": "T8",
    "T9": "T10",
    "T10": "T11",
    "T15": "T13",
}

# AND joins: (inputs that must all be closed, task that becomes available).
_J1 = (("T7", "T11"), "T12")
_J2 = (("T12", "T8"), "T13")
_JOINS = {"T7": _J1, "T11": _J1, "T12": _J2, "T8": _J2}


@dataclass(frozen=True)
class Update:
    """One change to one task's status. `value` is the data point recorded
    when a task with a data point is closed."""

    task_id: str
    status: str
    value: str | None = None


def initial_statuses() -> dict[str, str]:
    return {task_id: PENDING for task_id in TASK_IDS}


def validate_close(statuses: dict[str, str], task_id: object, value: object) -> str | None:
    """Return a human-readable reason to reject a task.closed event, or None
    if the event is acceptable. Does not change anything."""
    if not isinstance(task_id, str) or task_id not in statuses:
        return f"unknown task {task_id!r}"
    if statuses[task_id] != AVAILABLE:
        return f"task {task_id} is not available (status: {statuses[task_id]})"
    if task_id in DATA_POINTS:
        if not (isinstance(value, str) and value in (YES, NO)):
            return (
                f"task {task_id} needs {DATA_POINTS[task_id]} to be exactly "
                f"'yes' or 'no', got {value!r}"
            )
    elif value is not None:
        return f"task {task_id} has no data point but a value was given: {value!r}"
    return None


class _Planner:
    """Works on a private copy of the statuses so that later steps of the same
    event (joins, 'cancel unless closed') see the effect of earlier steps."""

    def __init__(self, statuses: dict[str, str]) -> None:
        self._st = dict(statuses)
        self.updates: list[Update] = []

    def _set(self, task_id: str, status: str, value: str | None = None) -> None:
        self._st[task_id] = status
        self.updates.append(Update(task_id, status, value))

    def close(self, task_id: str, value: str | None = None) -> None:
        self._set(task_id, CLOSED, value)

    def open(self, task_id: str) -> None:
        """Flow reaches a task. End tasks are closed straight away by the
        engine (pending -> closed, one update); others become available."""
        self._set(task_id, CLOSED if task_id in END_TASKS else AVAILABLE)

    def cancel(self, task_id: str) -> None:
        """The task became unreachable. Closed tasks keep their status."""
        if self._st[task_id] in (CLOSED, CANCELLED):
            return
        self._set(task_id, CANCELLED)

    def join(self, inputs: tuple[str, ...], out: str) -> None:
        if all(self._st[i] == CLOSED for i in inputs):
            self.open(out)


def plan(statuses: dict[str, str], task_id: str, value: str | None = None) -> list[Update]:
    """Ordered updates caused by the user closing `task_id` (already validated)."""
    p = _Planner(statuses)
    p.close(task_id, value if task_id in DATA_POINTS else None)

    if task_id in _SUCCESSOR:
        p.open(_SUCCESSOR[task_id])

    elif task_id == "T2":
        if value == YES:
            p.cancel("T3")
            for t in ("T4", "T5", "T6"):  # SPLIT: all branches start at once
                p.open(t)
        else:
            for t in TASK_IDS[3:]:  # T4 .. T15 can never be reached now
                p.cancel(t)
            p.open("T3")

    elif task_id == "T6":
        if value == YES:
            p.cancel("T9")
            p.cancel("T10")
            p.open("T11")
        else:
            p.open("T9")

    elif task_id in _JOINS:
        inputs, out = _JOINS[task_id]
        p.join(inputs, out)

    elif task_id == "T13":
        if value == YES:
            p.cancel("T15")  # no-op if it was closed on an earlier pass
            p.open("T14")
        else:
            p.open("T15")  # T14 stays pending: it is still reachable

    return p.updates
