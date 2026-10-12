"""Scenario 4: updates are applied one at a time, in arrival order."""

import threading
import time

from helpers import settle, start, run, updated_ids
from workflow.app import build_app
from workflow.clock import FakeClock
from workflow.events import TaskClosed
from workflow.memory import InMemoryStateStore


class OverlapDetectingStore(InMemoryStateStore):
    """Counts how many set_status calls are running at the same time."""

    def __init__(self):
        super().__init__()
        self._guard = threading.Lock()
        self.in_flight = 0
        self.max_in_flight = 0

    def set_status(self, task_id, status):
        with self._guard:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
        time.sleep(0.002)  # widen the window in which an overlap would show
        super().set_status(task_id, status)
        with self._guard:
            self.in_flight -= 1


def test_concurrent_events_never_overlap():
    store = OverlapDetectingStore()
    app = build_app(FakeClock(), limit=1000, store=store)
    start(app)
    run(app, "T1", ("T2", "yes"))

    events = [TaskClosed("T4"), TaskClosed("T5"), TaskClosed("T6", "yes")]
    barrier = threading.Barrier(len(events))

    def fire(event):
        barrier.wait()
        app.engine.handle(event)  # straight to the engine: no bus lock to hide behind

    threads = [threading.Thread(target=fire, args=(e,)) for e in events]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert store.max_in_flight == 1
    snapshot = app.store.snapshot()
    assert snapshot["T4"] == snapshot["T5"] == snapshot["T6"] == "closed"
    # each event's updates form one contiguous block: none interleaved
    ids = updated_ids(app)[8:]
    blocks = {"T4": ["T4", "T7"], "T5": ["T5", "T8"], "T6": ["T6", "T9", "T10", "T11"]}
    first_of_block = [i for i in ids if i in blocks]
    rebuilt = [x for first in first_of_block for x in blocks[first]]
    assert ids == rebuilt


def test_events_queued_behind_the_rate_limit_run_in_arrival_order(app):
    from test_rate_limit import ten_updates

    ten_updates(app)
    app.bus.publish(TaskClosed("T5"))
    app.bus.publish(TaskClosed("T6", "yes"))  # 4 updates, all waiting
    app.bus.publish(TaskClosed("T7"))  # queued behind them
    assert app.engine.pending_count == 4
    assert app.engine.inbox_count == 1

    app.clock.advance(60)
    app.engine.pump()
    assert updated_ids(app)[12:] == ["T6", "T9", "T10", "T11", "T7"]


def test_an_event_is_judged_when_it_is_processed_not_when_it_is_published(app):
    """T11 is not visibly available when this event is published, but the
    earlier T6 event (still waiting) will make it so before this one is handled."""
    from test_rate_limit import ten_updates

    ten_updates(app)
    app.bus.publish(TaskClosed("T5"))
    app.bus.publish(TaskClosed("T6", "yes"))
    app.bus.publish(TaskClosed("T11"))
    settle(app)
    assert app.store.get_status("T11") == "closed"
    assert not [e for e in app.log.entries() if e.kind == "rejection"]


def test_the_same_task_closed_twice_in_a_burst_is_rejected_the_second_time(app):
    start(app)
    run(app, "T1")
    app.bus.publish(TaskClosed("T2", "yes"))
    app.bus.publish(TaskClosed("T2", "no"))
    settle(app)
    assert app.store.data_points()["T2"] == "yes"
    assert len([e for e in app.log.entries() if e.kind == "rejection"]) == 1
