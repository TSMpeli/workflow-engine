"""The real-time driver: waiting updates are applied by the worker thread
once the rate limit has room. Uses real time, so the window is tiny."""

import time

import pytest

from workflow.app import build_app
from workflow.clock import RealClock
from workflow.events import InstanceStarted, SystemReset, TaskClosed
from workflow.runtime import Runtime


def wait_until(condition, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.005)
    return False


@pytest.fixture
def live():
    app = build_app(RealClock(), limit=2, window=0.3)
    runtime = Runtime(app.engine, app.bus)
    runtime.start()
    yield app
    runtime.stop()


def test_waiting_update_is_applied_by_the_worker_when_there_is_room(live):
    live.bus.publish(InstanceStarted())  # update 1
    live.bus.publish(TaskClosed("T1"))  # T1 closed (2), T2 available waits
    assert live.store.get_status("T2") == "pending"
    assert wait_until(lambda: live.store.get_status("T2") == "available")
    assert live.engine.pending_count == 0


def test_reset_stops_the_worker_from_applying_stale_updates(live):
    live.bus.publish(InstanceStarted())
    live.bus.publish(TaskClosed("T1"))
    assert live.engine.pending_count == 1
    live.bus.publish(SystemReset())
    time.sleep(0.6)  # long enough for the old window to have expired twice
    assert set(live.store.snapshot().values()) == {"pending"}
    assert live.engine.pending_count == 0
