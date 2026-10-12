"""Scenario 3: at most 12 updates in any rolling minute, extra ones wait in order."""

import pytest

from helpers import status, update_log, updated_ids
from workflow.clock import FakeClock
from workflow.events import InstanceStarted, TaskClosed
from workflow.ratelimit import RollingWindowLimiter


# ---- the limiter on its own -------------------------------------------

def test_limiter_allows_exactly_the_limit():
    clock = FakeClock()
    limiter = RollingWindowLimiter(clock, limit=12, window=60)
    assert [limiter.try_acquire() for _ in range(13)] == [True] * 12 + [False]


def test_window_is_rolling_not_a_fixed_bucket():
    clock = FakeClock()
    limiter = RollingWindowLimiter(clock, limit=12, window=60)
    for _ in range(6):
        assert limiter.try_acquire()  # t = 0
    clock.advance(30)
    for _ in range(6):
        assert limiter.try_acquire()  # t = 30
    assert not limiter.try_acquire()
    assert limiter.seconds_until_ready() == 30

    clock.advance(30)  # t = 60: only the six from t = 0 have left the window
    assert [limiter.try_acquire() for _ in range(7)] == [True] * 6 + [False]
    assert limiter.seconds_until_ready() == 30  # the t = 30 batch leaves at t = 90


def test_acquisition_exactly_one_window_old_no_longer_counts():
    clock = FakeClock()
    limiter = RollingWindowLimiter(clock, limit=1, window=60)
    assert limiter.try_acquire()
    clock.advance(59.999)
    assert not limiter.try_acquire()
    clock.advance(0.001)
    assert limiter.try_acquire()


def test_clear_empties_the_window():
    limiter = RollingWindowLimiter(FakeClock(), limit=1, window=60)
    assert limiter.try_acquire()
    assert not limiter.try_acquire()
    limiter.clear()
    assert limiter.try_acquire()


@pytest.mark.parametrize("limit,window", [(0, 60), (12, 0), (-1, 60)])
def test_limiter_rejects_nonsense_configuration(limit, window):
    with pytest.raises(ValueError):
        RollingWindowLimiter(FakeClock(), limit=limit, window=window)


# ---- through the engine -------------------------------------------------

def ten_updates(app):
    """start (1) + T1 (2) + T2 yes (5) + T4 (2) = 10 updates, all at t = 0."""
    app.bus.publish(InstanceStarted())
    app.bus.publish(TaskClosed("T1"))
    app.bus.publish(TaskClosed("T2", "yes"))
    app.bus.publish(TaskClosed("T4"))
    assert len(update_log(app)) == 10


def test_thirteenth_update_waits_and_is_applied_in_order(app):
    ten_updates(app)
    app.bus.publish(TaskClosed("T5"))  # updates 11 and 12
    assert len(update_log(app)) == 12

    app.bus.publish(TaskClosed("T6", "yes"))  # T6, T9, T10, T11: updates 13 to 16
    assert len(update_log(app)) == 12
    assert status(app, "T6") == "available"  # the 13th update has not happened
    assert app.engine.pending_count == 4

    app.clock.advance(59)
    assert app.engine.pump() == 1  # tells the caller how long to wait
    assert len(update_log(app)) == 12

    app.clock.advance(1)
    assert app.engine.pump() is None
    assert updated_ids(app)[12:] == ["T6", "T9", "T10", "T11"]
    assert [e.time for e in update_log(app)[12:]] == [60.0] * 4
    assert status(app, "T6") == "closed"
    assert app.engine.pending_count == 0


def test_an_event_can_be_split_by_the_limit(app):
    ten_updates(app)
    app.bus.publish(TaskClosed("T6", "yes"))  # updates 11 and 12 apply, 13 and 14 wait
    assert updated_ids(app)[10:] == ["T6", "T9"]
    assert status(app, "T10") == "pending"
    assert status(app, "T11") == "pending"
    app.clock.advance(60)
    app.engine.pump()
    assert updated_ids(app)[10:] == ["T6", "T9", "T10", "T11"]


def test_t2_no_is_throttled_like_any_other_event(app):
    app.bus.publish(InstanceStarted())
    app.bus.publish(TaskClosed("T1"))
    app.bus.publish(TaskClosed("T2", "no"))  # 14 updates, only 9 fit
    assert len(update_log(app)) == 12
    assert app.engine.pending_count == 5
    assert app.store.get_instance_status() == "running"  # T3 not closed yet
    assert status(app, "T3") == "pending"

    app.clock.advance(60)
    app.engine.pump()
    assert status(app, "T3") == "closed"
    assert app.store.get_instance_status() == "cancelled"


def test_waiting_is_logged_once_not_on_every_pump(app):
    ten_updates(app)
    app.bus.publish(TaskClosed("T5"))
    app.bus.publish(TaskClosed("T6", "yes"))
    for _ in range(5):
        app.engine.pump()
    assert len([e for e in app.log.entries() if e.kind == "rate_limit"]) == 1
