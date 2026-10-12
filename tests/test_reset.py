"""Scenario 6: a reset clears state, waiting updates and the rate-limit window."""

from helpers import close, run, settle, start, update_log
from test_rate_limit import ten_updates
from workflow.events import InstanceStarted, SystemReset, TaskClosed


def test_reset_mid_order_wipes_state_and_discards_waiting_work(app):
    ten_updates(app)
    app.bus.publish(TaskClosed("T5"))
    app.bus.publish(TaskClosed("T6", "yes"))  # 4 updates waiting
    app.bus.publish(TaskClosed("T7"))  # 1 event queued behind them
    assert app.engine.pending_count == 4
    assert app.engine.inbox_count == 1
    applied = len(update_log(app))

    app.bus.publish(SystemReset())

    assert set(app.store.snapshot().values()) == {"pending"}
    assert app.store.data_points() == {}
    assert app.store.get_instance_status() == "idle"
    assert app.engine.pending_count == 0
    assert app.engine.inbox_count == 0

    # nothing from before the reset is applied later
    app.clock.advance(300)
    assert app.engine.pump() is None
    assert len(update_log(app)) == applied
    assert set(app.store.snapshot().values()) == {"pending"}

    reset_entry = [e for e in app.log.entries() if e.kind == "reset"][0]
    assert reset_entry.data == {"dropped_updates": 4, "dropped_events": 1}


def test_reset_clears_the_rate_limit_window(app):
    ten_updates(app)
    app.bus.publish(TaskClosed("T5"))  # window is now full (12 of 12)
    app.bus.publish(SystemReset())
    before = len(update_log(app))

    # same sequence again, still at t = 0: all 12 fit immediately
    app.bus.publish(InstanceStarted())
    app.bus.publish(TaskClosed("T1"))
    app.bus.publish(TaskClosed("T2", "yes"))
    app.bus.publish(TaskClosed("T4"))
    app.bus.publish(TaskClosed("T5"))
    assert len(update_log(app)) - before == 12
    assert app.engine.pending_count == 0

    # and the limit still applies to the new order
    app.bus.publish(TaskClosed("T6", "yes"))
    assert len(update_log(app)) - before == 12
    assert app.engine.pending_count == 4


def test_reset_is_allowed_at_any_time(app):
    app.bus.publish(SystemReset())  # idle
    start(app)
    app.bus.publish(SystemReset())  # running
    start(app)
    run(app, "T1", ("T2", "no"))  # cancelled
    app.bus.publish(SystemReset())
    assert app.store.get_instance_status() == "idle"
    assert not [e for e in app.log.entries() if e.kind == "rejection"]


def test_a_new_order_works_after_reset(app):
    ten_updates(app)
    app.bus.publish(SystemReset())
    start(app)
    assert app.store.snapshot()["T1"] == "available"
    close(app, "T1")
    assert app.store.snapshot()["T2"] == "available"
