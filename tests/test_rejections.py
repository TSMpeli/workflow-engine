"""Scenario 5: every rejection case leaves the state unchanged and is logged."""

import pytest

from helpers import close, rejections, run, settle, start, update_log
from workflow.events import InstanceStarted, TaskClosed
from workflow.ports import ERROR


def at_idle(app):
    pass


def after_start(app):
    start(app)


def after_t1(app):
    start(app)
    run(app, "T1")


def after_contract_signed(app):
    start(app)
    run(app, "T1", ("T2", "yes"))


CASES = [
    # task is not available
    pytest.param(after_start, TaskClosed("T4"), id="pending-task"),
    pytest.param(after_t1, TaskClosed("T1"), id="already-closed"),
    pytest.param(after_contract_signed, TaskClosed("T3"), id="cancelled-task"),
    pytest.param(after_start, TaskClosed("T14"), id="end-task-by-user"),
    pytest.param(after_contract_signed, TaskClosed("T3", "yes"), id="end-task-with-value"),
    pytest.param(at_idle, TaskClosed("T1"), id="no-order-running"),
    pytest.param(after_start, TaskClosed("T99"), id="unknown-task"),
    # missing or invalid value
    pytest.param(after_t1, TaskClosed("T2"), id="missing-value"),
    pytest.param(after_t1, TaskClosed("T2", None), id="none-value"),
    pytest.param(after_t1, TaskClosed("T2", ""), id="empty-value"),
    pytest.param(after_t1, TaskClosed("T2", "maybe"), id="invalid-value"),
    pytest.param(after_t1, TaskClosed("T2", "YES"), id="wrong-case"),
    pytest.param(after_t1, TaskClosed("T2", " yes"), id="padded-value"),
    pytest.param(after_t1, TaskClosed("T2", True), id="boolean-value"),
    pytest.param(after_start, TaskClosed("T1", "yes"), id="value-on-task-without-data-point"),
    # order started while another one is running
    pytest.param(after_start, InstanceStarted(), id="start-while-running"),
    pytest.param(after_contract_signed, InstanceStarted(), id="start-mid-order"),
]


@pytest.mark.parametrize("setup,event", CASES)
def test_rejected_event_changes_nothing_and_is_logged(app, setup, event):
    setup(app)
    snapshot = app.store.snapshot()
    data = app.store.data_points()
    instance = app.store.get_instance_status()
    updates = len(update_log(app))
    rejected = len(rejections(app))
    events = len([e for e in app.log.entries() if e.kind == "event"])

    app.bus.publish(event)
    settle(app)

    assert app.store.snapshot() == snapshot
    assert app.store.data_points() == data
    assert app.store.get_instance_status() == instance
    assert len(update_log(app)) == updates
    assert app.engine.pending_count == 0

    new_rejections = rejections(app)[rejected:]
    assert len(new_rejections) == 1
    assert new_rejections[0].level == ERROR
    assert new_rejections[0].data["reason"]
    # the event itself was logged too
    assert len([e for e in app.log.entries() if e.kind == "event"]) == events + 1


def test_after_a_rejection_the_task_can_still_be_closed_properly(app):
    after_t1(app)
    app.bus.publish(TaskClosed("T2", "maybe"))
    settle(app)
    close(app, "T2", "yes")
    assert app.store.get_status("T2") == "closed"
    assert app.store.data_points()["T2"] == "yes"
