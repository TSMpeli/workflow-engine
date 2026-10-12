"""Scenario 1: the paths through the flow."""

import pytest

from helpers import close, reach_t13, run, start, status, update_log
from workflow.flow import plan, initial_statuses


def test_start_makes_t1_available(app):
    start(app)
    assert status(app, "T1") == "available"
    assert app.store.get_instance_status() == "running"
    assert len(update_log(app)) == 1


def test_t1_opens_t2(app):
    start(app)
    close(app, "T1")
    assert status(app, "T1") == "closed"
    assert status(app, "T2") == "available"


def test_contract_signed_splits_into_three_branches(app):
    start(app)
    close(app, "T1")
    before = len(update_log(app))
    close(app, "T2", "yes")
    assert len(update_log(app)) - before == 5
    s = app.store.snapshot()
    assert s["T2"] == "closed"
    assert s["T3"] == "cancelled"
    assert s["T4"] == s["T5"] == s["T6"] == "available"
    assert app.store.data_points()["T2"] == "yes"


def test_contract_refused_cancels_the_order(app):
    start(app)
    close(app, "T1")
    before = len(update_log(app))
    close(app, "T2", "no")
    updates = update_log(app)[before:]
    assert len(updates) == 14
    assert updates[0].data["task_id"] == "T2"
    assert updates[-1].data["task_id"] == "T3"  # end task closes last
    s = app.store.snapshot()
    assert s["T1"] == s["T2"] == s["T3"] == "closed"
    assert all(s[f"T{i}"] == "cancelled" for i in range(4, 16))
    assert app.store.get_instance_status() == "cancelled"


def test_stock_available_skips_purchase(app):
    start(app)
    run(app, "T1", ("T2", "yes"), ("T6", "yes"))
    s = app.store.snapshot()
    assert s["T9"] == s["T10"] == "cancelled"
    assert s["T11"] == "available"


def test_stock_missing_goes_through_purchase_and_receipt(app):
    start(app)
    run(app, "T1", ("T2", "yes"), ("T6", "no"))
    s = app.store.snapshot()
    assert s["T9"] == "available"
    assert s["T10"] == "pending"
    assert s["T11"] == "pending"  # not a join, but not reachable yet either
    close(app, "T9")
    assert status(app, "T10") == "available"
    assert status(app, "T11") == "pending"
    close(app, "T10")
    assert status(app, "T11") == "available"


def test_billing_branch_chain(app):
    start(app)
    run(app, "T1", ("T2", "yes"))
    assert status(app, "T7") == "pending"
    close(app, "T4")
    assert status(app, "T7") == "available"


@pytest.mark.parametrize("first,second", [("T7", "T11"), ("T11", "T7")])
def test_first_and_join_waits_for_both_branches(app, first, second):
    start(app)
    run(app, "T1", ("T2", "yes"), ("T6", "yes"), "T4")
    close(app, first)
    assert status(app, "T12") == "pending"
    close(app, second)
    assert status(app, "T12") == "available"


@pytest.mark.parametrize("first,second", [("T12", "T8"), ("T8", "T12")])
def test_second_and_join_waits_for_both_branches(app, first, second):
    start(app)
    run(app, "T1", ("T2", "yes"), ("T6", "yes"), "T4", "T5", "T7", "T11")
    assert status(app, "T12") == "available"
    close(app, first)
    assert status(app, "T13") == "pending"
    close(app, second)
    assert status(app, "T13") == "available"


def test_installation_loop_repeats_until_full_installation(app):
    reach_t13(app)
    for _ in range(3):
        before = len(update_log(app))
        close(app, "T13", "no")
        assert len(update_log(app)) - before == 2
        s = app.store.snapshot()
        assert s["T13"] == "closed"
        assert s["T15"] == "available"
        assert s["T14"] == "pending"  # still reachable, so NOT cancelled
        close(app, "T15")
        s = app.store.snapshot()
        assert s["T15"] == "closed"
        assert s["T13"] == "available"  # closed -> available again

    close(app, "T13", "yes")
    s = app.store.snapshot()
    assert s["T13"] == "closed"
    assert s["T15"] == "closed"  # closed on an earlier pass: keeps its status
    assert s["T14"] == "closed"
    assert app.store.get_instance_status() == "completed"


def test_full_installation_on_first_pass_completes_the_order(app):
    reach_t13(app)
    close(app, "T13", "yes")
    s = app.store.snapshot()
    assert s["T14"] == "closed"
    assert s["T15"] == "cancelled"
    assert app.store.get_instance_status() == "completed"
    closed = {t for t, v in s.items() if v == "closed"}
    cancelled = {t for t, v in s.items() if v == "cancelled"}
    assert closed == {"T1", "T2", "T4", "T5", "T6", "T7", "T8", "T11", "T12", "T13", "T14"}
    assert cancelled == {"T3", "T9", "T10", "T15"}


def test_full_run_through_the_purchase_branch(app):
    start(app)
    run(app, "T1", ("T2", "yes"), ("T6", "no"), "T9", "T10", "T11", "T4", "T7", "T5", "T8", "T12")
    assert status(app, "T13") == "available"
    close(app, "T13", "yes")
    s = app.store.snapshot()
    assert s["T9"] == s["T10"] == "closed"
    assert set(s.values()) <= {"closed", "cancelled"}
    assert app.store.get_instance_status() == "completed"


def test_a_new_order_can_start_after_one_finished(app):
    start(app)
    run(app, "T1", ("T2", "no"))
    assert app.store.get_instance_status() == "cancelled"
    start(app)
    s = app.store.snapshot()
    assert s["T1"] == "available"
    assert all(v == "pending" for t, v in s.items() if t != "T1")
    assert app.store.data_points() == {}
    assert app.store.get_instance_status() == "running"


def test_data_point_keeps_the_latest_loop_value(app):
    reach_t13(app)
    close(app, "T13", "no")
    assert app.store.data_points()["T13"] == "no"
    close(app, "T15")
    close(app, "T13", "yes")
    assert app.store.data_points()["T13"] == "yes"


def test_plan_is_pure():
    statuses = initial_statuses()
    statuses["T2"] = "available"
    snapshot = dict(statuses)
    plan(statuses, "T2", "no")
    assert statuses == snapshot
