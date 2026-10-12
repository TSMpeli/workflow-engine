"""Scenario 2: unreachable tasks are cancelled, closed tasks keep their status."""

from helpers import close, reach_t13, run, start, status
from workflow.flow import initial_statuses, plan


def test_examples_from_the_spec(app):
    start(app)
    run(app, "T1", ("T2", "yes"))
    assert status(app, "T3") == "cancelled"  # T2 = yes

    close(app, "T6", "yes")
    assert status(app, "T9") == status(app, "T10") == "cancelled"  # T6 = yes


def test_t2_no_cancels_everything_from_t4_to_t15(app):
    start(app)
    run(app, "T1", ("T2", "no"))
    s = app.store.snapshot()
    assert [s[f"T{i}"] for i in range(4, 16)] == ["cancelled"] * 12


def test_t13_yes_cancels_follow_up_if_never_used(app):
    reach_t13(app)
    close(app, "T13", "yes")
    assert status(app, "T15") == "cancelled"


def test_t13_yes_keeps_follow_up_closed_from_an_earlier_pass(app):
    reach_t13(app)
    close(app, "T13", "no")
    close(app, "T15")
    close(app, "T13", "yes")
    assert status(app, "T15") == "closed"


def test_plan_never_cancels_a_closed_task():
    statuses = initial_statuses()
    statuses["T2"] = "available"
    statuses["T5"] = "closed"  # hypothetical, to exercise the rule directly
    updates = plan(statuses, "T2", "no")
    touched = [u.task_id for u in updates]
    assert "T5" not in touched
    assert len(updates) == 13  # 14 minus the closed task


def test_every_task_ends_closed_or_cancelled_when_the_order_finishes(app):
    reach_t13(app)
    close(app, "T13", "yes")
    assert set(app.store.snapshot().values()) <= {"closed", "cancelled"}


def test_cancelling_an_end_task_does_not_finish_the_order(app):
    """T3 is cancelled when the contract is signed: the order keeps running."""
    start(app)
    run(app, "T1", ("T2", "yes"))
    assert status(app, "T3") == "cancelled"
    assert app.store.get_instance_status() == "running"


def test_refused_contract_ends_as_cancelled_not_completed(app):
    """T14 is cancelled along with T4..T15; only T3 closing ends the order."""
    start(app)
    run(app, "T1", ("T2", "no"))
    assert status(app, "T14") == "cancelled"
    assert app.store.get_instance_status() == "cancelled"
