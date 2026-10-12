import pytest

from helpers import start
from workflow.cli import UsageError, parse_command, render_table
from workflow.events import InstanceStarted, SystemReset, TaskClosed
from workflow.flow import TASK_IDS


def test_parse_commands():
    assert parse_command("start") == InstanceStarted()
    assert parse_command("reset") == SystemReset()
    assert parse_command("close t2 yes") == TaskClosed("T2", "yes")
    assert parse_command("close T1") == TaskClosed("T1", None)
    assert parse_command("   ") is None
    assert parse_command("quit") == "quit"


def test_value_is_passed_through_exactly_as_typed():
    assert parse_command("close T2 YES") == TaskClosed("T2", "YES")


@pytest.mark.parametrize("line", ["close", "close T1 yes extra", "dance", "start now"])
def test_malformed_commands_raise_usage_error(line):
    with pytest.raises(UsageError):
        parse_command(line)


def test_table_shows_every_task_and_the_order_status(app):
    start(app)
    table = render_table(app)
    for task_id in TASK_IDS:
        assert task_id + " " in table
    assert "order: running" in table
    assert "available" in table
