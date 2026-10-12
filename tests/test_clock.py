import pytest

from workflow.clock import FakeClock, RealClock


def test_fake_clock_starts_where_told_and_only_moves_on_advance():
    clock = FakeClock(start=10.0)
    assert clock.now() == 10.0
    assert clock.now() == 10.0
    clock.advance(2.5)
    assert clock.now() == 12.5


def test_fake_clock_rejects_negative_advance():
    with pytest.raises(ValueError):
        FakeClock().advance(-1)


def test_real_clock_is_monotonic():
    clock = RealClock()
    a = clock.now()
    b = clock.now()
    assert b >= a
