"""Time source for the engine.

The engine never calls time.* directly. It asks a Clock, so tests can use
FakeClock and control time deterministically, while the running application
uses RealClock.

Only now() is part of the contract. Waiting (sleeping until the rate limiter
has room) is the runtime's job, not the core's, which keeps the core
synchronous and easy to test.
"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Current time in seconds. Monotonic: never goes backwards."""
        ...


class RealClock:
    """Real time. Uses a monotonic clock so system clock changes (NTP, DST,
    manual edits) cannot break the rolling rate-limit window."""

    def now(self) -> float:
        return time.monotonic()


class FakeClock:
    """Manually controlled clock for tests. Time only moves via advance()."""

    def __init__(self, start: float = 0.0) -> None:
        self._t = float(start)

    def now(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("FakeClock cannot go backwards")
        self._t += seconds
