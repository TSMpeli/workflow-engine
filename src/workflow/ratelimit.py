"""Rolling-window rate limiter.

At most `limit` acquisitions in any rolling `window` seconds. The window is
half-open: an acquisition made exactly `window` seconds ago no longer counts.

Not thread-safe on its own. The engine calls it only while holding its lock.
"""

from __future__ import annotations

from collections import deque

from .clock import Clock


class RollingWindowLimiter:
    def __init__(self, clock: Clock, limit: int = 12, window: float = 60.0) -> None:
        if limit < 1 or window <= 0:
            raise ValueError("limit must be >= 1 and window > 0")
        self._clock = clock
        self._limit = limit
        self._window = window
        self._stamps: deque[float] = deque()

    def _evict(self, now: float) -> None:
        while self._stamps and self._stamps[0] <= now - self._window:
            self._stamps.popleft()

    def try_acquire(self) -> bool:
        """Take a slot if one is free right now."""
        now = self._clock.now()
        self._evict(now)
        if len(self._stamps) < self._limit:
            self._stamps.append(now)
            return True
        return False

    def seconds_until_ready(self) -> float:
        """0.0 if a slot is free now, else how long until the oldest
        acquisition leaves the window."""
        now = self._clock.now()
        self._evict(now)
        if len(self._stamps) < self._limit:
            return 0.0
        return self._stamps[0] + self._window - now

    def used(self) -> int:
        self._evict(self._clock.now())
        return len(self._stamps)

    def clear(self) -> None:
        self._stamps.clear()
