"""Wiring: builds a complete engine with in-memory bus, store and log."""

from __future__ import annotations

from dataclasses import dataclass

from .clock import Clock, RealClock
from .engine import Engine
from .memory import InMemoryEventBus, InMemoryLogSystem, InMemoryStateStore
from .ports import StateStore
from .ratelimit import RollingWindowLimiter


@dataclass
class Application:
    clock: Clock
    bus: InMemoryEventBus
    store: StateStore
    log: InMemoryLogSystem
    limiter: RollingWindowLimiter
    engine: Engine


def build_app(
    clock: Clock | None = None,
    *,
    limit: int = 12,
    window: float = 60.0,
    store: StateStore | None = None,
) -> Application:
    clock = clock or RealClock()
    bus = InMemoryEventBus()
    store = store or InMemoryStateStore()
    log = InMemoryLogSystem(clock)
    limiter = RollingWindowLimiter(clock, limit=limit, window=window)
    engine = Engine(bus, store, log, limiter)
    return Application(clock, bus, store, log, limiter, engine)
