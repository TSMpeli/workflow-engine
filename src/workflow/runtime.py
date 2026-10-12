"""Real-time driver: a worker thread that keeps pumping the engine.

The engine pumps itself whenever an event arrives. This thread exists for the
other case: updates that are waiting for the rate limit. It sleeps until the
limiter has room (or until a new event wakes it), then pumps again.
"""

from __future__ import annotations

import threading

from .engine import Engine
from .events import Event
from .ports import EventBus


class Runtime:
    def __init__(self, engine: Engine, bus: EventBus) -> None:
        self._engine = engine
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="engine-runtime", daemon=True)
        # Subscribed after the engine, so the engine has already handled the
        # event (and any reset) by the time this wakes the worker.
        bus.subscribe(self._on_event)

    def _on_event(self, _event: Event) -> None:
        self._wake.set()

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.is_set():
            wait = self._engine.pump()  # None means idle: sleep until woken
            self._wake.wait(timeout=wait)
            self._wake.clear()
