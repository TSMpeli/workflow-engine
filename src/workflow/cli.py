"""Command-line front end.

It only talks to the system through the event bus (to act) and by reading the
state store and log (to observe), exactly like the architecture diagram.
"""

from __future__ import annotations

import os
import sys
import threading

from .app import Application, build_app
from .clock import RealClock
from .events import Event, InstanceStarted, SystemReset, TaskClosed
from .flow import DATA_POINTS, TASKS
from .runtime import Runtime

HELP = """commands:
  start                 start a new order
  close TASK [yes|no]   close a task, e.g. "close T1" or "close T2 yes"
                        (T2, T6 and T13 carry a data point and need yes or no)
  reset                 wipe all state, waiting updates and the rate-limit window
  status                show the task table again
  log                   show every log entry
  help                  show this text
  quit                  leave"""

_COLORS = {
    "available": "\033[1;32m",
    "closed": "\033[2m",
    "cancelled": "\033[31m",
    "pending": "",
}
_RESET = "\033[0m"


class UsageError(Exception):
    pass


def parse_command(line: str) -> Event | str | None:
    """Turn a line of input into an event to publish, a local command name,
    or None for an empty line. Raises UsageError for malformed input.

    The value is passed through exactly as typed: the engine, not the front
    end, decides what is valid."""
    parts = line.split()
    if not parts:
        return None
    cmd, args = parts[0].lower(), parts[1:]
    if cmd == "start" and not args:
        return InstanceStarted()
    if cmd == "reset" and not args:
        return SystemReset()
    if cmd == "close":
        if len(args) not in (1, 2):
            raise UsageError("usage: close TASK [yes|no]")
        return TaskClosed(args[0].upper(), args[1] if len(args) == 2 else None)
    if cmd in ("status", "log", "help", "quit", "exit") and not args:
        return cmd
    raise UsageError(f"unknown command {line.strip()!r}, type 'help'")


def render_table(app: Application, color: bool = False) -> str:
    snapshot = app.store.snapshot()
    data = app.store.data_points()
    backlog = app.engine.backlog()
    header = f"order: {app.store.get_instance_status()}"
    if backlog.updates or backlog.events:
        header += f"   waiting: {backlog.updates} update(s), {backlog.events} event(s)"
        if backlog.updates:
            header += f", next update in {backlog.seconds:.1f}s"
    lines = [header]
    for task_id, status in snapshot.items():
        marker = ">" if status == "available" else " "
        point = ""
        if task_id in DATA_POINTS:
            point = f"  {DATA_POINTS[task_id]} = {data[task_id]}" if task_id in data else f"  ({DATA_POINTS[task_id]}?)"
        row = f" {marker} {task_id:<4}{TASKS[task_id]:<34}{status:<10}{point}"
        if color and _COLORS[status]:
            row = f"{_COLORS[status]}{row}{_RESET}"
        lines.append(row)
    return "\n".join(lines)


class Display:
    """Prints new log entries and the full task table. Called after every
    command, and by a watcher thread so changes caused by the rate limiter
    (updates that were waiting) show up on their own."""

    def __init__(self, app: Application, color: bool) -> None:
        self._app = app
        self._color = color
        self._t0 = app.clock.now()
        self._seen = 0
        self._key: object = None
        self._lock = threading.Lock()

    def _format(self, entry) -> str:
        return f"  [{entry.seq:03d}] {entry.time - self._t0:7.2f}s {entry.level:<5} {entry.message}"

    def refresh(self, force: bool = False) -> None:
        with self._lock:
            entries = self._app.log.entries()
            new = entries[self._seen :]
            self._seen = len(entries)
            backlog = self._app.engine.backlog()
            key = (
                tuple(self._app.store.snapshot().items()),
                self._app.store.get_instance_status(),
                backlog.updates,
                backlog.events,
            )
            if not (new or force or key != self._key):
                return
            self._key = key
            for entry in new:
                print(self._format(entry))
            print(render_table(self._app, self._color))
            print(flush=True)

    def print_log(self) -> None:
        with self._lock:
            for entry in self._app.log.entries():
                print(self._format(entry))


def main() -> int:
    app = build_app(RealClock())
    runtime = Runtime(app.engine, app.bus)
    runtime.start()
    color = sys.stdout.isatty() and "NO_COLOR" not in os.environ
    display = Display(app, color)

    stop = threading.Event()

    def watch() -> None:
        while not stop.wait(0.2):
            display.refresh()

    threading.Thread(target=watch, name="display-watcher", daemon=True).start()

    print("Order fulfillment workflow engine. Type 'help' for commands.\n")
    display.refresh(force=True)
    try:
        while True:
            try:
                line = input("> ")
            except EOFError:
                break
            try:
                action = parse_command(line)
            except UsageError as exc:
                print(exc)
                continue
            if action is None:
                continue
            if action in ("quit", "exit"):
                break
            if action == "help":
                print(HELP)
            elif action == "log":
                display.print_log()
            elif action == "status":
                display.refresh(force=True)
            else:
                app.bus.publish(action)
                display.refresh(force=True)
    except KeyboardInterrupt:
        print()
    finally:
        stop.set()
        runtime.stop()
    return 0
