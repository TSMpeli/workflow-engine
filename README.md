# Order Fulfillment Workflow Engine

A small event-driven engine that runs one order at a time through a locked
15-task process (see the exercise brief), with a rolling rate limit, a
command-line front end and a test suite that drives everything through the
event bus.

Pure Python standard library. Python 3.10 or newer. `pytest` is the only
development dependency.

## Run it

```
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

python -m workflow               # start the CLI (also installed as `workflow`)
pytest                           # run the tests
```

You can also run without installing: `PYTHONPATH=src python -m workflow`.

### Using the CLI

```
start                 start a new order
close TASK [yes|no]     close a task, e.g. "close T1" or "close T2 yes"
reset                 wipe all state, waiting updates and the rate-limit window
status                show the task table again
log                   show every log entry
help, quit
```

After every command the CLI prints the new log lines and the status of all 15
tasks. Tasks you can close right now are marked with `>`. Rejections appear as
`ERROR` log lines. If the rate limit is holding updates back, the header shows
how many are waiting and when the next one will be applied; the table refreshes
by itself when they are.

Example:

```
> start
  [003]    0.00s INFO  T1: pending -> available
order: running
 > T1  Receive order                     available
...
> close T2
  [012]    0.00s ERROR task.closed rejected: task T2 needs contract_signed to be exactly 'yes' or 'no', got None
> close T2 yes
  [014]    0.00s INFO  T2: available -> closed
  [015]    0.00s INFO  T3: pending -> cancelled
  [016]    0.00s INFO  T4: pending -> available
  ...
```

## Design

```
Front end -> Event bus -> Engine -> State store
                            |
                            +-----> Log system        (front end reads store and log)
```

Each box is behind its own interface (`ports.py`) with an in-memory
implementation (`memory.py`). The engine receives only `instance.started`,
`task.closed` and `system.reset`, and is the only writer of the state store.

| Module | Responsibility |
|---|---|
| `flow.py` | The locked process as data, plus two pure functions: `validate_close` (should this event be rejected?) and `plan` (which updates does it cause, in order?). No I/O, no time. |
| `engine.py` | Queues, the pump loop, applying updates, rejections, reset. |
| `ratelimit.py` | Rolling-window limiter (12 per 60 s by default). |
| `clock.py` | `Clock` protocol with `RealClock` (monotonic) and `FakeClock` (tests). |
| `runtime.py` | Worker thread that applies waiting updates in real time. |
| `memory.py` | In-memory bus, store and log. |
| `cli.py` | The front end. It only publishes events and reads store and log. |

### The pump model

The engine keeps two FIFO queues: `inbox` (events not yet handled) and
`pending` (planned task updates not yet applied). `pump()` loops:

1. If an update is pending, apply it if the rate limiter has room. If not, stop
   and return how many seconds to wait.
2. Otherwise, if an event is waiting, validate it against the current state and
   append its planned updates to `pending`.
3. Otherwise, stop.

This gives the required behaviour with very little machinery:

* **One update at a time.** Everything runs under one lock, and one update is
  applied per loop iteration.
* **Events are handled in arrival order**, and each is judged only after every
  update of the previous event has been applied, so validation always sees a
  consistent state.
* **The core never sleeps.** It reports how long to wait instead. Tests pass a
  `FakeClock` and call `pump()` after advancing it, so rate-limit tests are
  instant and deterministic. In the real application, `Runtime` is a thread
  that sleeps for the reported time (or until a new event wakes it) and pumps.

### Rate limiting

`RollingWindowLimiter` keeps the timestamps of applied updates. An update is
allowed if fewer than 12 timestamps are newer than `now - 60s`. It is a true
rolling window, not a fixed one-minute bucket. Every task update counts,
including cancellations and the automatic closing of T3 and T14. An event can
be cut in the middle by the limit: its remaining updates simply wait.

### Reset

`system.reset` is handled immediately (it jumps the queue) while holding the
engine lock. It clears `inbox` and `pending`, clears the limiter, and wipes the
store. Because a waiting update lives in `pending` and is applied only under
the same lock, nothing from before a reset can be applied afterwards, even if
the worker thread is asleep at that moment.

### Flow representation

`plan()` works on a private copy of the statuses and applies each planned
update to that copy as it goes, so later steps in the same event (AND joins,
"cancel unless already closed") see the effect of earlier steps. Joins are
simply "all inputs are closed", checked whenever one input closes. T11 is not a
join: it is opened by T6 = yes or by T10 closing.

Update order within an event is deterministic and matches the brief: the
closed task first, then cancellations, then newly reached tasks. For `T2 = no`
that is T2 closed, T4 to T15 cancelled, T3 closed (14 updates, T3 last).

## Assumptions

1. **Events are judged when they are processed, not when published.** If the
   rate limit is holding updates back, later events wait in the inbox and are
   validated after the earlier events' updates have been applied. This is what
   makes "one update at a time" and strict ordering well defined. A consequence
   is that an event can be valid when it is handled even though the front end
   did not yet show its task as available when it was sent.
2. **The window is half-open.** An update that is exactly 60 seconds old no
   longer counts toward the limit.
3. **A value on a task without a data point is rejected** (for example
   `close T1 yes`). A missing or invalid value on T2, T6 or T13 is rejected as
   specified. The value must be the exact lowercase string `yes` or `no`.
4. **The order finishes when the end task is actually closed**, not when it is
   planned. If the update that closes T3 or T14 is waiting for the rate limit,
   the order is still `running`. Cancelling T3 (contract signed) or T14
   (contract refused) does not finish the order.
5. **Starting a new order after one finished** wipes the old order's state (all
   tasks `pending`, data points cleared) and then makes T1 available. The wipe
   is not counted as task updates, and the rate-limit window is not cleared
   (only a reset clears it). T1 becoming available is 1 update, as specified.
6. **T14 stays `pending` while the installation loop can still repeat.** It is
   only cancelled by `T2 = no`.
7. **Data points** keep the latest value (T13 can be closed several times).
8. **Log timestamps** come from the injected clock. With `RealClock` that is a
   monotonic clock, so the CLI shows them as seconds since it started.
9. **Rejections are shown from the log**: the front end reads `ERROR` entries
   from the log system, as in the architecture diagram.

## Tests

`pytest` runs everything. All tests drive the engine through the event bus and
assert on the state store and the log. They use `FakeClock`, except
`test_runtime.py`, which uses real time with a very small window.

| Brief scenario | File |
|---|---|
| 1. Paths: cancellation, both stock branches, both AND joins, loop | `test_flow_paths.py` |
| 2. Unreachable tasks cancelled, closed tasks keep status | `test_cancellation.py` |
| 3. The 13th update waits and is applied in order | `test_rate_limit.py` |
| 4. One update at a time, even for simultaneous events | `test_serial.py` |
| 5. Every rejection leaves state unchanged and is logged | `test_rejections.py` |
| 6. Reset clears state, waiting updates and the window | `test_reset.py` |

Also covered: the limiter on its own (rolling window, exact boundary), the
real-time worker thread, and CLI parsing and rendering.
