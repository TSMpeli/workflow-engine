"""Small helpers shared by the tests. Everything goes through the bus."""

from workflow.events import InstanceStarted, TaskClosed


def settle(app):
    """Pump the engine, moving the fake clock forward whenever the rate limit
    makes it wait, until there is nothing left to do."""
    while True:
        wait = app.engine.pump()
        if wait is None:
            return
        app.clock.advance(wait)


def start(app):
    app.bus.publish(InstanceStarted())
    settle(app)


def close(app, task_id, value=None):
    app.bus.publish(TaskClosed(task_id, value))
    settle(app)


def run(app, *steps):
    """run(app, "T1", ("T2", "yes"), "T4"): close each task in turn."""
    for step in steps:
        if isinstance(step, tuple):
            close(app, *step)
        else:
            close(app, step)


def update_log(app):
    return [e for e in app.log.entries() if e.kind == "update"]


def updated_ids(app):
    return [e.data["task_id"] for e in update_log(app)]


def rejections(app):
    return [e for e in app.log.entries() if e.kind == "rejection"]


def status(app, task_id):
    return app.store.get_status(task_id)


def reach_t13(app):
    """Start an order and play it (stock available) until T13 is available."""
    start(app)
    run(app, "T1", ("T2", "yes"), ("T6", "yes"), "T4", "T5", "T7", "T11", "T12", "T8")
    assert status(app, "T13") == "available"
