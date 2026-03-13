"""Tests for watcher-loop recovery behavior."""

import time
from unittest.mock import patch

import pytest
from django.tasks import TaskResultStatus, task_backends
from django.utils import timezone

from django_tasks_local_db.models import DBTaskResult

from .tasks import counting_task, get_call_count, reset_call_counts


@pytest.fixture
def backend():
    return task_backends["default"]


@pytest.fixture(autouse=True)
def _reset_counters():
    reset_call_counts()
    yield
    reset_call_counts()


@pytest.mark.django_db(transaction=True)
def test_watcher_does_not_double_dispatch_running_tasks(backend):
    """The watcher loop should not re-dispatch tasks with fresh heartbeats.

    The watcher dispatches READY tasks via due() and recovers stale RUNNING
    tasks. A RUNNING task with a recent heartbeat should NOT be re-dispatched.
    """
    # Enqueue a slow task — the watcher will dispatch it
    result = counting_task.enqueue("watcher_test", 2)

    # Wait for it to start running
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        db_result = DBTaskResult.objects.get(id=result.id)
        if db_result.status == TaskResultStatus.RUNNING:
            break
        time.sleep(0.05)
    assert db_result.status == TaskResultStatus.RUNNING

    # Manually trigger a watcher tick — it should not re-dispatch the task
    # because the heartbeat is fresh (within HEARTBEAT_TIMEOUT)
    backend._watcher_tick()

    # Wait for the task to finish
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        db_result.refresh_from_db()
        if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            break
        time.sleep(0.05)

    assert db_result.status == TaskResultStatus.SUCCESSFUL

    # The task should have been called exactly once
    count = get_call_count("watcher_test")
    assert count == 1, (
        f"Task was executed {count} times — watcher double-dispatched"
    )


@pytest.mark.django_db(transaction=True)
def test_watcher_recovers_stale_tasks(backend):
    """The watcher should recover tasks with stale heartbeats."""
    # Create a RUNNING task with an old heartbeat (simulating a dead worker)
    stale_time = timezone.now() - timezone.timedelta(seconds=60)
    db_result = DBTaskResult.objects.create(
        args_kwargs={"args": [1, 2], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
        status=TaskResultStatus.RUNNING,
        started_at=stale_time,
        last_heartbeat_at=stale_time,
    )

    # Trigger a watcher tick — should recover the stale task
    backend._watcher_tick()

    # Wait for recovery
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        db_result.refresh_from_db()
        if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            break
        time.sleep(0.05)

    assert db_result.status == TaskResultStatus.SUCCESSFUL
    assert db_result.return_value == 3


@pytest.mark.django_db(transaction=True)
def test_cross_process_watcher_no_double_dispatch(backend):
    """Two processes' watchers should NOT double-dispatch the same READY task.

    Because claim() runs inside the SELECT FOR UPDATE transaction, the task
    transitions to RUNNING before the lock is released. A second watcher's
    tick will not see the task as READY.
    """
    from django_tasks_local_db.backend import LocalDBBackend

    db_result = DBTaskResult.objects.create(
        args_kwargs={"args": ["race_test", 2], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.counting_task",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )

    # Create a second backend instance simulating a second process
    backend_b = LocalDBBackend("default", {
        "BACKEND": "django_tasks_local_db.LocalDBBackend",
        "OPTIONS": {"MAX_WORKERS": 25},
    })

    try:
        # Process A's watcher picks up and claims the READY task
        backend._watcher_tick()

        # Task should now be RUNNING (claimed inside the transaction)
        db_result.refresh_from_db()
        assert db_result.status == TaskResultStatus.RUNNING, (
            f"Expected RUNNING after first tick, got {db_result.status}"
        )

        # Process B's watcher should NOT pick it up — it's no longer READY
        backend_b._watcher_tick()

        # Wait for the task to finish
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            db_result.refresh_from_db()
            if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
                break
            time.sleep(0.05)

        assert db_result.status == TaskResultStatus.SUCCESSFUL

        # Wait a bit for any second execution to finish if it was dispatched
        time.sleep(1)

        # The task should have been called exactly once
        count = get_call_count("race_test")
        assert count == 1, (
            f"Task was executed {count} times — cross-process race caused double dispatch"
        )
    finally:
        # Only stop backend_b's watcher — don't shut down the shared executor
        # (both backends share the same executor keyed by "tasks-default")
        backend_b._watcher_stop.set()
        if backend_b._watcher_thread is not None:
            backend_b._watcher_thread.join(timeout=5)
        backend_b._watcher_started = False
