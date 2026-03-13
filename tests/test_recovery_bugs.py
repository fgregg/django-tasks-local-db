"""Tests for watcher-loop recovery behavior."""

import time

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
