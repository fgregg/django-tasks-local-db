"""Tests for recovery of in-flight tasks."""

import time

import pytest
from django.tasks import TaskResultStatus, task_backends

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
def test_recover_tasks_does_not_double_execute_inflight(backend):
    """recover_tasks() should not re-submit tasks that are currently running.

    Bug: recover_tasks() queries for status IN (READY, RUNNING) and
    resubmits them all. But a RUNNING task may be actively executing
    in the current process's thread pool — not orphaned. Re-submitting
    it causes the task function to execute twice.
    """
    # Enqueue a slow task — it will be RUNNING in the thread pool
    result = counting_task.enqueue("inflight_test", 2)

    # Wait for it to start running
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        db_result = DBTaskResult.objects.get(id=result.id)
        if db_result.status == TaskResultStatus.RUNNING:
            break
        time.sleep(0.05)
    assert db_result.status == TaskResultStatus.RUNNING

    # Now call recover_tasks() while the task is still running.
    # This SHOULD recover 0 tasks (nothing is orphaned).
    recovered = backend.recover_tasks()

    # Wait for everything to finish
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        db_result.refresh_from_db()
        if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            break
        time.sleep(0.05)

    assert db_result.status == TaskResultStatus.SUCCESSFUL

    # The task should have been called exactly once
    count = get_call_count("inflight_test")
    assert count == 1, (
        f"Task was executed {count} times — recover_tasks() caused double execution"
    )

    # recover_tasks should not have claimed any in-flight tasks
    assert recovered == 0, (
        f"recover_tasks() recovered {recovered} tasks that were actively running"
    )
