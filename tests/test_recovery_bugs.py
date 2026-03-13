"""Tests for recovery bugs — these should FAIL until fixed.

Gap 3: recover_tasks() picks up tasks that are currently running in the
same process, causing double execution.

Gap 2: enqueue() during executor shutdown creates orphaned DB rows.
"""

import threading
import time

import pytest
from django.tasks import TaskResultStatus, task_backends

from django_tasks_local_db.models import DBTaskResult

from .tasks import counting_task, get_call_count, reset_call_counts, slow_task


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


@pytest.mark.django_db(transaction=True)
def test_enqueue_during_shutdown_no_orphans(backend):
    """enqueue() while executor is shutting down should not orphan DB rows.

    Bug: enqueue() creates the DB row first, then calls executor.submit().
    If the executor is shutting down, submit() raises RuntimeError,
    leaving a READY row in the DB that will never be processed.
    """
    # Enqueue a slow task to keep the executor busy during shutdown
    slow_task.enqueue(2)
    time.sleep(0.1)  # Let it start

    # Start shutdown in a background thread (it blocks waiting for slow_task)
    shutdown_done = threading.Event()

    def do_shutdown():
        backend.close()
        shutdown_done.set()

    t = threading.Thread(target=do_shutdown)
    t.start()

    time.sleep(0.1)  # Let shutdown begin

    # Try to enqueue while shutdown is in progress
    orphan_created = False
    try:
        result = counting_task.enqueue("during_shutdown", 0.1)
        # If enqueue didn't raise, the task should eventually complete
        orphan_created = False
    except RuntimeError:
        # enqueue raised because executor is shutting down —
        # but did it leave a DB row behind?
        orphans = DBTaskResult.objects.filter(
            task_path="tests.tasks.counting_task",
            status=TaskResultStatus.READY,
        )
        orphan_created = orphans.exists()

    shutdown_done.wait(timeout=10)
    # Reset so subsequent tests get a fresh executor
    backend._state = None

    assert not orphan_created, (
        "enqueue() raised RuntimeError but left an orphaned READY row in the DB"
    )
