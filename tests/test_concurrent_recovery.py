"""Tests for multi-process concurrent recovery (issue #7)."""

import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from django.tasks import TaskResultStatus, task_backends

from django_tasks_local_db.models import DBTaskResult


@pytest.fixture
def backend():
    return task_backends["default"]


def _wait_for_db_result(db_result, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        db_result.refresh_from_db()
        if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            return
        time.sleep(0.05)
    raise TimeoutError(f"DB task {db_result.id} did not finish within {timeout}s")


@pytest.mark.django_db(transaction=True)
def test_concurrent_recovery_no_double_execution(backend):
    """Two concurrent recover_tasks() calls should not double-execute tasks.

    select_for_update(skip_locked=True) should prevent this, but SQLite
    doesn't support row-level locking so this test exposes that gap.
    """
    # Create several orphaned tasks
    orphans = []
    for i in range(5):
        db_result = DBTaskResult.objects.create(
            args_kwargs={"args": [i, i], "kwargs": {}},
            priority=0,
            task_path="tests.tasks.add_numbers",
            queue_name="default",
            backend_name="default",
            exception_class_path="",
            traceback="",
        )
        orphans.append(db_result)

    # Run recover_tasks from two threads concurrently
    with ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(backend.recover_tasks)
        f2 = executor.submit(backend.recover_tasks)

        recovered1 = f1.result(timeout=10)
        recovered2 = f2.result(timeout=10)

    total_recovered = recovered1 + recovered2

    # Each task should be recovered exactly once — no double execution
    assert total_recovered == 5, (
        f"Expected 5 total recovered, got {recovered1} + {recovered2} = {total_recovered}"
    )

    # Wait for all tasks to finish
    for db_result in orphans:
        _wait_for_db_result(db_result)


@pytest.mark.django_db(transaction=True)
def test_concurrent_recovery_worker_ids(backend):
    """Check how many times a task gets claimed during recovery."""
    db_result = DBTaskResult.objects.create(
        args_kwargs={"args": [1, 1], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )

    backend.recover_tasks()
    _wait_for_db_result(db_result)

    db_result.refresh_from_db()
    # recover_tasks calls claim() once, then _execute_task calls claim() again
    print(f"worker_ids: {db_result.worker_ids}")
    print(f"worker_ids count: {len(db_result.worker_ids)}")
    # Document current behavior: claim() is called twice in the recovery path
    assert len(db_result.worker_ids) == 2, (
        f"Expected 2 worker_ids (recover + execute), got {len(db_result.worker_ids)}: {db_result.worker_ids}"
    )
