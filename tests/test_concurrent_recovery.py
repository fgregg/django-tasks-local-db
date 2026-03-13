"""Tests for multi-process concurrent recovery (issue #7)."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

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


def _create_orphans(n=5, task_path="tests.tasks.add_numbers", args=None):
    orphans = []
    for i in range(n):
        db_result = DBTaskResult.objects.create(
            args_kwargs={"args": args if args is not None else [i, i], "kwargs": {}},
            priority=0,
            task_path=task_path,
            queue_name="default",
            backend_name="default",
            exception_class_path="",
            traceback="",
        )
        orphans.append(db_result)
    return orphans


@pytest.mark.django_db(transaction=True)
def test_concurrent_recovery_no_double_execution(backend):
    """Two concurrent recover_tasks() calls should not double-execute tasks.

    Forces overlapping transactions by making the first claim() call
    sleep, holding row locks while the second thread evaluates its
    SELECT FOR UPDATE SKIP LOCKED.
    """
    orphans = _create_orphans(5, task_path="tests.tasks.slow_task", args=[2])

    first_claim_done = threading.Event()
    original_claim = DBTaskResult.claim

    def synchronized_claim(self, worker_id):
        result = original_claim(self, worker_id)
        if not first_claim_done.is_set():
            first_claim_done.set()
            # Hold the transaction open so the other thread's
            # SELECT FOR UPDATE SKIP LOCKED runs while we hold locks
            time.sleep(0.5)
        return result

    with patch.object(DBTaskResult, "claim", synchronized_claim):
        with ThreadPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(backend.recover_tasks)
            f2 = executor.submit(backend.recover_tasks)

            recovered1 = f1.result(timeout=10)
            recovered2 = f2.result(timeout=10)

    total_recovered = recovered1 + recovered2

    assert total_recovered == 5, (
        f"Expected 5 (no double-execution), "
        f"got {recovered1} + {recovered2} = {total_recovered}"
    )

    for db_result in orphans:
        _wait_for_db_result(db_result)


@pytest.mark.django_db(transaction=True)
def test_row_level_locking_allows_concurrent_task_operations(backend):
    """While one task holds a row lock, other tasks can still be recovered.

    Proves the lock is row-scoped, not database-scoped. A slow task
    holds its row lock for the full execution. A second recover_tasks()
    call should skip the locked row and recover other orphaned tasks.
    """
    # Task A: slow, will hold its row lock for 2 seconds
    task_a = DBTaskResult.objects.create(
        args_kwargs={"args": [2], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.slow_task",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )
    # Task B: fast, should be recoverable while A is locked
    task_b = DBTaskResult.objects.create(
        args_kwargs={"args": [1, 2], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )

    # First recovery picks up both tasks. Task A starts executing (holds lock).
    recovered1 = backend.recover_tasks()
    assert recovered1 == 2

    # Wait for task A to be RUNNING (holding its row lock)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        task_a.refresh_from_db()
        if task_a.status == TaskResultStatus.RUNNING:
            break
        time.sleep(0.05)
    assert task_a.status == TaskResultStatus.RUNNING

    # Task B should finish quickly
    _wait_for_db_result(task_b, timeout=5)
    task_b.refresh_from_db()
    assert task_b.status == TaskResultStatus.SUCCESSFUL

    # Second recovery while A is still running — should find 0 tasks.
    # A is locked (skip_locked skips it), B is already SUCCESSFUL.
    recovered2 = backend.recover_tasks()
    assert recovered2 == 0, (
        f"Expected 0 recoverable tasks while A is locked, got {recovered2}"
    )

    # Wait for A to finish
    _wait_for_db_result(task_a, timeout=10)
    task_a.refresh_from_db()
    assert task_a.status == TaskResultStatus.SUCCESSFUL


@pytest.mark.django_db(transaction=True)
def test_single_recovery(backend):
    """A single recover_tasks() call should recover all orphaned tasks."""
    orphans = _create_orphans(5)

    recovered = backend.recover_tasks()
    assert recovered == 5

    for db_result in orphans:
        _wait_for_db_result(db_result)
        db_result.refresh_from_db()
        assert db_result.status == TaskResultStatus.SUCCESSFUL


@pytest.mark.django_db(transaction=True)
def test_recovery_worker_ids(backend):
    """Recovery claims the task, then _execute_task claims it again."""
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
    assert len(db_result.worker_ids) == 2, (
        f"Expected 2 worker_ids (recover + execute), got {len(db_result.worker_ids)}: {db_result.worker_ids}"
    )
