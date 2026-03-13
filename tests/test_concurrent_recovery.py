"""Tests for multi-process concurrent recovery (issue #7)."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest
from django.db import connection
from django.tasks import TaskResultStatus, task_backends

from django_tasks_local_db.models import DBTaskResult

_supports_skip_locked = connection.features.has_select_for_update_skip_locked


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


@pytest.mark.skipif(
    not _supports_skip_locked,
    reason="Database does not support select_for_update(skip_locked=True)",
)
@pytest.mark.django_db(transaction=True)
def test_concurrent_recovery_no_double_execution(backend):
    """Two concurrent recover_tasks() calls should not double-execute tasks.

    Forces overlapping transactions by making the first claim() call
    sleep, holding row locks while the second thread evaluates its
    SELECT FOR UPDATE SKIP LOCKED.
    """
    # Use slow tasks so they're still RUNNING when the second thread queries.
    # On SQLite, writes serialize — Thread 2 starts after Thread 1 commits.
    # If tasks complete before Thread 2 queries, there's nothing to double-claim.
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

    # Each task should be recovered exactly once — no double execution.
    # Requires a database with row-level locking (PostgreSQL, MySQL).
    # On SQLite, skip_locked is ignored, so this assertion will fail.
    assert total_recovered == 5, (
        f"Expected 5 (no double-execution), "
        f"got {recovered1} + {recovered2} = {total_recovered}"
    )

    # Wait for tasks to finish (may have been submitted twice on SQLite)
    for db_result in orphans:
        _wait_for_db_result(db_result)


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
