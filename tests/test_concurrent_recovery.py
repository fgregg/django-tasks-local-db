"""Tests for watcher-based task recovery (issues #1, #7)."""

import time

import pytest
from django.tasks import TaskResultStatus, task_backends
from django.utils import timezone

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
def test_watcher_recovers_orphaned_ready_tasks(backend):
    """The watcher should pick up and execute all orphaned READY tasks."""
    orphans = _create_orphans(5)

    backend._ensure_watcher()

    for db_result in orphans:
        _wait_for_db_result(db_result)
        db_result.refresh_from_db()
        assert db_result.status == TaskResultStatus.SUCCESSFUL


@pytest.mark.django_db(transaction=True)
def test_watcher_recovers_stale_running_and_ready(backend):
    """The watcher should recover both stale RUNNING tasks and READY tasks."""
    stale_time = timezone.now() - timezone.timedelta(seconds=60)

    # Stale RUNNING task (simulates dead worker)
    task_a = DBTaskResult.objects.create(
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
    # Orphaned READY task
    task_b = DBTaskResult.objects.create(
        args_kwargs={"args": [3, 4], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )

    backend._ensure_watcher()

    _wait_for_db_result(task_a)
    _wait_for_db_result(task_b)

    task_a.refresh_from_db()
    task_b.refresh_from_db()
    assert task_a.status == TaskResultStatus.SUCCESSFUL
    assert task_a.return_value == 3
    assert task_b.status == TaskResultStatus.SUCCESSFUL
    assert task_b.return_value == 7


@pytest.mark.django_db(transaction=True)
def test_watcher_records_worker_id(backend):
    """Watcher-dispatched task should record the executing worker's ID."""
    db_result = DBTaskResult.objects.create(
        args_kwargs={"args": [1, 1], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )

    backend._ensure_watcher()
    _wait_for_db_result(db_result)

    db_result.refresh_from_db()
    assert len(db_result.worker_ids) == 1, (
        f"Expected 1 worker_id, got {len(db_result.worker_ids)}: {db_result.worker_ids}"
    )
