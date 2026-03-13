"""Tests for deferred task execution (run_after / issue #2)."""

import time

import pytest
from django.tasks import TaskResultStatus, task_backends
from django.utils import timezone

from django_tasks_local_db.models import DBTaskResult

from .tasks import add_numbers


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
def test_deferred_task_not_dispatched_before_run_after(backend):
    """A task with run_after in the future should not be dispatched early."""
    future_time = timezone.now() + timezone.timedelta(seconds=30)
    deferred = add_numbers.using(run_after=future_time)
    result = deferred.enqueue(1, 2)

    # Wait a couple of watcher cycles
    time.sleep(3)

    db_result = DBTaskResult.objects.get(id=result.id)
    assert db_result.status == TaskResultStatus.READY, (
        f"Task should still be READY (run_after is 30s away), but is {db_result.status}"
    )
    assert db_result.run_after == future_time


@pytest.mark.django_db(transaction=True)
def test_deferred_task_dispatched_after_run_after(backend):
    """A task with run_after in the near past should be dispatched."""
    past_time = timezone.now() - timezone.timedelta(seconds=1)
    deferred = add_numbers.using(run_after=past_time)
    result = deferred.enqueue(3, 4)

    db_result = DBTaskResult.objects.get(id=result.id)
    _wait_for_db_result(db_result)

    assert db_result.status == TaskResultStatus.SUCCESSFUL
    assert db_result.return_value == 7


@pytest.mark.django_db(transaction=True)
def test_deferred_task_becomes_due(backend):
    """A task with a short run_after delay should execute after the delay."""
    run_at = timezone.now() + timezone.timedelta(seconds=2)
    deferred = add_numbers.using(run_after=run_at)
    result = deferred.enqueue(5, 6)

    # Should still be READY immediately
    db_result = DBTaskResult.objects.get(id=result.id)
    assert db_result.status == TaskResultStatus.READY

    # Wait for the delay plus watcher cycles
    _wait_for_db_result(db_result, timeout=10)

    assert db_result.status == TaskResultStatus.SUCCESSFUL
    assert db_result.return_value == 11


@pytest.mark.django_db(transaction=True)
def test_due_queryset_excludes_future_tasks(backend):
    """The due() queryset should not include tasks with future run_after."""
    future_time = timezone.now() + timezone.timedelta(seconds=60)
    past_time = timezone.now() - timezone.timedelta(seconds=1)

    future_task = DBTaskResult.objects.create(
        args_kwargs={"args": [1, 2], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
        run_after=future_time,
    )
    past_task = DBTaskResult.objects.create(
        args_kwargs={"args": [3, 4], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
        run_after=past_time,
    )
    no_defer_task = DBTaskResult.objects.create(
        args_kwargs={"args": [5, 6], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )

    due_ids = set(str(r.id) for r in DBTaskResult.objects.due())
    assert str(future_task.id) not in due_ids
    assert str(past_task.id) in due_ids
    assert str(no_defer_task.id) in due_ids


@pytest.mark.django_db(transaction=True)
def test_run_after_stored_in_task_result(backend):
    """run_after should be preserved in the TaskResult returned by enqueue."""
    run_at = timezone.now() + timezone.timedelta(seconds=60)
    deferred = add_numbers.using(run_after=run_at)
    result = deferred.enqueue(1, 2)

    db_result = DBTaskResult.objects.get(id=result.id)
    assert db_result.run_after == run_at

    # The task_result property should also carry run_after
    task_result = db_result.task_result
    assert task_result.task.run_after == run_at
