"""Tests for model properties, QuerySet methods, and signal handlers."""

import time

import pytest
from django.core.exceptions import SuspiciousOperation
from django.tasks import TaskResultStatus, task_backends

from django_tasks_local_db.models import DBTaskResult, _get_date_max

from .tasks import add_numbers, failing_task


@pytest.fixture
def backend():
    return task_backends["default"]


def _wait_for_result(result, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            return
        time.sleep(0.05)
    raise TimeoutError(f"Task {result.id} did not finish within {timeout}s")


# --- task_name property ---


@pytest.mark.django_db(transaction=True)
def test_task_name_returns_task_name(backend):
    result = add_numbers.enqueue(1, 2)
    _wait_for_result(result)

    db_row = DBTaskResult.objects.get(id=result.id)
    assert db_row.task_name == "add_numbers"


@pytest.mark.django_db(transaction=True)
def test_task_name_falls_back_to_task_path_on_import_error(backend):
    db_row = DBTaskResult.objects.create(
        args_kwargs={"args": [], "kwargs": {}},
        task_path="nonexistent.module.task_func",
        queue_name="default",
        backend_name="default",
        run_after=_get_date_max(),
    )
    assert db_row.task_name == "nonexistent.module.task_func"


# --- task property raises SuspiciousOperation ---


@pytest.mark.django_db(transaction=True)
def test_task_property_raises_suspicious_operation_for_non_task(backend):
    """task property should raise SuspiciousOperation when path points to a non-Task."""
    db_row = DBTaskResult.objects.create(
        args_kwargs={"args": [], "kwargs": {}},
        task_path="os.path.join",  # valid import, but not a Task
        queue_name="default",
        backend_name="default",
        run_after=_get_date_max(),
    )
    with pytest.raises(SuspiciousOperation, match="does not point to a Task"):
        db_row.task


# --- QuerySet methods ---


@pytest.mark.django_db(transaction=True)
def test_queryset_successful(backend):
    result = add_numbers.enqueue(1, 2)
    _wait_for_result(result)

    assert DBTaskResult.objects.successful().filter(id=result.id).exists()
    assert not DBTaskResult.objects.failed().filter(id=result.id).exists()
    assert DBTaskResult.objects.finished().filter(id=result.id).exists()


@pytest.mark.django_db(transaction=True)
def test_queryset_failed(backend):
    result = failing_task.enqueue()
    _wait_for_result(result)

    assert DBTaskResult.objects.failed().filter(id=result.id).exists()
    assert not DBTaskResult.objects.successful().filter(id=result.id).exists()
    assert DBTaskResult.objects.finished().filter(id=result.id).exists()


@pytest.mark.django_db(transaction=True)
def test_queryset_finished_excludes_ready():
    db_row = DBTaskResult.objects.create(
        args_kwargs={"args": [], "kwargs": {}},
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        run_after=_get_date_max(),
        status=TaskResultStatus.READY,
    )
    assert not DBTaskResult.objects.finished().filter(id=db_row.id).exists()
    assert not DBTaskResult.objects.successful().filter(id=db_row.id).exists()
    assert not DBTaskResult.objects.failed().filter(id=db_row.id).exists()


# --- pre_save signal handler ---


@pytest.mark.django_db(transaction=True)
def test_pre_save_sets_run_after_when_none():
    """Signal handler should set run_after to date_max when None."""
    db_row = DBTaskResult.objects.create(
        args_kwargs={"args": [], "kwargs": {}},
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        run_after=None,
    )
    db_row.refresh_from_db()
    assert db_row.run_after == _get_date_max()


@pytest.mark.django_db(transaction=True)
def test_pre_save_preserves_explicit_run_after():
    """Signal handler should not overwrite an explicit run_after value."""
    from django.utils import timezone

    explicit_time = timezone.now()
    db_row = DBTaskResult.objects.create(
        args_kwargs={"args": [], "kwargs": {}},
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        run_after=explicit_time,
    )
    db_row.refresh_from_db()
    # Compare to the second (DateTimeField may truncate microseconds on some DBs)
    assert abs((db_row.run_after - explicit_time).total_seconds()) < 1


# --- Meta verbose names ---


def test_verbose_name():
    assert DBTaskResult._meta.verbose_name == "task result"
    assert DBTaskResult._meta.verbose_name_plural == "task results"


# --- queue_name index exists ---


def test_queue_name_index():
    index_names = [idx.name for idx in DBTaskResult._meta.indexes]
    queue_index = [
        idx
        for idx in DBTaskResult._meta.indexes
        if hasattr(idx, "fields") and "queue_name" in idx.fields
    ]
    assert len(queue_index) == 1, f"Expected queue_name index, found indexes: {index_names}"
