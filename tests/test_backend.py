import time
import uuid

import pytest
from django.tasks import TaskResultStatus, task_backends
from django.tasks.exceptions import TaskResultDoesNotExist

from django_tasks_local_db.models import DBTaskResult
from tests.tasks import add_numbers, failing_task, get_task_id


@pytest.fixture
def backend():
    return task_backends["default"]


def _wait_for_result(result, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            return
        time.sleep(0.05)
    raise TimeoutError(f"Task {result.id} did not finish within {timeout}s")


def _wait_for_db_result(db_result, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        db_result.refresh_from_db()
        if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            return
        time.sleep(0.05)
    raise TimeoutError(f"DB task {db_result.id} did not finish within {timeout}s")


@pytest.mark.django_db(transaction=True)
def test_enqueue_creates_db_row(backend):
    result = add_numbers.enqueue(2, 3)

    assert result.id is not None
    db_row = DBTaskResult.objects.get(id=result.id)
    assert db_row.task_path == "tests.tasks.add_numbers"
    assert db_row.args_kwargs == {"args": [2, 3], "kwargs": {}}


@pytest.mark.django_db(transaction=True)
def test_task_completes_successfully(backend):
    result = add_numbers.enqueue(2, 3)
    _wait_for_result(result)

    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 5

    db_row = DBTaskResult.objects.get(id=result.id)
    assert db_row.status == TaskResultStatus.SUCCESSFUL
    assert db_row.return_value == 5
    assert db_row.finished_at is not None


@pytest.mark.django_db(transaction=True)
def test_task_failure_recorded(backend):
    result = failing_task.enqueue()
    _wait_for_result(result)

    assert result.status == TaskResultStatus.FAILED

    db_row = DBTaskResult.objects.get(id=result.id)
    assert db_row.status == TaskResultStatus.FAILED
    assert db_row.exception_class_path == "builtins.ValueError"
    assert "intentional failure" in db_row.traceback
    assert db_row.finished_at is not None


@pytest.mark.django_db(transaction=True)
def test_get_result(backend):
    result = add_numbers.enqueue(10, 20)
    _wait_for_result(result)

    fetched = add_numbers.get_result(result.id)
    assert fetched.status == TaskResultStatus.SUCCESSFUL
    assert fetched.return_value == 30


@pytest.mark.django_db(transaction=True)
def test_get_result_not_found(backend):
    with pytest.raises(TaskResultDoesNotExist):
        add_numbers.get_result(str(uuid.uuid4()))


@pytest.mark.django_db(transaction=True)
def test_watcher_picks_up_orphaned_ready_tasks(backend):
    """Orphaned READY rows in DB should be picked up by the watcher."""
    db_result = DBTaskResult.objects.create(
        args_kwargs={"args": [7, 8], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )

    # Start the watcher — it will find and dispatch the orphaned task
    backend._ensure_watcher()
    _wait_for_db_result(db_result)

    assert db_result.status == TaskResultStatus.SUCCESSFUL
    assert db_result.return_value == 15


@pytest.mark.django_db(transaction=True)
def test_get_result_after_watcher_recovery(backend):
    """Regression test for #1: get_result must work on watcher-recovered tasks."""
    db_result = DBTaskResult.objects.create(
        args_kwargs={"args": [50, 50], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )

    backend._ensure_watcher()
    _wait_for_db_result(db_result)

    result = add_numbers.get_result(str(db_result.id))
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 100


@pytest.mark.django_db(transaction=True)
def test_watcher_recovers_stale_running_tasks(backend):
    """Tasks stuck in RUNNING with stale heartbeats should be recovered."""
    from django.utils import timezone

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

    backend._ensure_watcher()
    _wait_for_db_result(db_result)

    assert db_result.status == TaskResultStatus.SUCCESSFUL
    assert db_result.return_value == 3


@pytest.mark.django_db(transaction=True)
def test_takes_context(backend):
    result = get_task_id.enqueue()
    _wait_for_result(result)

    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == result.id
