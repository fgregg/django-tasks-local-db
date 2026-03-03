import time
import uuid

import pytest
from django.tasks import TaskResultStatus, task_backends
from django.tasks.exceptions import TaskResultDoesNotExist

from django_tasks_local_db.models import DBTaskResult
from tests.tasks import add_numbers, failing_task


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
def test_recover_orphaned_tasks(backend):
    """Create orphaned DB rows and verify recovery picks them up."""
    db_result = DBTaskResult.objects.create(
        args_kwargs={"args": [7, 8], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
    )

    recovered = backend.recover_tasks()
    assert recovered == 1

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        db_result.refresh_from_db()
        if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            break
        time.sleep(0.05)

    assert db_result.status == TaskResultStatus.SUCCESSFUL
    assert db_result.return_value == 15


@pytest.mark.django_db(transaction=True)
def test_recover_running_tasks(backend):
    """Tasks left in RUNNING state should also be recovered."""
    from django.utils import timezone

    db_result = DBTaskResult.objects.create(
        args_kwargs={"args": [1, 2], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        status=TaskResultStatus.RUNNING,
        started_at=timezone.now(),
    )

    recovered = backend.recover_tasks()
    assert recovered == 1

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        db_result.refresh_from_db()
        if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            break
        time.sleep(0.05)

    assert db_result.status == TaskResultStatus.SUCCESSFUL
    assert db_result.return_value == 3


@pytest.mark.django_db(transaction=True)
def test_process_pool_rejects_unpickleable_args(settings):
    from asgiref.local import Local

    settings.TASKS = {
        "default": {
            "BACKEND": "django_tasks_local_db.ProcessPoolBackend",
            "OPTIONS": {"MAX_WORKERS": 1},
        }
    }
    task_backends._connections = Local()
    pp_backend = task_backends["default"]

    with pytest.raises(ValueError, match="pickleable"):
        add_numbers.enqueue(lambda: None, 1)

    pp_backend.close()
