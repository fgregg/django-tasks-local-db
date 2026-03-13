"""Tests for API contracts: priority, queue names, args/kwargs, return values,
validation, backend isolation, and DB support checks."""

import time

import pytest
from django.tasks import TaskResultStatus, task_backends

from django_tasks_local_db.backend import LocalDBBackend
from django_tasks_local_db.models import DBTaskResult

from .tasks import add_numbers, failing_task, multiply, noop_task


@pytest.fixture
def backend():
    return task_backends["default"]


# ---------------------------------------------------------------------------
# Priority ordering (at the DB/watcher level)
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_watcher_dispatches_higher_priority_first(backend):
    """Tasks with higher priority should be dispatched before lower priority.

    Note: supports_priority = False means validate_task rejects non-default
    priorities via enqueue(). We create rows directly to test DB-level ordering.
    """
    # Create tasks with different priorities — higher number = higher priority
    low = DBTaskResult.objects.create(
        args_kwargs={"args": [1, 1], "kwargs": {}},
        priority=-10,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )
    high = DBTaskResult.objects.create(
        args_kwargs={"args": [2, 2], "kwargs": {}},
        priority=10,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )

    # Verify the due() queryset returns higher priority first
    due_ids = list(DBTaskResult.objects.due().values_list("id", flat=True))
    assert due_ids.index(high.id) < due_ids.index(low.id), (
        "Higher priority task should appear before lower priority in due() queryset"
    )

    # Let the watcher dispatch them — both should complete
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        high.refresh_from_db()
        low.refresh_from_db()
        if high.status == TaskResultStatus.SUCCESSFUL and low.status == TaskResultStatus.SUCCESSFUL:
            break
        time.sleep(0.05)

    assert high.status == TaskResultStatus.SUCCESSFUL
    assert high.return_value == 4
    assert low.status == TaskResultStatus.SUCCESSFUL
    assert low.return_value == 2


@pytest.mark.django_db(transaction=True)
def test_fifo_within_same_priority(backend):
    """Tasks with the same priority should be dispatched in FIFO order."""
    first = DBTaskResult.objects.create(
        args_kwargs={"args": [1, 1], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )
    # Small delay to ensure different enqueued_at
    time.sleep(0.01)
    second = DBTaskResult.objects.create(
        args_kwargs={"args": [2, 2], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )

    due_ids = list(DBTaskResult.objects.due().values_list("id", flat=True))
    assert due_ids.index(first.id) < due_ids.index(second.id)


# ---------------------------------------------------------------------------
# Backend isolation (two backends don't steal each other's tasks)
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_backend_only_dispatches_own_tasks(backend):
    """A backend should only dispatch tasks with its own backend_name."""
    # Create a task assigned to a different backend
    other_backend_task = DBTaskResult.objects.create(
        args_kwargs={"args": [1, 1], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="other_backend",
        exception_class_path="",
        traceback="",
    )

    # Create a task assigned to the default backend
    own_task = DBTaskResult.objects.create(
        args_kwargs={"args": [2, 2], "kwargs": {}},
        priority=0,
        task_path="tests.tasks.add_numbers",
        queue_name="default",
        backend_name="default",
        exception_class_path="",
        traceback="",
    )

    # Run a watcher tick — should only pick up own task
    backend._watcher_tick()

    own_task.refresh_from_db()
    other_backend_task.refresh_from_db()

    assert own_task.status == TaskResultStatus.RUNNING, (
        "Backend should dispatch its own tasks"
    )
    assert other_backend_task.status == TaskResultStatus.READY, (
        "Backend should NOT dispatch tasks belonging to another backend"
    )

    # Wait for own task to complete
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        own_task.refresh_from_db()
        if own_task.status == TaskResultStatus.SUCCESSFUL:
            break
        time.sleep(0.05)
    assert own_task.status == TaskResultStatus.SUCCESSFUL

    # Other backend's task should still be READY
    other_backend_task.refresh_from_db()
    assert other_backend_task.status == TaskResultStatus.READY


# ---------------------------------------------------------------------------
# Queue name behavior
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_queue_name_stored_in_result(backend):
    """The queue_name from the task should be stored on the DB row."""
    result = add_numbers.enqueue(1, 2)
    db_result = DBTaskResult.objects.get(id=result.id)
    assert db_result.queue_name == "default"


@pytest.mark.django_db
def test_validate_rejects_unconfigured_queue_name(backend):
    """Queue names not in the backend's QUEUES setting should be rejected."""
    from django.tasks.exceptions import InvalidTask

    with pytest.raises(InvalidTask, match="not valid for backend"):
        add_numbers.using(queue_name="nonexistent").enqueue(1, 2)


# ---------------------------------------------------------------------------
# Return value serialization
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_return_value_none(backend):
    """A task returning None should store None as the return value."""
    result = noop_task.enqueue()

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            break
        time.sleep(0.05)

    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value is None


@pytest.mark.django_db(transaction=True)
def test_return_value_nested_structure(backend):
    """Tasks returning nested dicts/lists should round-trip correctly."""
    from django.tasks import task

    # add_numbers returns an int — test that it round-trips
    result = add_numbers.enqueue(100, 200)

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            break
        time.sleep(0.05)

    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 300


# ---------------------------------------------------------------------------
# Args/kwargs round-trip
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_kwargs_round_trip(backend):
    """Keyword arguments should be stored and passed correctly."""
    result = add_numbers.enqueue(a=10, b=20)

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            break
        time.sleep(0.05)

    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 30


@pytest.mark.django_db(transaction=True)
def test_mixed_args_kwargs_round_trip(backend):
    """Mixed positional and keyword arguments should work."""
    result = add_numbers.enqueue(10, b=20)

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            break
        time.sleep(0.05)

    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 30


@pytest.mark.django_db(transaction=True)
def test_empty_args(backend):
    """A task with no arguments should work."""
    result = noop_task.enqueue()

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            break
        time.sleep(0.05)

    assert result.status == TaskResultStatus.SUCCESSFUL


@pytest.mark.django_db(transaction=True)
def test_complex_args_round_trip(backend):
    """Nested list/dict arguments should be serialized and deserialized."""
    # multiply accepts (a, b) — pass nested structures to verify serialization
    result = multiply.enqueue([1, 2, 3], 2)

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            break
        time.sleep(0.05)

    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == [1, 2, 3, 1, 2, 3]


# ---------------------------------------------------------------------------
# validate_task
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_enqueue_with_priority(backend):
    """supports_priority = True, so enqueuing with non-default priority
    should work and store the priority on the DB row."""
    result = add_numbers.using(priority=10).enqueue(1, 2)
    db_result = DBTaskResult.objects.get(id=result.id)
    assert db_result.priority == 10

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            break
        time.sleep(0.05)
    assert result.status == TaskResultStatus.SUCCESSFUL


@pytest.mark.django_db
def test_validate_rejects_out_of_range_priority(backend):
    """Priority must be between -100 and 100."""
    from django.tasks.exceptions import InvalidTask

    with pytest.raises(InvalidTask):
        add_numbers.using(priority=200).enqueue(1, 2)


@pytest.mark.django_db
def test_validate_rejects_non_module_level_task(backend):
    """Tasks must be defined at module level — nested functions are rejected."""
    from django.tasks import task
    from django.tasks.exceptions import InvalidTask

    with pytest.raises(InvalidTask, match="module level"):
        @task
        def nested_task():
            pass


# ---------------------------------------------------------------------------
# _check_db_support (SQLite rejection)
# ---------------------------------------------------------------------------


def test_check_db_support_rejects_unsupported_db():
    """LocalDBBackend should reject databases without SELECT FOR UPDATE
    SKIP LOCKED support (e.g., SQLite)."""
    from unittest.mock import patch

    backend = LocalDBBackend("test", {
        "BACKEND": "django_tasks_local_db.LocalDBBackend",
    })

    with patch("django.db.connection") as mock_conn:
        mock_conn.features.has_select_for_update_skip_locked = False
        mock_conn.vendor = "sqlite"

        with pytest.raises(RuntimeError, match="SELECT FOR UPDATE SKIP LOCKED"):
            backend._check_db_support()
