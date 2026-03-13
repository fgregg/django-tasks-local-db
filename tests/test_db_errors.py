"""Tests for DB error handling in worker threads (issue #6)."""

import time
from unittest.mock import patch

import pytest
from django.tasks import TaskResultStatus, task_backends

from django_tasks_local_db.models import DBTaskResult

from .tasks import add_numbers


@pytest.fixture
def backend():
    return task_backends["default"]


def _wait_for_db_result(db_result, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        db_result.refresh_from_db()
        if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            return
        time.sleep(0.05)


@pytest.mark.django_db(transaction=True)
def test_on_complete_db_failure_reaches_terminal_state(backend):
    """If set_successful fails, the task should still reach a terminal state."""
    from django.db import OperationalError

    with patch.object(
        DBTaskResult, "set_successful", side_effect=OperationalError("DB gone")
    ):
        result = add_numbers.enqueue(3, 4)

        # Give it time to try and fail
        time.sleep(2)

        db_result = DBTaskResult.objects.get(id=result.id)
        assert db_result.status in (
            TaskResultStatus.SUCCESSFUL,
            TaskResultStatus.FAILED,
        ), f"Task stuck in {db_result.status} — should reach a terminal state"


@pytest.mark.django_db(transaction=True)
def test_set_failed_failure_reaches_terminal_state(backend):
    """If set_failed fails, the task should still reach a terminal state."""
    from django.db import OperationalError

    with patch.object(
        DBTaskResult, "set_failed", side_effect=OperationalError("DB gone")
    ):
        from .tasks import always_fails

        result = always_fails.enqueue()

        # Give it time
        time.sleep(2)

        db_result = DBTaskResult.objects.get(id=result.id)
        assert db_result.status in (
            TaskResultStatus.SUCCESSFUL,
            TaskResultStatus.FAILED,
        ), f"Task stuck in {db_result.status} — should reach a terminal state"


@pytest.mark.django_db(transaction=True)
def test_pool_survives_db_error_in_on_complete(backend):
    """After a DB error in _on_complete, the pool should still process new tasks."""
    from django.db import OperationalError

    # First, cause a DB error
    with patch.object(
        DBTaskResult, "set_successful", side_effect=OperationalError("DB gone")
    ):
        result_broken = add_numbers.enqueue(1, 2)
        time.sleep(2)

    # Now, without the patch, enqueue a new task
    result_ok = add_numbers.enqueue(10, 20)

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        result_ok.refresh()
        if result_ok.is_finished:
            break
        time.sleep(0.05)

    assert result_ok.status == TaskResultStatus.SUCCESSFUL
    assert result_ok.return_value == 30
