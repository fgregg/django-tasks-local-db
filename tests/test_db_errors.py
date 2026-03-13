"""Tests for DB error handling in worker threads (issue #6)."""

import logging
import time
from threading import Lock
from unittest.mock import patch

import pytest
from django.tasks import TaskResultStatus, task_backends

from django_tasks_local_db.models import DBTaskResult

from .tasks import add_numbers, always_fails


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


class ThreadSafeLogCapture(logging.Handler):
    """Log handler that captures records from any thread."""

    def __init__(self):
        super().__init__()
        self.records = []
        self._lock = Lock()

    def emit(self, record):
        with self._lock:
            self.records.append(record)


@pytest.mark.django_db(transaction=True)
def test_on_complete_db_failure_emits_warning(backend):
    """If set_successful fails, a warning should be logged."""
    from django.db import OperationalError

    handler = ThreadSafeLogCapture()
    logger = logging.getLogger("django_tasks_local_db")
    logger.addHandler(handler)
    try:
        with patch.object(
            DBTaskResult, "set_successful", side_effect=OperationalError("DB gone")
        ):
            add_numbers.enqueue(3, 4)
            time.sleep(2)
    finally:
        logger.removeHandler(handler)

    warnings = [r.getMessage().lower() for r in handler.records if r.levelno >= logging.WARNING]
    assert any(
        "failed to write result to db" in msg
        for msg in warnings
    ), f"Expected warning about DB write failure, got: {warnings}"


@pytest.mark.django_db(transaction=True)
def test_set_failed_failure_emits_warning(backend):
    """If set_failed fails, a warning should be logged."""
    from django.db import OperationalError

    handler = ThreadSafeLogCapture()
    logger = logging.getLogger("django_tasks_local_db")
    logger.addHandler(handler)
    try:
        with patch.object(
            DBTaskResult, "set_failed", side_effect=OperationalError("DB gone")
        ):
            always_fails.enqueue()
            time.sleep(2)
    finally:
        logger.removeHandler(handler)

    warnings = [r.getMessage().lower() for r in handler.records if r.levelno >= logging.WARNING]
    assert any(
        "failed to write result to db" in msg
        for msg in warnings
    ), f"Expected warning about DB write failure, got: {warnings}"


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
    _wait_for_result(result_ok)

    assert result_ok.status == TaskResultStatus.SUCCESSFUL
    assert result_ok.return_value == 30


@pytest.mark.django_db(transaction=True)
def test_stuck_task_recovered_on_restart(backend):
    """A task stuck in RUNNING after DB failure should be recoverable."""
    from django.db import OperationalError

    with patch.object(
        DBTaskResult, "set_successful", side_effect=OperationalError("DB gone")
    ):
        result = add_numbers.enqueue(3, 4)
        time.sleep(2)

    # Task should be stuck in RUNNING
    db_result = DBTaskResult.objects.get(id=result.id)
    assert db_result.status == TaskResultStatus.RUNNING

    # Simulate restart: recover_tasks finds it and re-executes
    recovered = backend.recover_tasks()
    assert recovered == 1

    # Wait for recovery to complete
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        db_result.refresh_from_db()
        if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            break
        time.sleep(0.05)

    assert db_result.status == TaskResultStatus.SUCCESSFUL
    assert db_result.return_value == 7
