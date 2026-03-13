"""Tests for thread pool resilience under failure (issue #4)."""

import time

import pytest
from django.tasks import TaskResultStatus, task_backends

from django_tasks_local_db.models import DBTaskResult

from .tasks import add_numbers, always_fails, multiply, slow_task


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


def _wait_for_db_result(db_result, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        db_result.refresh_from_db()
        if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            return
        time.sleep(0.05)
    raise TimeoutError(f"DB task {db_result.id} did not finish within {timeout}s")


@pytest.mark.django_db(transaction=True)
def test_pool_healthy_after_many_failures(backend):
    """After many consecutive failures, the pool should still process tasks."""
    fail_results = [always_fails.enqueue() for _ in range(10)]
    for r in fail_results:
        _wait_for_result(r)

    for r in fail_results:
        assert r.status == TaskResultStatus.FAILED

    # Pool should still work
    result = add_numbers.enqueue(1, 2)
    _wait_for_result(result)
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 3


@pytest.mark.django_db(transaction=True)
def test_mixed_failing_and_succeeding_concurrent(backend):
    """Failures in some tasks shouldn't affect others running concurrently."""
    results = []
    for i in range(5):
        results.append(("fail", always_fails.enqueue()))
        results.append(("succeed", add_numbers.enqueue(i, i)))

    for expected, r in results:
        _wait_for_result(r)
        if expected == "fail":
            assert r.status == TaskResultStatus.FAILED
        else:
            assert r.status == TaskResultStatus.SUCCESSFUL


@pytest.mark.django_db(transaction=True)
def test_backpressure_when_pool_saturated(backend):
    """Tasks enqueued when all threads are busy should queue and complete.

    Test settings use MAX_WORKERS=2. We fill both threads with slow tasks,
    then enqueue a fast task. All three should eventually complete.
    """
    slow1 = slow_task.enqueue(0.5)
    slow2 = slow_task.enqueue(0.5)

    # Give the slow tasks a moment to claim threads
    time.sleep(0.1)

    # This should queue internally in the ThreadPoolExecutor
    fast = add_numbers.enqueue(10, 20)

    _wait_for_result(slow1, timeout=10)
    _wait_for_result(slow2, timeout=10)
    _wait_for_result(fast, timeout=10)

    assert slow1.status == TaskResultStatus.SUCCESSFUL
    assert slow2.status == TaskResultStatus.SUCCESSFUL
    assert fast.status == TaskResultStatus.SUCCESSFUL
    assert fast.return_value == 30
