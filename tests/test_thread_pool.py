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

    Enqueue more slow tasks than MAX_WORKERS to fill the pool, then
    enqueue a fast task. All should eventually complete.
    """
    n_slow = backend._max_workers + 2
    slow_results = [slow_task.enqueue(1) for _ in range(n_slow)]

    # Give the slow tasks a moment to claim threads
    time.sleep(0.1)

    # This should queue until a worker frees up
    fast = add_numbers.enqueue(10, 20)

    for r in slow_results:
        _wait_for_result(r, timeout=30)
    _wait_for_result(fast, timeout=30)

    for r in slow_results:
        assert r.status == TaskResultStatus.SUCCESSFUL
    assert fast.status == TaskResultStatus.SUCCESSFUL
    assert fast.return_value == 30


@pytest.mark.django_db(transaction=True)
def test_poison_pill_does_not_block_other_tasks(backend):
    """A task that always fails should not block other tasks from completing.

    Enqueue several always-failing tasks alongside a good task.
    The good task should still complete successfully.
    """
    poison_results = [always_fails.enqueue() for _ in range(5)]
    good_result = add_numbers.enqueue(42, 58)

    _wait_for_result(good_result, timeout=10)
    assert good_result.status == TaskResultStatus.SUCCESSFUL
    assert good_result.return_value == 100

    # All poison pills should have failed, not hung
    for r in poison_results:
        _wait_for_result(r, timeout=10)
        assert r.status == TaskResultStatus.FAILED
