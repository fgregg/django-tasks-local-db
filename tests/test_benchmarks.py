"""Benchmark/stress tests for the LocalDBBackend.

These are excluded from the default test run. Run with:
    pytest -m benchmark --benchmark-enable
"""

import time

import pytest
from django.tasks import TaskResultStatus, task_backends

from django_tasks_local_db.models import DBTaskResult

from .tasks import add_numbers, cpu_task, noop_task, slow_task

pytestmark = pytest.mark.benchmark


@pytest.fixture
def backend():
    return task_backends["default"]


def _wait_for_db_result(db_result, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        db_result.refresh_from_db()
        if db_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            return
        time.sleep(0.05)
    raise TimeoutError(f"DB task {db_result.id} did not finish within {timeout}s")


def _wait_all_finished(result_ids, timeout=60):
    """Wait until all tasks reach a terminal state."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pending = DBTaskResult.objects.filter(
            id__in=result_ids,
        ).exclude(
            status__in=[TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED],
        ).count()
        if pending == 0:
            return
        time.sleep(0.1)
    raise TimeoutError(
        f"{pending} tasks still pending after {timeout}s"
    )


# ---------------------------------------------------------------------------
# Throughput: how many tasks/sec can we enqueue + complete?
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_throughput_noop(benchmark, backend):
    """Measure end-to-end throughput with minimal (noop) tasks."""
    n = 50

    def enqueue_and_drain():
        ids = []
        for _ in range(n):
            result = noop_task.enqueue()
            ids.append(result.id)
        _wait_all_finished(ids, timeout=30)
        return ids

    benchmark.pedantic(enqueue_and_drain, rounds=3, warmup_rounds=1)


@pytest.mark.django_db(transaction=True)
def test_throughput_cpu_bound(benchmark, backend):
    """Measure throughput with CPU-bound tasks."""
    n = 20

    def enqueue_and_drain():
        ids = []
        for _ in range(n):
            result = cpu_task.enqueue(10000)
            ids.append(result.id)
        _wait_all_finished(ids, timeout=30)

    benchmark.pedantic(enqueue_and_drain, rounds=3, warmup_rounds=1)


# ---------------------------------------------------------------------------
# Latency: time from enqueue to task completion
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_enqueue_to_completion_latency(benchmark, backend):
    """Measure latency from enqueue() to task reaching SUCCESSFUL."""

    def enqueue_and_wait():
        result = noop_task.enqueue()
        db_result = DBTaskResult.objects.get(id=result.id)
        _wait_for_db_result(db_result, timeout=10)
        return db_result

    db_result = benchmark.pedantic(enqueue_and_wait, rounds=10, warmup_rounds=2)
    assert db_result.status == TaskResultStatus.SUCCESSFUL


# ---------------------------------------------------------------------------
# Burst: enqueue many tasks at once, measure time to drain
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_burst_enqueue(benchmark, backend):
    """Enqueue a burst of tasks and measure total drain time."""
    n = 100

    def burst():
        ids = []
        for _ in range(n):
            result = add_numbers.enqueue(1, 1)
            ids.append(result.id)
        _wait_all_finished(ids, timeout=60)

    benchmark.pedantic(burst, rounds=3, warmup_rounds=1)


# ---------------------------------------------------------------------------
# Saturation: fill the pool and measure queuing behavior
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_pool_saturation(benchmark, backend):
    """With MAX_WORKERS=2, enqueue tasks that exceed pool capacity.

    Measures how well the system handles backpressure when the pool
    is saturated with slow tasks.
    """
    n_slow = backend._max_workers + 5
    n_fast = 20

    def saturate_and_drain():
        ids = []
        # Fill pool with slow tasks
        for _ in range(n_slow):
            result = slow_task.enqueue(0.5)
            ids.append(result.id)
        # Add fast tasks that must wait
        for _ in range(n_fast):
            result = add_numbers.enqueue(1, 1)
            ids.append(result.id)
        _wait_all_finished(ids, timeout=30)

    benchmark.pedantic(saturate_and_drain, rounds=3, warmup_rounds=1)


# ---------------------------------------------------------------------------
# Watcher tick: measure cost of one watcher cycle
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_watcher_tick_empty(benchmark, backend):
    """Measure the cost of a watcher tick with no tasks to dispatch."""
    # Ensure the backend state is initialized
    backend._get_state()

    benchmark(backend._watcher_tick)


@pytest.mark.django_db(transaction=True)
def test_watcher_tick_with_due_tasks(benchmark, backend):
    """Measure watcher tick cost when there are tasks to dispatch."""

    def setup():
        # Create 10 READY tasks for the watcher to find
        for i in range(10):
            DBTaskResult.objects.create(
                args_kwargs={"args": [i, i], "kwargs": {}},
                priority=0,
                task_path="tests.tasks.noop_task",
                queue_name="default",
                backend_name="default",
                exception_class_path="",
                traceback="",
            )

    setup()
    # Ensure backend state is initialized
    backend._get_state()

    benchmark(backend._watcher_tick)

    # Clean up: wait for dispatched tasks
    _wait_all_finished(
        list(DBTaskResult.objects.filter(
            status__in=[TaskResultStatus.READY, TaskResultStatus.RUNNING]
        ).values_list("id", flat=True)),
        timeout=10,
    )


# ---------------------------------------------------------------------------
# Enqueue-only: measure the cost of writing to DB (no execution)
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_enqueue_write_cost(benchmark, backend):
    """Measure the cost of enqueue() alone (DB write + signal)."""

    def enqueue_one():
        return noop_task.enqueue()

    benchmark.pedantic(enqueue_one, rounds=50, warmup_rounds=5)
