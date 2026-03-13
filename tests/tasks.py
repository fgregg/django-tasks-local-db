"""Test task functions used by test_backend.py."""

from django.tasks import task
from django.tasks.base import TaskContext


@task
def add_numbers(a, b):
    return a + b


@task
def failing_task():
    raise ValueError("intentional failure")


@task
def slow_task(seconds=0.1):
    import time

    time.sleep(seconds)
    return "done"


@task(takes_context=True)
def get_task_id(context: TaskContext) -> str:
    return context.task_result.id


@task
def always_fails():
    raise RuntimeError("always fails")


@task
def multiply(a, b):
    return a * b


# Thread-safe counter for detecting double execution
import threading

_call_counter_lock = threading.Lock()
_call_counts: dict[str, int] = {}


@task
def counting_task(key, sleep_seconds=1):
    """Sleeps then increments a counter. Used to detect double execution."""
    import time

    time.sleep(sleep_seconds)
    with _call_counter_lock:
        _call_counts[key] = _call_counts.get(key, 0) + 1
    return _call_counts[key]


def get_call_count(key):
    with _call_counter_lock:
        return _call_counts.get(key, 0)


def reset_call_counts():
    with _call_counter_lock:
        _call_counts.clear()


@task
def noop_task():
    """Minimal task for benchmarking overhead."""
    pass


@task
def cpu_task(n=1000):
    """CPU-bound task for stress testing."""
    total = 0
    for i in range(n):
        total += i * i
    return total
