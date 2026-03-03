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
