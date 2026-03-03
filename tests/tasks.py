"""Test task functions used by test_backend.py."""

from django.tasks import task


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
