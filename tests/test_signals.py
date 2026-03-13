"""Tests for task lifecycle signals."""

import time

import pytest
from django.tasks import TaskResultStatus, task_backends
from django.tasks.signals import task_enqueued, task_finished, task_started

from django_tasks_local_db.backend import LocalDBBackend
from django_tasks_local_db.models import DBTaskResult

from .tasks import add_numbers, failing_task


@pytest.fixture
def backend():
    return task_backends["default"]


class SignalCapture:
    """Capture signal emissions for assertions."""

    def __init__(self):
        self.calls = []

    def handler(self, sender, **kwargs):
        self.calls.append({"sender": sender, **kwargs})


def _wait_for_result(result, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            return
        time.sleep(0.05)
    raise TimeoutError(f"Task {result.id} did not finish within {timeout}s")


@pytest.mark.django_db(transaction=True)
def test_task_enqueued_signal_fires(backend):
    """task_enqueued should fire when a task is enqueued."""
    capture = SignalCapture()
    task_enqueued.connect(capture.handler)
    try:
        result = add_numbers.enqueue(1, 2)

        assert len(capture.calls) == 1
        call = capture.calls[0]
        assert call["sender"] is LocalDBBackend
        assert call["task_result"].id == result.id
        assert call["task_result"].status == TaskResultStatus.READY
    finally:
        task_enqueued.disconnect(capture.handler)


@pytest.mark.django_db(transaction=True)
def test_task_started_signal_fires(backend):
    """task_started should fire when a task begins execution."""
    capture = SignalCapture()
    task_started.connect(capture.handler)
    try:
        result = add_numbers.enqueue(1, 2)
        _wait_for_result(result)

        assert len(capture.calls) == 1
        call = capture.calls[0]
        assert call["sender"] is LocalDBBackend
        assert call["task_result"].id == result.id
    finally:
        task_started.disconnect(capture.handler)


@pytest.mark.django_db(transaction=True)
def test_task_finished_signal_fires(backend):
    """task_finished should fire when a task completes."""
    capture = SignalCapture()
    task_finished.connect(capture.handler)
    try:
        result = add_numbers.enqueue(1, 2)
        _wait_for_result(result)
        # task_finished fires in _on_complete callback, give it a moment
        time.sleep(0.5)

        assert len(capture.calls) == 1
        call = capture.calls[0]
        assert call["sender"] is LocalDBBackend
        assert call["task_result"].id == result.id
        assert call["task_result"].status == TaskResultStatus.SUCCESSFUL
    finally:
        task_finished.disconnect(capture.handler)


@pytest.mark.django_db(transaction=True)
def test_task_finished_signal_fires_on_failure(backend):
    """task_finished should fire even when a task fails."""
    capture = SignalCapture()
    task_finished.connect(capture.handler)
    try:
        result = failing_task.enqueue()
        _wait_for_result(result)
        time.sleep(0.5)

        assert len(capture.calls) == 1
        call = capture.calls[0]
        assert call["task_result"].status == TaskResultStatus.FAILED
    finally:
        task_finished.disconnect(capture.handler)


@pytest.mark.django_db(transaction=True)
def test_all_three_signals_fire_in_order(backend):
    """enqueued, started, finished should all fire for a single task."""
    order = []

    def on_enqueued(sender, **kwargs):
        order.append("enqueued")

    def on_started(sender, **kwargs):
        order.append("started")

    def on_finished(sender, **kwargs):
        order.append("finished")

    task_enqueued.connect(on_enqueued)
    task_started.connect(on_started)
    task_finished.connect(on_finished)
    try:
        result = add_numbers.enqueue(1, 2)
        _wait_for_result(result)
        time.sleep(0.5)

        assert order == ["enqueued", "started", "finished"]
    finally:
        task_enqueued.disconnect(on_enqueued)
        task_started.disconnect(on_started)
        task_finished.disconnect(on_finished)
