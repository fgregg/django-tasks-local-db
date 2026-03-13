"""Tests for shutdown behavior (issue #5)."""

import os
import signal
import subprocess
import sys
import textwrap
import time

import pytest
from django.tasks import TaskResultStatus, task_backends

from django_tasks_local_db.models import DBTaskResult

from .tasks import slow_task


@pytest.fixture
def backend():
    return task_backends["default"]


@pytest.mark.django_db(transaction=True)
def test_close_waits_for_inflight_tasks(backend):
    """close() should wait for in-flight tasks to finish."""
    result = slow_task.enqueue(0.5)

    # Give the task a moment to start
    time.sleep(0.1)

    # close() calls shutdown(wait=True) — should block until task finishes
    backend.close()
    # Reset so subsequent tests get a fresh executor
    backend._state = None

    db_result = DBTaskResult.objects.get(id=result.id)
    assert db_result.status == TaskResultStatus.SUCCESSFUL
    assert db_result.return_value == "done"


@pytest.mark.django_db(transaction=True)
def test_enqueue_after_close_does_not_orphan(backend):
    """After close(), enqueue should either raise or still work — not orphan a row."""
    # Ensure we have an active executor first
    backend._get_state()
    backend.close()

    try:
        result = slow_task.enqueue(0.1)
        enqueue_raised = False
        result_id = result.id
    except RuntimeError:
        enqueue_raised = True
        result_id = None
    finally:
        # Reset so subsequent tests get a fresh executor
        backend._state = None

    if enqueue_raised:
        # If enqueue raised, there should be no orphaned READY rows
        orphans = DBTaskResult.objects.filter(status=TaskResultStatus.READY)
        assert not orphans.exists(), (
            f"enqueue raised RuntimeError but left orphaned DB row: {orphans.first().id}"
        )
    else:
        # If enqueue didn't raise, the task should actually complete
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            result.refresh()
            if result.is_finished:
                break
            time.sleep(0.05)
        assert result.is_finished, (
            f"enqueue succeeded but task {result_id} never finished"
        )


# --- Subprocess-based SIGTERM test ---


def _make_worker_script():
    """Generate a self-contained subprocess script that uses PostgreSQL."""
    return textwrap.dedent("""\
        import os, sys, time, django
        from django.conf import settings

        settings.configure(
            SECRET_KEY="test-secret-key",
            DATABASES={
                "default": {
                    "ENGINE": "django.db.backends.postgresql",
                    "NAME": os.environ.get("PGDATABASE", "django_tasks_test"),
                    "USER": os.environ.get("PGUSER", "django_tasks"),
                    "PASSWORD": os.environ.get("PGPASSWORD", "django_tasks"),
                    "HOST": os.environ.get("PGHOST", "localhost"),
                    "PORT": os.environ.get("PGPORT", "5433"),
                }
            },
            INSTALLED_APPS=[
                "django.contrib.contenttypes",
                "django_tasks_local_db",
            ],
            TASKS={
                "default": {
                    "BACKEND": "django_tasks_local_db.LocalDBBackend",
                    "OPTIONS": {"MAX_WORKERS": 2},
                }
            },
            USE_TZ=True,
            DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
        )
        django.setup()

        # Run migrations
        from django.core.management import call_command
        call_command("migrate", "--run-syncdb", verbosity=0)

        from tests.tasks import slow_task
        result = slow_task.enqueue(2.0)
        print(f"TASK_ID={result.id}", flush=True)

        # Wait for the task to start running
        time.sleep(0.5)
        print("READY", flush=True)

        # Wait to be killed
        time.sleep(30)
    """)


def test_sigterm_kills_inflight_task():
    """SIGTERM: does an in-flight task complete or get orphaned?

    Starts a subprocess that enqueues a slow task, sends SIGTERM,
    then checks the DB state via psycopg.
    """
    import psycopg

    script = _make_worker_script()

    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    task_id = None
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        line = proc.stdout.readline().strip()
        if line.startswith("TASK_ID="):
            task_id = line.split("=", 1)[1]
        elif line == "READY":
            break

    assert task_id is not None, (
        f"Never got TASK_ID from subprocess.\nstderr: {proc.stderr.read()}"
    )

    # Send SIGTERM
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=10)

    # Check DB state via psycopg
    conn = psycopg.connect(
        dbname=os.environ.get("PGDATABASE", "django_tasks_test"),
        user=os.environ.get("PGUSER", "django_tasks"),
        password=os.environ.get("PGPASSWORD", "django_tasks"),
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5433"),
    )
    row = conn.execute(
        "SELECT status FROM django_tasks_local_db_dbtaskresult WHERE id = %s",
        (task_id,),
    ).fetchone()
    conn.close()

    assert row is not None, f"Task {task_id} not found in DB"
    status = row[0]

    # After SIGTERM, the task should reach a terminal state
    # (either finished successfully or marked as failed)
    assert status in ("SUCCESSFUL", "FAILED"), (
        f"Task stuck in {status} after SIGTERM — should reach a terminal state"
    )
