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
    result = slow_task.enqueue(2)

    # Wait for the watcher to dispatch the task and it to start running
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        db_result = DBTaskResult.objects.get(id=result.id)
        if db_result.status == TaskResultStatus.RUNNING:
            break
        time.sleep(0.05)
    assert db_result.status == TaskResultStatus.RUNNING

    # close() calls shutdown(wait=True) — should block until task finishes
    backend.close()

    db_result.refresh_from_db()
    assert db_result.status == TaskResultStatus.SUCCESSFUL
    assert db_result.return_value == "done"


@pytest.mark.django_db(transaction=True)
def test_enqueue_after_close_still_works(backend):
    """After close(), enqueue should still work — it creates a fresh executor."""
    # Ensure we have an active executor first, then shut it down
    backend._get_state()
    backend.close()

    # enqueue() should succeed — it writes to DB and starts a new watcher
    result = slow_task.enqueue(0.1)

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            break
        time.sleep(0.05)
    assert result.is_finished, (
        f"enqueue succeeded but task {result.id} never finished"
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
        from django_tasks_local_db.models import DBTaskResult
        result = slow_task.enqueue(5.0)
        print(f"TASK_ID={result.id}", flush=True)

        # Wait for the task to actually start running
        for _ in range(100):
            db_result = DBTaskResult.objects.get(id=result.id)
            if db_result.status == "RUNNING":
                break
            time.sleep(0.1)
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
