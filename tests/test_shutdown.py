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
def test_enqueue_after_close_and_restart(backend):
    """After close() + _ensure_watcher(), enqueue should still work."""
    # Ensure we have an active executor first, then shut it down
    backend._get_state()
    backend.close()

    # Restart the watcher (in production this would happen on process restart)
    backend._ensure_watcher()

    # enqueue() should succeed — the restarted watcher picks up the task
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


def test_sigkill_leaves_task_for_recovery():
    """SIGKILL: task is left RUNNING with a stale heartbeat.

    Simulates OOM or hard kill. The task can't finish gracefully, so it
    stays RUNNING. A new process's watcher should recover it via heartbeat
    timeout. Here we just verify the task is stuck in RUNNING after kill.
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

    # SIGKILL — no graceful shutdown, threads die immediately
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=10)

    # Check DB state — task should be stuck in RUNNING (no chance to finish)
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

    # After SIGKILL, the task should be stuck in RUNNING — it had no
    # chance to complete. A new process's watcher would recover it via
    # heartbeat timeout.
    assert status == "RUNNING", (
        f"Expected RUNNING after SIGKILL, got {status}"
    )


def _make_enqueue_during_sigterm_script():
    """Script that enqueues a task during SIGTERM grace period."""
    return textwrap.dedent("""\
        import os, sys, signal, time, django
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

        from django.core.management import call_command
        call_command("migrate", "--run-syncdb", verbosity=0)

        from tests.tasks import add_numbers

        # Override SIGTERM: enqueue a task during the grace period, then exit
        def sigterm_handler(signum, frame):
            result = add_numbers.enqueue(10, 20)
            print(f"LATE_TASK_ID={result.id}", flush=True)
            # Give the watcher a moment to dispatch
            time.sleep(3)
            sys.exit(0)

        signal.signal(signal.SIGTERM, sigterm_handler)

        print("READY", flush=True)
        time.sleep(30)
    """)


def test_enqueue_during_sigterm_grace_period():
    """A task enqueued during SIGTERM handling should not be lost.

    The task is written to DB during the SIGTERM handler. Even if the
    process exits before it completes, it should be in the DB (READY
    or a terminal state) — not lost.
    """
    import psycopg

    script = _make_enqueue_during_sigterm_script()

    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    deadline = time.monotonic() + 15
    ready = False
    while time.monotonic() < deadline:
        line = proc.stdout.readline().strip()
        if line == "READY":
            ready = True
            break

    assert ready, f"Never got READY.\nstderr: {proc.stderr.read()}"

    # Send SIGTERM — the handler will enqueue a task
    proc.send_signal(signal.SIGTERM)

    # Read the late task ID
    task_id = None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        line = proc.stdout.readline().strip()
        if line.startswith("LATE_TASK_ID="):
            task_id = line.split("=", 1)[1]
            break

    proc.wait(timeout=10)

    assert task_id is not None, (
        f"Never got LATE_TASK_ID.\nstderr: {proc.stderr.read()}"
    )

    # Check DB — the task should exist (READY, SUCCESSFUL, or FAILED — not missing)
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

    assert row is not None, (
        f"Task {task_id} enqueued during SIGTERM was lost — not found in DB"
    )
    # The task was enqueued and the process waited 3s, so it may have completed
    status = row[0]
    assert status in ("READY", "SUCCESSFUL", "FAILED"), (
        f"Unexpected status {status} for task enqueued during SIGTERM"
    )
