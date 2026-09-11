import logging
import threading
from contextlib import contextmanager
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from django.apps import apps
from django.core import checks
from django.db import close_old_connections
from django.tasks.base import TaskContext
from django.tasks.signals import task_finished, task_started
from django.utils import timezone
from django.utils.crypto import get_random_string
from django.utils.json import normalize_json
from django.utils.module_loading import import_string
from django_tasks_db.backend import DatabaseBackend
from django_tasks_db.utils import exclusive_transaction

from .state import get_executor_state, shutdown_executor

logger = logging.getLogger("django_tasks_local_db")


@contextmanager
def _db_connection():
    """Ensure stale connections are cleaned up on entry,
    and the thread-local connection is closed on exit."""
    from django.db import connection

    close_old_connections()
    try:
        yield
    finally:
        connection.close()


def _execute_task(
    func_path: str,
    args: tuple,
    kwargs: dict,
    result_id: str,
    backend_name: str,
):
    """Execute a task function. Runs in the thread pool."""
    from django_tasks_db.models import DBTaskResult

    with _db_connection():
        db_result = DBTaskResult.objects.get(id=result_id)

        task = import_string(func_path)
        task_result = db_result.task_result
        backend_type = type(task.get_backend())

        task_started.send(sender=backend_type, task_result=task_result)

        task_exc = None
        try:
            if task.takes_context:
                raw_return_value = task.call(
                    TaskContext(task_result=task_result), *args, **kwargs
                )
            else:
                raw_return_value = task.call(*args, **kwargs)
            return_value = normalize_json(raw_return_value)
        except BaseException as exc:
            # BaseException, not Exception: signals never reach pool threads,
            # so a SystemExit/KeyboardInterrupt/CancelledError here was raised
            # by the task's own code and is a deterministic failure. Letting it
            # escape would leave the row RUNNING and have heartbeat recovery
            # re-run it forever.
            task_exc = exc

        # Write result — if this fails, the exception propagates
        # and _on_complete logs a warning.
        db_result.refresh_from_db()
        if task_exc is not None:
            db_result.set_failed(task_exc)
        else:
            db_result.set_successful(return_value)


# Heartbeats are written once per watcher tick, so a task is only "stale"
# after this many consecutive ticks without one. Below this, a single slow
# tick could get a live task recovered and run twice.
MIN_HEARTBEAT_TIMEOUT_TICKS = 3


class LocalDBBackend(DatabaseBackend):
    """django-tasks-db's DatabaseBackend, plus an in-process thread pool
    that dispatches due tasks and recovers orphaned ones."""

    executor_class = ThreadPoolExecutor

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        options = params.get("OPTIONS", {})
        self._max_workers = options.get("MAX_WORKERS", 10)
        self._poll_interval = options.get("POLL_INTERVAL", 1.0)
        self._heartbeat_timeout = options.get("HEARTBEAT_TIMEOUT", 30)
        self._name = f"tasks-{alias}"
        self._worker_id = get_random_string(32)
        self._state = None
        self._watcher_started = False
        self._watcher_stop = threading.Event()
        self._watcher_thread = None
        self._watcher_lock = threading.Lock()

    def check(self, **kwargs: Any) -> list[checks.CheckMessage]:
        errors = super().check(**kwargs)
        if not apps.is_installed("django_tasks_local_db"):
            errors.append(
                checks.Error(
                    f"{type(self).__name__} configured as task backend, "
                    "but django_tasks_local_db app not installed",
                    "Insert 'django_tasks_local_db' in INSTALLED_APPS",
                )
            )
        min_timeout = MIN_HEARTBEAT_TIMEOUT_TICKS * self._poll_interval
        if self._heartbeat_timeout < min_timeout:
            errors.append(
                checks.Error(
                    f"HEARTBEAT_TIMEOUT ({self._heartbeat_timeout}s) is less than "
                    f"{MIN_HEARTBEAT_TIMEOUT_TICKS} x POLL_INTERVAL "
                    f"({self._poll_interval}s) for task backend '{self.alias}'. "
                    "Heartbeats are sent once per poll, so live tasks would be "
                    "treated as orphaned and run again.",
                    f"Set HEARTBEAT_TIMEOUT to at least {min_timeout:g}s, or "
                    "lower POLL_INTERVAL.",
                    id="django_tasks_local_db.E001",
                )
            )
        return errors

    def _get_state(self):
        if self._state is None:
            self._state = get_executor_state(
                name=self._name,
                executor_class=self.executor_class,
                max_workers=self._max_workers,
            )
        return self._state

    def _ensure_watcher(self):
        """Start the watcher loop if not already running."""
        with self._watcher_lock:
            if self._watcher_started:
                return
            self._watcher_started = True
            self._watcher_stop.clear()
            self._watcher_thread = threading.Thread(
                target=self._watcher_loop,
                daemon=True,
                name=f"tasks-watcher-{self.alias}",
            )
            self._watcher_thread.start()

    def _submit_task(self, result_id, task_path, args, kwargs) -> bool:
        """Submit a task to the thread pool. Returns False if the executor
        has already been shut down."""
        state = self._get_state()
        try:
            future = state.executor.submit(
                _execute_task,
                task_path,
                args,
                kwargs,
                result_id,
                self.alias,
            )
        except RuntimeError:
            # Executor was shut down between _get_state() and submit().
            # The task stays in RUNNING and will be recovered by heartbeat timeout.
            logger.debug("Executor shut down, task %s will be recovered later", result_id)
            return False
        else:
            with state.lock:
                state.futures[result_id] = future
            future.add_done_callback(
                lambda f, rid=result_id: self._on_complete(rid, f)
            )
            return True

    def _on_complete(self, result_id: str, future: Future) -> None:
        from django_tasks_db.models import DBTaskResult

        with _db_connection():
            try:
                exc = future.exception()
                if exc is not None:
                    logger.warning(
                        "Task %s completed but failed to write result to DB. "
                        "Task will be stuck in RUNNING until recovered.",
                        result_id,
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )
                    return

                try:
                    db_result = DBTaskResult.objects.get(id=result_id)
                    task_finished.send(
                        sender=type(self), task_result=db_result.task_result
                    )
                except Exception:
                    pass
            finally:
                if self._state is not None:
                    with self._state.lock:
                        self._state.futures.pop(result_id, None)

    def _watcher_loop(self):
        """Background loop: dispatch due tasks, update heartbeats, recover stale tasks."""
        # Brief delay to ensure Django startup completes
        self._watcher_stop.wait(timeout=0.5)

        with _db_connection():
            while not self._watcher_stop.is_set():
                try:
                    self._watcher_tick()
                except Exception:
                    logger.exception("Error in watcher loop for backend '%s'", self.alias)

                self._watcher_stop.wait(timeout=self._poll_interval)

    def _available_slots(self):
        """How many more tasks can be submitted to the thread pool."""
        state = self._get_state()
        with state.lock:
            inflight = len(state.futures)
        return max(0, self._max_workers - inflight)

    def _claim_and_submit(self, queryset, log_message=None):
        """Lock up to `available` rows from `queryset`, claim them for this
        worker, then submit them to the thread pool once the lock is released."""
        from .models import TaskHeartbeat

        available = self._available_slots()
        if available <= 0:
            return

        # Same claim strategy as db_worker: SKIP LOCKED where the database
        # has it, silently ignored on SQLite where exclusive_transaction
        # serialises claimers instead.
        to_submit = []
        with exclusive_transaction(queryset.db):
            rows = (
                queryset.filter(backend_name=self.alias)
                .select_for_update(skip_locked=True)[:available]
            )
            for db_result in rows:
                db_result.claim(self._worker_id)
                TaskHeartbeat.objects.update_or_create(
                    task_result=db_result,
                    defaults={"last_heartbeat_at": timezone.now()},
                )
                to_submit.append((
                    str(db_result.id),
                    db_result.task_path,
                    tuple(db_result.args_kwargs["args"]),
                    db_result.args_kwargs["kwargs"],
                ))

        for result_id, task_path, args, kwargs in to_submit:
            if self._submit_task(result_id, task_path, args, kwargs) and log_message:
                logger.info(log_message, result_id, task_path)

    def _watcher_tick(self):
        """One iteration of the watcher loop."""
        from django_tasks_db.models import DBTaskResult

        from .models import TaskHeartbeat, stale_results

        # 1. Update heartbeats for in-flight tasks
        state = self._get_state()
        with state.lock:
            inflight_ids = list(state.futures.keys())

        if inflight_ids:
            TaskHeartbeat.objects.filter(task_result_id__in=inflight_ids).update(
                last_heartbeat_at=timezone.now()
            )

        # 2. Dispatch due READY tasks (only up to available worker slots)
        self._claim_and_submit(DBTaskResult.objects.ready())

        # 3. Recover stale RUNNING tasks (also limited to available slots)
        self._claim_and_submit(
            stale_results(self._heartbeat_timeout),
            log_message="Recovered stale task %s (%s)",
        )

    def close(self):
        with self._watcher_lock:
            self._watcher_stop.set()
            if self._watcher_thread is not None:
                self._watcher_thread.join()
                self._watcher_thread = None
            self._watcher_started = False
        shutdown_executor(self._name)
        self._state = None
