import logging
import threading
from contextlib import contextmanager
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from django.db import close_old_connections, transaction
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import Task, TaskContext, TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone
from django.utils.crypto import get_random_string
from django.utils.json import normalize_json
from django.utils.module_loading import import_string

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
    from .models import DBTaskResult

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
        except Exception as exc:
            task_exc = exc

        # Write result — if this fails, the exception propagates
        # and _on_complete logs a warning.
        db_result.refresh_from_db()
        if task_exc is not None:
            db_result.set_failed(task_exc)
        else:
            db_result.set_successful(return_value)


class LocalDBBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = False
    supports_get_result = True
    supports_priority = True
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

    def _check_db_support(self):
        from django.db import connection

        if not connection.features.has_select_for_update_skip_locked:
            raise RuntimeError(
                f"LocalDBBackend requires a database that supports "
                f"SELECT FOR UPDATE SKIP LOCKED (e.g. PostgreSQL, MySQL). "
                f"The current database engine ({connection.vendor}) does not "
                f"support row-level locking."
            )

    def _get_state(self):
        if self._state is None:
            self._check_db_support()
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

    def enqueue(self, task: Task, args=None, kwargs=None):
        from .models import DBTaskResult

        self.validate_task(task)

        func_path = task.module_path
        normalized_args = normalize_json({"args": list(args or ()), "kwargs": dict(kwargs or {})})

        db_result = DBTaskResult.objects.create(
            args_kwargs=normalized_args,
            priority=task.priority,
            task_path=func_path,
            queue_name=task.queue_name,
            backend_name=self.alias,
            run_after=task.run_after,
            exception_class_path="",
            traceback="",
        )

        task_enqueued.send(type(self), task_result=db_result.task_result)

        return db_result.task_result

    def _submit_task(self, result_id, task_path, args, kwargs):
        """Submit a task to the thread pool."""
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
            return
        with state.lock:
            state.futures[result_id] = future
        future.add_done_callback(
            lambda f, rid=result_id: self._on_complete(rid, f)
        )

    def _on_complete(self, result_id: str, future: Future) -> None:
        from .models import DBTaskResult

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

    def _watcher_tick(self):
        """One iteration of the watcher loop."""
        from .models import DBTaskResult

        # 1. Update heartbeats for in-flight tasks
        state = self._get_state()
        with state.lock:
            inflight_ids = list(state.futures.keys())

        if inflight_ids:
            DBTaskResult.objects.filter(
                id__in=inflight_ids,
                status=TaskResultStatus.RUNNING,
            ).update(last_heartbeat_at=timezone.now())

        # 2. Dispatch due READY tasks (only up to available worker slots)
        available = self._available_slots()
        tasks_to_submit = []
        if available > 0:
            with transaction.atomic():
                due_tasks = (
                    DBTaskResult.objects.due()
                    .filter(backend_name=self.alias)
                    .select_for_update(skip_locked=True)[:available]
                )
                for db_result in due_tasks:
                    db_result.claim(self._worker_id)
                    result_id = str(db_result.id)
                    tasks_to_submit.append((
                        result_id,
                        db_result.task_path,
                        tuple(db_result.args_kwargs["args"]),
                        db_result.args_kwargs["kwargs"],
                    ))

            for result_id, task_path, args, kwargs in tasks_to_submit:
                self._submit_task(result_id, task_path, args, kwargs)

        # 3. Recover stale RUNNING tasks (also limited to available slots)
        available = self._available_slots()
        stale_to_submit = []
        if available > 0:
            with transaction.atomic():
                stale_tasks = (
                    DBTaskResult.objects.stale(self._heartbeat_timeout)
                    .filter(backend_name=self.alias)
                    .select_for_update(skip_locked=True)[:available]
                )
                for db_result in stale_tasks:
                    db_result.claim(self._worker_id)
                    result_id = str(db_result.id)
                    stale_to_submit.append((
                        result_id,
                        db_result.task_path,
                        tuple(db_result.args_kwargs["args"]),
                        db_result.args_kwargs["kwargs"],
                    ))
                    logger.info(
                        "Recovered stale task %s (%s)",
                        result_id,
                        db_result.task_path,
                    )

            for result_id, task_path, args, kwargs in stale_to_submit:
                self._submit_task(result_id, task_path, args, kwargs)

    def get_result(self, result_id: str):
        from .models import DBTaskResult

        try:
            return DBTaskResult.objects.get(id=result_id).task_result
        except DBTaskResult.DoesNotExist as e:
            raise TaskResultDoesNotExist(result_id) from e

    def close(self):
        with self._watcher_lock:
            self._watcher_stop.set()
            if self._watcher_thread is not None:
                self._watcher_thread.join()
                self._watcher_thread = None
            self._watcher_started = False
        shutdown_executor(self._name)
        self._state = None
