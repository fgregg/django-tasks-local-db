import logging
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from django.db import close_old_connections, transaction
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import Task, TaskContext, TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils.crypto import get_random_string
from django.utils.json import normalize_json
from django.utils.module_loading import import_string

from .state import get_executor_state, shutdown_executor

logger = logging.getLogger("django_tasks_local_db")


def _execute_task(
    func_path: str,
    args: tuple,
    kwargs: dict,
    result_id: str,
    backend_name: str,
    worker_id: str,
):
    """Execute a task function. Runs in the thread pool.

    Holds a ``SELECT FOR UPDATE`` lock on the task row for the entire
    duration of execution.  This prevents ``recover_tasks()`` (which
    uses ``skip_locked=True``) from resubmitting a task that is still
    running — even across processes.
    """
    from .models import DBTaskResult

    close_old_connections()

    # Claim immediately (autocommit) so the RUNNING status is visible
    db_result = DBTaskResult.objects.get(id=result_id)
    db_result.claim(worker_id)

    task = import_string(func_path)
    task_result = db_result.task_result
    backend_type = type(task.get_backend())

    task_started.send(sender=backend_type, task_result=task_result)

    # Hold a row lock for the entire execution + result write.
    # recover_tasks() uses skip_locked=True, so it will skip this row.
    with transaction.atomic():
        db_result = DBTaskResult.objects.select_for_update().get(id=result_id)

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

        # Write result — if this fails, the exception propagates,
        # the transaction rolls back, and _on_complete logs a warning.
        if task_exc is not None:
            db_result.set_failed(task_exc)
        else:
            db_result.set_successful(return_value)


class LocalDBBackend(BaseTaskBackend):
    supports_defer = False
    supports_async_task = False
    supports_get_result = True
    supports_priority = False
    executor_class = ThreadPoolExecutor

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        options = params.get("OPTIONS", {})
        self._max_workers = options.get("MAX_WORKERS", 10)
        self._name = f"tasks-{alias}"
        self._worker_id = get_random_string(32)
        self._state = None

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
            exception_class_path="",
            traceback="",
        )

        result_id = str(db_result.id)

        task_enqueued.send(type(self), task_result=db_result.task_result)

        state = self._get_state()
        future = state.executor.submit(
            _execute_task,
            func_path,
            tuple(normalized_args["args"]),
            normalized_args["kwargs"],
            result_id,
            self.alias,
            self._worker_id,
        )

        with state.lock:
            state.futures[result_id] = future

        future.add_done_callback(lambda f: self._on_complete(result_id, f))

        return db_result.task_result

    def _on_complete(self, result_id: str, future: Future) -> None:
        from .models import DBTaskResult

        try:
            close_old_connections()

            exc = future.exception()
            if exc is not None:
                logger.warning(
                    "Task %s completed but failed to write result to DB. "
                    "Task will be stuck in RUNNING until recovered on "
                    "next startup.",
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

    def get_result(self, result_id: str):
        from .models import DBTaskResult

        try:
            return DBTaskResult.objects.get(id=result_id).task_result
        except DBTaskResult.DoesNotExist as e:
            raise TaskResultDoesNotExist(result_id) from e

    def recover_tasks(self) -> int:
        """Recover orphaned tasks from DB and resubmit to the executor.

        Returns the number of tasks recovered.

        Uses ``select_for_update(skip_locked=True)`` so that concurrent
        recovery calls (e.g. multiple processes starting simultaneously)
        each claim a disjoint set of tasks.  Requires a database that
        supports row-level locking (PostgreSQL, MySQL).
        """
        from .models import DBTaskResult

        self._check_db_support()

        recovered = 0
        tasks_to_submit = []
        with transaction.atomic():
            orphaned = (
                DBTaskResult.objects.filter(
                    status__in=[TaskResultStatus.READY, TaskResultStatus.RUNNING],
                    backend_name=self.alias,
                )
                .select_for_update(skip_locked=True)
            )
            for db_result in orphaned:
                result_id = str(db_result.id)
                db_result.claim(self._worker_id)
                tasks_to_submit.append((
                    result_id,
                    db_result.task_path,
                    tuple(db_result.args_kwargs["args"]),
                    db_result.args_kwargs["kwargs"],
                ))
                recovered += 1
                logger.info(
                    "Recovered orphaned task %s (%s)",
                    result_id,
                    db_result.task_path,
                )

        for result_id, task_path, args, kwargs in tasks_to_submit:
            state = self._get_state()
            future = state.executor.submit(
                _execute_task,
                task_path,
                args,
                kwargs,
                result_id,
                self.alias,
                self._worker_id,
            )
            with state.lock:
                state.futures[result_id] = future
            future.add_done_callback(
                lambda f, rid=result_id: self._on_complete(rid, f)
            )

        return recovered

    def close(self):
        shutdown_executor(self._name)
        self._state = None
