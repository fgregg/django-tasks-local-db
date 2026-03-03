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
    """Execute a task function. Runs in the thread pool."""
    from .models import DBTaskResult

    close_old_connections()

    db_result = DBTaskResult.objects.get(id=result_id)
    db_result.claim(worker_id)

    task = import_string(func_path)
    task_result = db_result.task_result
    backend_type = type(task.get_backend())

    task_started.send(sender=backend_type, task_result=task_result)

    if task.takes_context:
        raw_return_value = task.call(
            TaskContext(task_result=task_result), *args, **kwargs
        )
    else:
        raw_return_value = task.call(*args, **kwargs)

    return normalize_json(raw_return_value)


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

    def _get_state(self):
        if self._state is None:
            self._state = get_executor_state(
                name=self._name,
                executor_class=self.executor_class,
                max_workers=self._max_workers,
            )
        return self._state

    def enqueue(self, task: Task, args=None, kwargs=None):
        from .models import DBTaskResult, _get_date_max

        self.validate_task(task)

        func_path = task.module_path
        normalized_args = normalize_json({"args": list(args or ()), "kwargs": dict(kwargs or {})})

        db_result = DBTaskResult.objects.create(
            args_kwargs=normalized_args,
            priority=task.priority,
            task_path=func_path,
            queue_name=task.queue_name,
            run_after=task.run_after if task.run_after is not None else _get_date_max(),
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

            task_exc = None
            return_value = None
            try:
                return_value = future.result()
            except Exception as exc:
                task_exc = exc

            db_result = DBTaskResult.objects.get(id=result_id)
            if task_exc is not None:
                db_result.set_failed(task_exc)
            else:
                db_result.set_successful(return_value)

            task_finished.send(
                sender=type(self), task_result=db_result.task_result
            )
        finally:
            state = self._get_state()
            with state.lock:
                state.futures.pop(result_id, None)

    def get_result(self, result_id: str):
        from .models import DBTaskResult

        try:
            return DBTaskResult.objects.get(id=result_id).task_result
        except DBTaskResult.DoesNotExist as e:
            raise TaskResultDoesNotExist(result_id) from e

    def recover_tasks(self) -> int:
        """Recover orphaned tasks from DB and resubmit to the executor.

        Returns the number of tasks recovered.
        """
        from .models import DBTaskResult

        recovered = 0
        tasks_to_submit = []
        with transaction.atomic():
            orphaned = (
                DBTaskResult.objects.filter(
                    status__in=[TaskResultStatus.READY, TaskResultStatus.RUNNING]
                )
                .filter(backend_name=self.alias)
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
                logger.info("Recovered orphaned task %s (%s)", result_id, db_result.task_path)

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
