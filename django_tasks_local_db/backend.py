import logging
import pickle
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from contextvars import ContextVar
from typing import Any

from django.db import close_old_connections, transaction
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import Task, TaskResult
from django.tasks.exceptions import TaskResultDoesNotExist
from django.utils.crypto import get_random_string
from django.utils.module_loading import import_string

from .state import get_executor_state, shutdown_executor

logger = logging.getLogger("django_tasks_local_db")

current_result_id: ContextVar[str] = ContextVar("current_result_id")


def _normalize_json(obj):
    """Normalize values for JSON serialization."""
    from collections.abc import Mapping, Sequence

    match obj:
        case Mapping():
            return {_normalize_json(k): _normalize_json(v) for k, v in obj.items()}
        case bytes():
            return obj.decode("utf-8")
        case str() | int() | float() | bool() | None:
            return obj
        case Sequence():
            return [_normalize_json(v) for v in obj]
        case _:
            raise TypeError(f"Unsupported type: {type(obj)}")


def _execute_task(func_path: str, args: tuple, kwargs: dict, result_id: str):
    """Execute a task function. Runs in thread/process pool."""
    token = current_result_id.set(result_id)
    try:
        obj = import_string(func_path)
        func = getattr(obj, "func", obj)
        return func(*args, **kwargs)
    finally:
        current_result_id.reset(token)


class FuturesBackend(BaseTaskBackend):
    supports_defer = False
    supports_async_task = False
    supports_get_result = True
    supports_priority = False
    executor_class: type = None

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

    def enqueue(self, task: Task, args=None, kwargs=None) -> TaskResult:
        from .models import DBTaskResult

        self.validate_task(task)
        self._validate_pickleable(task, args, kwargs)

        func_path = task.module_path
        normalized_args = _normalize_json({"args": list(args or ()), "kwargs": dict(kwargs or {})})

        from .models import _get_date_max

        db_result = DBTaskResult.objects.create(
            args_kwargs=normalized_args,
            priority=task.priority,
            task_path=func_path,
            queue_name=task.queue_name,
            run_after=task.run_after if task.run_after is not None else _get_date_max(),
            backend_name=self.alias,
        )

        result_id = str(db_result.id)

        state = self._get_state()
        future = state.executor.submit(
            _execute_task,
            func_path,
            tuple(normalized_args["args"]),
            normalized_args["kwargs"],
            result_id,
        )

        with state.lock:
            state.futures[result_id] = future

        future.add_done_callback(lambda f: self._on_complete(result_id, f))

        return db_result.task_result

    def _on_complete(self, result_id: str, future: Future) -> None:
        import time

        from .models import DBTaskResult

        try:
            close_old_connections()

            # Determine the outcome first
            task_exc = None
            return_value = None
            try:
                return_value = future.result()
                return_value = _normalize_json(return_value)
            except Exception as exc:
                logger.exception("Task %s failed", result_id)
                task_exc = exc

            # Retry DB writes to handle transient locking (e.g., SQLite)
            for attempt in range(5):
                try:
                    db_result = DBTaskResult.objects.get(id=result_id)
                    if task_exc is not None:
                        db_result.set_failed(task_exc)
                    else:
                        db_result.set_successful(return_value)
                    break
                except Exception:
                    if attempt == 4:
                        logger.exception("Error updating task result %s", result_id)
                    else:
                        time.sleep(0.1 * (attempt + 1))
        finally:
            state = self._get_state()
            with state.lock:
                state.futures.pop(result_id, None)

    def get_result(self, result_id: str) -> TaskResult:
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
        with transaction.atomic():
            orphaned = (
                DBTaskResult.objects.orphaned()
                .filter(backend_name=self.alias)
                .select_for_update(skip_locked=True)
            )
            for db_result in orphaned:
                result_id = str(db_result.id)
                db_result.claim(self._worker_id)

                state = self._get_state()
                future = state.executor.submit(
                    _execute_task,
                    db_result.task_path,
                    tuple(db_result.args_kwargs["args"]),
                    db_result.args_kwargs["kwargs"],
                    result_id,
                )
                with state.lock:
                    state.futures[result_id] = future
                future.add_done_callback(
                    lambda f, rid=result_id: self._on_complete(rid, f)
                )
                recovered += 1
                logger.info("Recovered orphaned task %s (%s)", result_id, db_result.task_path)

        return recovered

    def _validate_pickleable(self, task, args, kwargs):
        pass

    def close(self):
        shutdown_executor(self._name)


class ThreadPoolBackend(FuturesBackend):
    executor_class = ThreadPoolExecutor


class ProcessPoolBackend(FuturesBackend):
    executor_class = ProcessPoolExecutor

    def _validate_pickleable(self, task, args, kwargs):
        try:
            pickle.dumps((args, kwargs))
        except (pickle.PicklingError, TypeError, AttributeError) as e:
            raise ValueError(
                f"Task arguments must be pickleable for ProcessPoolBackend: {e}"
            ) from e
