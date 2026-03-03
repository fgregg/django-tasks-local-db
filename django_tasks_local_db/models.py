import datetime
import logging
import uuid

from django.conf import settings
from django.db import models
from django.db.models import F, Q
from django.tasks.base import (
    DEFAULT_TASK_PRIORITY,
    DEFAULT_TASK_QUEUE_NAME,
    TASK_MAX_PRIORITY,
    TASK_MIN_PRIORITY,
    Task,
    TaskError,
    TaskResult,
    TaskResultStatus,
)
from django.utils import timezone
from django.utils.module_loading import import_string

logger = logging.getLogger("django_tasks_local_db")


def _get_module_path(val):
    return f"{val.__module__}.{val.__qualname__}"


def _get_exception_traceback(exc):
    from traceback import format_exception

    return "".join(format_exception(exc))


def _get_date_max():
    return datetime.datetime(
        9999, 1, 1, tzinfo=datetime.timezone.utc if settings.USE_TZ else None
    )


class DBTaskResultQuerySet(models.QuerySet):
    def ready(self):
        return self.filter(status=TaskResultStatus.READY).filter(
            Q(run_after=_get_date_max()) | Q(run_after__lte=timezone.now())
        )

    def running(self):
        return self.filter(status=TaskResultStatus.RUNNING)

    def orphaned(self):
        """Tasks that are READY or RUNNING — candidates for recovery."""
        return self.filter(status__in=[TaskResultStatus.READY, TaskResultStatus.RUNNING])

    def get_locked(self):
        """Claim a single task using SELECT ... FOR UPDATE SKIP LOCKED.

        Falls back to a plain first() on backends that don't support
        select_for_update (e.g., SQLite).
        """
        from django.db import connection

        if connection.features.has_select_for_update_skip_locked:
            return self.select_for_update(skip_locked=True).first()
        return self.first()


class DBTaskResult(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    status = models.CharField(
        choices=TaskResultStatus.choices,
        default=TaskResultStatus.READY,
        max_length=max(len(value) for value in TaskResultStatus.values),
    )
    enqueued_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True)
    finished_at = models.DateTimeField(null=True)
    args_kwargs = models.JSONField()
    priority = models.IntegerField(default=DEFAULT_TASK_PRIORITY)
    task_path = models.TextField()
    worker_ids = models.JSONField(default=list)
    queue_name = models.CharField(default=DEFAULT_TASK_QUEUE_NAME, max_length=32)
    backend_name = models.CharField(max_length=32)
    run_after = models.DateTimeField(default=_get_date_max)
    return_value = models.JSONField(default=None, null=True)
    exception_class_path = models.TextField(default="")
    traceback = models.TextField(default="")

    objects = DBTaskResultQuerySet.as_manager()

    class Meta:
        ordering = [F("priority").desc(), F("run_after").asc()]
        indexes = [
            models.Index(
                "status",
                F("priority").desc(),
                F("run_after").asc(),
                name="local_db_ready_idx",
                condition=Q(status=TaskResultStatus.READY),
            ),
            models.Index(fields=["backend_name"]),
        ]
        constraints = [
            models.CheckConstraint(
                condition=Q(
                    priority__gte=TASK_MIN_PRIORITY,
                    priority__lte=TASK_MAX_PRIORITY,
                ),
                name="local_db_priority_range",
            ),
        ]

    @property
    def task(self) -> Task:
        task_obj = import_string(self.task_path)
        if not isinstance(task_obj, Task):
            raise ValueError(
                f"Task {self.id} does not point to a Task ({self.task_path})"
            )
        return task_obj.using(
            priority=self.priority,
            queue_name=self.queue_name,
            run_after=None if self.run_after == _get_date_max() else self.run_after,
            backend=self.backend_name,
        )

    @property
    def task_result(self) -> TaskResult:
        task_result = TaskResult(
            task=self.task,
            id=str(self.id),
            status=TaskResultStatus[self.status],
            enqueued_at=self.enqueued_at,
            started_at=self.started_at,
            last_attempted_at=self.started_at,
            finished_at=self.finished_at,
            args=self.args_kwargs["args"],
            kwargs=self.args_kwargs["kwargs"],
            backend=self.backend_name,
            errors=[],
            worker_ids=self.worker_ids,
        )
        if self.status == TaskResultStatus.FAILED and self.exception_class_path:
            task_result.errors.append(
                TaskError(
                    exception_class_path=self.exception_class_path,
                    traceback=self.traceback,
                )
            )
        object.__setattr__(task_result, "_return_value", self.return_value)
        return task_result

    def claim(self, worker_id: str) -> None:
        self.status = TaskResultStatus.RUNNING
        self.started_at = timezone.now()
        self.worker_ids = [*self.worker_ids, worker_id]
        self.save(update_fields=["status", "started_at", "worker_ids"])

    def set_successful(self, return_value) -> None:
        self.status = TaskResultStatus.SUCCESSFUL
        self.finished_at = timezone.now()
        self.return_value = return_value
        self.exception_class_path = ""
        self.traceback = ""
        self.save(
            update_fields=[
                "status",
                "return_value",
                "finished_at",
                "exception_class_path",
                "traceback",
            ]
        )

    def set_failed(self, exc: BaseException) -> None:
        self.status = TaskResultStatus.FAILED
        self.finished_at = timezone.now()
        self.exception_class_path = _get_module_path(type(exc))
        self.traceback = _get_exception_traceback(exc)
        self.return_value = None
        self.save(
            update_fields=[
                "status",
                "return_value",
                "finished_at",
                "exception_class_path",
                "traceback",
            ]
        )

    def __str__(self):
        return f"{self.task_path} ({self.status})"
