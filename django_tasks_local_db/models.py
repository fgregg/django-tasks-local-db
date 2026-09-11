from datetime import timedelta

from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _
from django_tasks_db.models import DBTaskResult, DBTaskResultQuerySet


class TaskHeartbeat(models.Model):
    """Liveness marker for a RUNNING task.

    django-tasks-db's DBTaskResult has no heartbeat column, so this side
    table records when the in-process executor last confirmed a task was
    still being worked on. A RUNNING task with no heartbeat, or one older
    than the backend's HEARTBEAT_TIMEOUT, is treated as orphaned and
    re-dispatched.
    """

    task_result = models.OneToOneField(
        DBTaskResult,
        primary_key=True,
        on_delete=models.CASCADE,
        related_name="heartbeat",
        verbose_name=_("task result"),
    )
    last_heartbeat_at = models.DateTimeField(verbose_name=_("last heartbeat at"))

    class Meta:
        verbose_name = _("task heartbeat")
        verbose_name_plural = _("task heartbeats")

    def __str__(self):
        return f"{self.task_result_id} @ {self.last_heartbeat_at}"


def stale_results(timeout: float) -> DBTaskResultQuerySet:
    """RUNNING tasks with no heartbeat, or one older than `timeout` seconds."""
    cutoff = timezone.now() - timedelta(seconds=timeout)
    fresh = TaskHeartbeat.objects.filter(last_heartbeat_at__gte=cutoff).values(
        "task_result_id"
    )
    return DBTaskResult.objects.running().exclude(id__in=fresh)
