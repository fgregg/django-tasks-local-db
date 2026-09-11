from django.contrib import admin, messages
from django.contrib.admin.sites import NotRegistered
from django.core.exceptions import SuspiciousOperation
from django.tasks import TaskResultStatus
from django_tasks_db.admin import DBTaskResultAdmin as BaseDBTaskResultAdmin
from django_tasks_db.models import DBTaskResult, get_date_max


class DBTaskResultAdmin(BaseDBTaskResultAdmin):
    """django-tasks-db's admin, plus heartbeat visibility and re-enqueueing."""

    list_display = (
        *BaseDBTaskResultAdmin.list_display,
        "run_after_or_empty",
        "last_heartbeat_at",
    )
    list_filter = (*BaseDBTaskResultAdmin.list_filter, "backend_name")
    actions = ["requeue"]

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("heartbeat")

    def get_readonly_fields(self, request, obj=None):
        return [*super().get_readonly_fields(request, obj), "last_heartbeat_at"]

    @admin.display(description="run after", ordering="run_after")
    def run_after_or_empty(self, obj):
        # Upstream stores "no run_after" as a far-future sentinel rather than NULL.
        if obj.run_after == get_date_max():
            return self.get_empty_value_display()
        return obj.run_after

    @admin.display(description="last heartbeat at")
    def last_heartbeat_at(self, obj):
        try:
            return obj.heartbeat.last_heartbeat_at
        except DBTaskResult.heartbeat.RelatedObjectDoesNotExist:
            return self.get_empty_value_display()

    def has_requeue_permission(self, request):
        # Rows stay read-only (has_change_permission is False at the
        # ModelAdmin level), but re-enqueueing is gated on the standard
        # change permission for the model.
        return request.user.has_perm("django_tasks_database.change_dbtaskresult")

    @admin.action(
        description="Re-enqueue as a new task", permissions=["requeue"]
    )
    def requeue(self, request, queryset):
        """Re-enqueue FAILED tasks as fresh tasks via the normal backend path.

        The original row is left untouched (audit trail); a new task is
        enqueued with the same task, args, kwargs, priority, and queue. Only
        FAILED rows are eligible — anything else is skipped. The new task's
        kwargs are exactly the original's, so app-level retry conventions
        (e.g. an `attempt` kwarg) carry over verbatim.
        """
        requeued = 0
        skipped = 0
        for db_result in queryset:
            if db_result.status != TaskResultStatus.FAILED:
                skipped += 1
                continue
            args = db_result.args_kwargs.get("args", [])
            kwargs = db_result.args_kwargs.get("kwargs", {})
            try:
                new_result = db_result.task.enqueue(*args, **kwargs)
            except (ImportError, SuspiciousOperation) as exc:
                self.message_user(
                    request,
                    f"Could not re-enqueue {db_result.id} "
                    f"({db_result.task_path}): {exc}",
                    level=messages.ERROR,
                )
            else:
                requeued += 1
                self.message_user(
                    request,
                    f"Re-enqueued {db_result.id} as new task {new_result.id}.",
                    level=messages.SUCCESS,
                )
        if skipped:
            self.message_user(
                request,
                f"Skipped {skipped} task(s) not in FAILED status.",
                level=messages.WARNING,
            )


# Replace django-tasks-db's registration with ours. Importing
# django_tasks_db.admin above guarantees upstream registered first,
# regardless of INSTALLED_APPS order.
try:
    admin.site.unregister(DBTaskResult)
except NotRegistered:
    pass
admin.site.register(DBTaskResult, DBTaskResultAdmin)
