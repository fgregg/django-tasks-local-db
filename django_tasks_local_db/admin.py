from django.contrib import admin, messages
from django.core.exceptions import SuspiciousOperation
from django.tasks import TaskResultStatus
from django.utils.html import format_html

from .models import DBTaskResult


@admin.register(DBTaskResult)
class DBTaskResultAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "task_name",
        "status",
        "enqueued_at",
        "started_at",
        "finished_at",
        "run_after",
        "last_heartbeat_at",
        "priority",
        "queue_name",
    ]
    list_filter = ["status", "priority", "queue_name", "backend_name"]
    actions = ["requeue"]
    readonly_fields = [
        "id",
        "task_name",
        "task_path",
        "status",
        "enqueued_at",
        "started_at",
        "finished_at",
        "run_after",
        "last_heartbeat_at",
        "priority",
        "queue_name",
        "backend_name",
        "args_kwargs",
        "return_value",
        "exception_class_path",
        "formatted_traceback",
    ]
    exclude = ["traceback", "worker_ids"]

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_requeue_permission(self, request):
        # Rows stay read-only (has_change_permission is False at the
        # ModelAdmin level), but re-enqueueing is gated on the standard
        # change permission for the model.
        return request.user.has_perm("django_tasks_local_db.change_dbtaskresult")

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

    @admin.display(description="Task name")
    def task_name(self, obj):
        return obj.task_name

    @admin.display(description="Traceback")
    def formatted_traceback(self, obj):
        if obj.traceback:
            return format_html("<pre>{}</pre>", obj.traceback)
        return ""
