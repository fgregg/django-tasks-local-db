from django.contrib import admin
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

    @admin.display(description="Task name")
    def task_name(self, obj):
        return obj.task_name

    @admin.display(description="Traceback")
    def formatted_traceback(self, obj):
        if obj.traceback:
            return format_html("<pre>{}</pre>", obj.traceback)
        return ""
