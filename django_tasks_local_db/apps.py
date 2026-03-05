import logging

from django.apps import AppConfig

logger = logging.getLogger("django_tasks_local_db")


class DjangoTasksLocalDbConfig(AppConfig):
    name = "django_tasks_local_db"
    verbose_name = "Django Tasks Local DB"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self):
        import threading

        # Defer recovery until after Django is fully initialized.
        # This avoids "database accessed during app initialization" warnings
        # and ensures the task backends are fully configured.
        thread = threading.Thread(target=self._recover_orphaned_tasks, daemon=True)
        thread.start()

    def _recover_orphaned_tasks(self):
        import time

        from django.conf import settings

        # Brief delay to ensure Django startup completes
        time.sleep(0.5)

        from .backend import LocalDBBackend

        tasks_settings = getattr(settings, "TASKS", {})
        for alias, params in tasks_settings.items():
            backend_path = params.get("BACKEND", "")
            if "django_tasks_local_db" not in backend_path:
                continue
            try:
                from django.tasks import task_backends

                backend = task_backends[alias]
                if isinstance(backend, LocalDBBackend):
                    recovered = backend.recover_tasks()
                    if recovered:
                        logger.info(
                            "Recovered %d orphaned task(s) for backend '%s'",
                            recovered,
                            alias,
                        )
            except Exception:
                logger.exception(
                    "Failed to recover tasks for backend '%s'", alias
                )
