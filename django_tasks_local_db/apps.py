import logging
import signal
import sys

from django.apps import AppConfig

logger = logging.getLogger("django_tasks_local_db")


def _sigterm_handler(signum, frame):
    """Gracefully shut down all task executors on SIGTERM."""
    from .state import _executor_states, _registry_lock

    with _registry_lock:
        for name, state in list(_executor_states.items()):
            state.executor.shutdown(wait=True)

    sys.exit(0)


class DjangoTasksLocalDbConfig(AppConfig):
    name = "django_tasks_local_db"
    verbose_name = "Django Tasks Local DB"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self):
        import threading

        # Register SIGTERM handler for graceful shutdown (e.g. Cloud Run)
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGTERM, _sigterm_handler)

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
