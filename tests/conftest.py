import pytest
from django.tasks import task_backends

from django_tasks_local_db.backend import LocalDBBackend


@pytest.fixture(autouse=True)
def _cleanup_backend():
    """Shut down and restart the backend between tests.

    The watcher loop runs as a daemon thread and will pick up orphan rows
    from other tests if not stopped. This fixture ensures each test gets
    a fresh backend state.
    """
    yield

    for alias in task_backends:
        backend = task_backends[alias]
        if isinstance(backend, LocalDBBackend):
            backend.close()
            # Restart the watcher for the next test
            backend._ensure_watcher()
