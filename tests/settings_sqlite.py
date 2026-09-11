"""Test settings running against SQLite instead of PostgreSQL.

Run with: pytest --ds=tests.settings_sqlite
"""

from .settings import *  # noqa: F401,F403

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": "django_tasks_test.sqlite3",
        # Django would otherwise use a shared in-memory database, which
        # behaves badly when the watcher and task threads each hold their
        # own connection.
        "TEST": {"NAME": "django_tasks_test.sqlite3"},
        "OPTIONS": {"timeout": 20},
    }
}
