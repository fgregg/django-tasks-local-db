# django-tasks-local-db

A [Django Tasks](https://docs.djangoproject.com/en/6.0/topics/tasks/) backend that combines in-process thread pool execution with database persistence. Tasks run in your web process without a separate worker, while results are durably stored in the database.

This project is a remix of [django-tasks-local](https://github.com/lincolnloop/django-tasks-local) and [django-tasks-db](https://github.com/RealOrangeOne/django-tasks-db). It builds directly on django-tasks-db's `DatabaseBackend` and `DBTaskResult` model, and adds the in-process executor approach from django-tasks-local.

## How it works

- `enqueue()` writes a task row to the database (django-tasks-db's `DBTaskResult`)
- A background watcher thread in each process polls for due tasks and runs them on an in-process thread pool, sending a heartbeat while they run
- On completion, the DB row is updated with the result or error
- A `RUNNING` task whose heartbeat has gone silent (e.g. the process died) is recovered and re-run by any live process

Requires a database that supports `SELECT ... FOR UPDATE SKIP LOCKED` (PostgreSQL, MySQL 8+).

## Installation

```bash
pip install django-tasks-local-db
```

## Configuration

```python
INSTALLED_APPS = [
    # ...
    "django_tasks_db",
    "django_tasks_local_db",
]

TASKS = {
    "default": {
        "BACKEND": "django_tasks_local_db.LocalDBBackend",
        "OPTIONS": {
            "MAX_WORKERS": 4,          # thread pool size (default 10)
            "POLL_INTERVAL": 1.0,      # seconds between watcher ticks (default 1.0)
            "HEARTBEAT_TIMEOUT": 30,   # seconds of silence before a RUNNING task is recovered (default 30)
        },
    }
}
```

Then run migrations:

```bash
python manage.py migrate
```

Task results live in django-tasks-db's `DBTaskResult` table and show up in the admin under **Tasks Database Backend**, with the heartbeat column and a "Re-enqueue as a new task" action added by this package.

## Upgrading from 0.3.x

0.3.x carried its own copy of the `DBTaskResult` model. Add `"django_tasks_db"` to `INSTALLED_APPS` and run `migrate`: migration `0005` copies every row from the old `django_tasks_local_db_dbtaskresult` table into django-tasks-db's table (queued and in-flight tasks included) and then drops the old table. Code that imported `DBTaskResult` from `django_tasks_local_db.models` should import it from `django_tasks_db.models` instead.
