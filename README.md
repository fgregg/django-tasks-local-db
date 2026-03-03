# django-tasks-local-db

A Django Tasks backend that combines in-process thread pool execution with database persistence. Ideal for environments like Cloud Run where running a separate worker process adds significant infrastructure complexity.

## How it works

- `enqueue()` writes a task row to PostgreSQL, then submits it to a `ThreadPoolExecutor` in the same process
- On completion, the DB row is updated with the result or error
- On cold start (e.g., Cloud Run instance spin-up), orphaned tasks are recovered from the DB and resubmitted
- `SELECT ... FOR UPDATE SKIP LOCKED` prevents double-execution across multiple instances

## Installation

```bash
pip install django-tasks-local-db
```

## Configuration

```python
INSTALLED_APPS = [
    # ...
    "django_tasks_local_db",
]

TASKS = {
    "default": {
        "BACKEND": "django_tasks_local_db.ThreadPoolBackend",
        "OPTIONS": {
            "MAX_WORKERS": 4,
        },
    }
}
```

Then run migrations:

```bash
python manage.py migrate django_tasks_local_db
```

## Backends

- **`ThreadPoolBackend`** - Uses `concurrent.futures.ThreadPoolExecutor` (recommended)
- **`ProcessPoolBackend`** - Uses `concurrent.futures.ProcessPoolExecutor` (arguments must be pickleable)
