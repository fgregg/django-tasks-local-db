# django-tasks-local-db

A [Django Tasks](https://docs.djangoproject.com/en/6.0/topics/tasks/) backend that combines in-process thread pool execution with database persistence. Tasks run in your web process without a separate worker, while results are durably stored in the database.

This project is a remix of [django-tasks-local](https://github.com/lincolnloop/django-tasks-local) and [django-tasks-db](https://github.com/RealOrangeOne/django-tasks-db). It takes the in-process executor approach from django-tasks-local and adds DB-backed result storage from django-tasks-db.

## How it works

- `enqueue()` writes a task row to the database, then submits it to an in-process thread pool
- On completion, the DB row is updated with the result or error
- On restart, orphaned tasks are recovered from the database and resubmitted

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
        "BACKEND": "django_tasks_local_db.LocalDBBackend",
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
