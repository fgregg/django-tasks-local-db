# django-tasks-local-db

A [Django Tasks](https://docs.djangoproject.com/en/6.0/topics/tasks/) backend that combines in-process thread pool execution with database persistence. Tasks run in your web process without a separate worker, while results are durably stored in the database.

This project is a remix of [django-tasks-local](https://github.com/lincolnloop/django-tasks-local) and [django-tasks-db](https://github.com/RealOrangeOne/django-tasks-db). It builds directly on django-tasks-db's `DatabaseBackend` and `DBTaskResult` model, and adds the in-process executor approach from django-tasks-local.

## How it works

- `enqueue()` writes a task row to the database (django-tasks-db's `DBTaskResult`)
- A background watcher thread in each process polls for due tasks and runs them on an in-process thread pool, sending a heartbeat while they run
- On completion, the DB row is updated with the result or error
- A `RUNNING` task whose heartbeat has gone silent (e.g. the process died) is recovered and re-run by any live process

Tasks are claimed the same way django-tasks-db's `db_worker` claims them (`SELECT ... FOR UPDATE SKIP LOCKED` where available, an exclusive transaction on SQLite), so any database django-tasks-db supports works.

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

Heartbeats are written once per poll, so `HEARTBEAT_TIMEOUT` must be at least 3 × `POLL_INTERVAL`; `manage.py check` reports an error otherwise. The heartbeat records that the process which claimed a task is still alive and still has it in flight, not that the task is making progress: a task that hangs keeps its heartbeat until its process exits.

Then run migrations:

```bash
python manage.py migrate
```

Task results live in django-tasks-db's `DBTaskResult` table and show up in the admin under **Tasks Database Backend**, with the heartbeat column and a "Re-enqueue as a new task" action added by this package.

## Running on Cloud Run

Tasks run on background threads inside your web process, and by default Cloud Run only allocates CPU to an instance **while it is handling a request**. Between requests the whole process is throttled to near zero, which stalls both the tasks themselves and the watcher thread that sends their heartbeats. A task enqueued at the end of a request will crawl or freeze until the next request arrives, and if the freeze outlasts `HEARTBEAT_TIMEOUT`, another instance will treat the task as orphaned and run it a second time.

Turn throttling off with CPU always allocated:

```bash
gcloud run services update SERVICE --no-cpu-throttling
```

(or **CPU allocation: CPU is always allocated** in the console). This is a separate setting from scaling: an instance with CPU always allocated can still be scaled to zero when idle, at which point it is shut down normally and any in-flight tasks are recovered by heartbeat. If you also want tasks to keep running with no request traffic at all, set `--min-instances=1` so there is always a live process to run the watcher.

## Upgrading from 0.3.x

0.3.x carried its own copy of the `DBTaskResult` model. Add `"django_tasks_db"` to `INSTALLED_APPS` and run `migrate`: migration `0005` copies every row from the old `django_tasks_local_db_dbtaskresult` table into django-tasks-db's table (queued and in-flight tasks included) and then drops the old table. Code that imported `DBTaskResult` from `django_tasks_local_db.models` should import it from `django_tasks_db.models` instead.
