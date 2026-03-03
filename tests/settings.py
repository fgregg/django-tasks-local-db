SECRET_KEY = "test-secret-key-not-for-production"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
        "OPTIONS": {
            "transaction_mode": "IMMEDIATE",
        },
    }
}

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django_tasks_local_db",
]

TASKS = {
    "default": {
        "BACKEND": "django_tasks_local_db.LocalDBBackend",
        "OPTIONS": {
            "MAX_WORKERS": 2,
        },
    }
}

USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
