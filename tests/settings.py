import os
import tempfile

SECRET_KEY = "test-secret-key-not-for-production"

DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "django_tasks_test",
            "USER": "django_tasks",
            "PASSWORD": "django_tasks",
            "HOST": "localhost",
            "PORT": "5433",
        }
    }
else:
    _TEST_DB_DIR = tempfile.mkdtemp(prefix="django_tasks_test_")
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": os.path.join(_TEST_DB_DIR, "test.sqlite3"),
            "OPTIONS": {
                "transaction_mode": "IMMEDIATE",
            },
            "TEST": {
                "NAME": os.path.join(_TEST_DB_DIR, "test.sqlite3"),
            },
        }
    }

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django_tasks_local_db",
]

MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "OPTIONS": {
            "context_processors": [
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
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
