import os

SECRET_KEY = "test-secret-key-not-for-production"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("PGDATABASE", "django_tasks_test"),
        "USER": os.environ.get("PGUSER", "django_tasks"),
        "PASSWORD": os.environ.get("PGPASSWORD", "django_tasks"),
        "HOST": os.environ.get("PGHOST", "localhost"),
        "PORT": os.environ.get("PGPORT", "5433"),
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
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
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

ROOT_URLCONF = "tests.urls"

USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
