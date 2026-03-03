import datetime
import uuid

import django.db.models.functions.comparison
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="DBTaskResult",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4,
                        editable=False,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("READY", "Ready"),
                            ("RUNNING", "Running"),
                            ("FAILED", "Failed"),
                            ("SUCCESSFUL", "Successful"),
                        ],
                        default="READY",
                        max_length=10,
                    ),
                ),
                ("enqueued_at", models.DateTimeField(auto_now_add=True)),
                ("started_at", models.DateTimeField(null=True)),
                ("finished_at", models.DateTimeField(null=True)),
                ("args_kwargs", models.JSONField()),
                ("priority", models.IntegerField(default=0)),
                ("task_path", models.TextField()),
                ("worker_ids", models.JSONField(default=list)),
                (
                    "queue_name",
                    models.CharField(default="default", max_length=32),
                ),
                ("backend_name", models.CharField(max_length=32)),
                (
                    "run_after",
                    models.DateTimeField(
                        default=datetime.datetime(
                            9999, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
                        )
                    ),
                ),
                ("return_value", models.JSONField(default=None, null=True)),
                ("exception_class_path", models.TextField(default="")),
                ("traceback", models.TextField(default="")),
            ],
            options={
                "ordering": [
                    django.db.models.expressions.OrderBy(
                        django.db.models.expressions.F("priority"), descending=True
                    ),
                    django.db.models.expressions.OrderBy(
                        django.db.models.expressions.F("run_after")
                    ),
                ],
            },
        ),
        migrations.AddIndex(
            model_name="dbtaskresult",
            index=models.Index(
                "status",
                django.db.models.expressions.OrderBy(
                    django.db.models.expressions.F("priority"), descending=True
                ),
                django.db.models.expressions.OrderBy(
                    django.db.models.expressions.F("run_after")
                ),
                condition=models.Q(("status", "READY")),
                name="local_db_ready_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="dbtaskresult",
            index=models.Index(
                fields=["backend_name"], name="django_task_backend_b87c8f_idx"
            ),
        ),
        migrations.AddConstraint(
            model_name="dbtaskresult",
            constraint=models.CheckConstraint(
                condition=models.Q(
                    ("priority__gte", -100), ("priority__lte", 100)
                ),
                name="local_db_priority_range",
            ),
        ),
    ]
