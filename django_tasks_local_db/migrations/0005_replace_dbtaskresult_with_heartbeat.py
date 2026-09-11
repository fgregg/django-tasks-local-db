"""Switch to django-tasks-db's DBTaskResult table.

django_tasks_local_db used to carry its own copy of DBTaskResult. Now it
depends on django-tasks-db directly and only adds a TaskHeartbeat side
table. Existing rows are copied across so queued and in-flight tasks
survive the upgrade; the old table is then dropped.

Reversing this migration recreates the old table but does not copy rows
back.
"""

import datetime

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


def copy_results_forward(apps, schema_editor):
    old = apps.get_model("django_tasks_local_db", "DBTaskResult")._meta.db_table
    new = apps.get_model("django_tasks_database", "DBTaskResult")._meta.db_table
    heartbeat = apps.get_model("django_tasks_local_db", "TaskHeartbeat")._meta.db_table
    qn = schema_editor.connection.ops.quote_name

    columns = [
        "id",
        "status",
        "enqueued_at",
        "started_at",
        "finished_at",
        "args_kwargs",
        "priority",
        "task_path",
        "worker_ids",
        "queue_name",
        "backend_name",
        "return_value",
        "exception_class_path",
        "traceback",
    ]
    quoted = ", ".join(qn(c) for c in columns)

    # django-tasks-db stores "no run_after" as a far-future sentinel rather
    # than NULL (see django_tasks_db.models.get_date_max).
    date_max = datetime.datetime(
        9999, 1, 1, tzinfo=datetime.timezone.utc if settings.USE_TZ else None
    )

    with schema_editor.connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {qn(new)} ({quoted}, {qn('run_after')}) "
            f"SELECT {quoted}, COALESCE({qn('run_after')}, %s) FROM {qn(old)}",
            [date_max],
        )
        cursor.execute(
            f"INSERT INTO {qn(heartbeat)} ({qn('task_result_id')}, {qn('last_heartbeat_at')}) "
            f"SELECT {qn('id')}, {qn('last_heartbeat_at')} FROM {qn(old)} "
            f"WHERE {qn('last_heartbeat_at')} IS NOT NULL"
        )


class Migration(migrations.Migration):

    dependencies = [
        ('django_tasks_database', '0021_conditional_partial_index_ordering'),
        ('django_tasks_local_db', '0004_dbtaskresult_last_heartbeat_at_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='TaskHeartbeat',
            fields=[
                ('task_result', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, primary_key=True, related_name='heartbeat', serialize=False, to='django_tasks_database.dbtaskresult', verbose_name='task result')),
                ('last_heartbeat_at', models.DateTimeField(verbose_name='last heartbeat at')),
            ],
            options={
                'verbose_name': 'task heartbeat',
                'verbose_name_plural': 'task heartbeats',
            },
        ),
        migrations.RunPython(copy_results_forward, migrations.RunPython.noop),
        migrations.RemoveIndex(
            model_name='dbtaskresult',
            name='django_task_backend_0535b8_idx',
        ),
        migrations.RemoveIndex(
            model_name='dbtaskresult',
            name='django_task_queue_n_7c9efa_idx',
        ),
        migrations.RemoveIndex(
            model_name='dbtaskresult',
            name='local_db_ready_idx',
        ),
        migrations.RemoveConstraint(
            model_name='dbtaskresult',
            name='local_db_priority_range',
        ),
        migrations.DeleteModel(
            name='DBTaskResult',
        ),
    ]
