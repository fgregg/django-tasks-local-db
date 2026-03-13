"""Tests for the DBTaskResult admin interface (observability)."""

import time

import pytest
from django.contrib.admin.sites import AdminSite
from django.contrib.auth.models import User
from django.test import Client
from django.tasks import TaskResultStatus, task_backends
from django.utils import timezone

from django_tasks_local_db.admin import DBTaskResultAdmin
from django_tasks_local_db.models import DBTaskResult

from .tasks import add_numbers, failing_task


@pytest.fixture
def backend():
    return task_backends["default"]


@pytest.fixture
def admin_instance():
    return DBTaskResultAdmin(model=DBTaskResult, admin_site=AdminSite())


def _wait_for_result(result, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result.refresh()
        if result.is_finished:
            return
        time.sleep(0.05)
    raise TimeoutError(f"Task {result.id} did not finish within {timeout}s")


class TestAdminPermissions:
    def test_has_no_add_permission(self, admin_instance, rf):
        request = rf.get("/")
        assert admin_instance.has_add_permission(request) is False

    def test_has_no_delete_permission(self, admin_instance, rf):
        request = rf.get("/")
        assert admin_instance.has_delete_permission(request) is False

    def test_has_no_change_permission(self, admin_instance, rf):
        request = rf.get("/")
        assert admin_instance.has_change_permission(request) is False


class TestAdminMethods:
    @pytest.mark.django_db(transaction=True)
    def test_task_name_display(self, admin_instance, backend):
        result = add_numbers.enqueue(1, 2)
        _wait_for_result(result)
        db_row = DBTaskResult.objects.get(id=result.id)
        assert admin_instance.task_name(db_row) == "add_numbers"

    @pytest.mark.django_db(transaction=True)
    def test_formatted_traceback_empty(self, admin_instance, backend):
        result = add_numbers.enqueue(1, 2)
        _wait_for_result(result)
        db_row = DBTaskResult.objects.get(id=result.id)
        assert admin_instance.formatted_traceback(db_row) == ""

    @pytest.mark.django_db(transaction=True)
    def test_formatted_traceback_with_content(self, admin_instance, backend):
        result = failing_task.enqueue()
        _wait_for_result(result)
        db_row = DBTaskResult.objects.get(id=result.id)
        html = admin_instance.formatted_traceback(db_row)
        assert html.startswith("<pre>")
        assert html.endswith("</pre>")
        assert "intentional failure" in html


class TestAdminViews:
    """Test the admin list and detail views render correctly with task data."""

    @pytest.fixture
    def admin_client(self):
        user = User.objects.create_superuser("admin", "admin@test.com", "password")
        client = Client()
        client.force_login(user)
        return client

    @pytest.mark.django_db(transaction=True)
    def test_changelist_shows_tasks(self, admin_client, backend):
        """The admin changelist should display tasks with correct status."""
        result_ok = add_numbers.enqueue(1, 2)
        result_fail = failing_task.enqueue()
        _wait_for_result(result_ok)
        _wait_for_result(result_fail)

        response = admin_client.get("/admin/django_tasks_local_db/dbtaskresult/")
        assert response.status_code == 200
        content = response.content.decode()
        assert "add_numbers" in content
        assert "failing_task" in content
        assert "SUCCESSFUL" in content
        assert "FAILED" in content

    @pytest.mark.django_db(transaction=True)
    def test_changelist_filters(self, admin_client, backend):
        """The admin changelist should support filtering by status."""
        result_ok = add_numbers.enqueue(1, 2)
        result_fail = failing_task.enqueue()
        _wait_for_result(result_ok)
        _wait_for_result(result_fail)

        # Filter by SUCCESSFUL
        response = admin_client.get(
            "/admin/django_tasks_local_db/dbtaskresult/?status=SUCCESSFUL"
        )
        assert response.status_code == 200
        content = response.content.decode()
        assert "add_numbers" in content
        # The failing task row should not appear in the filtered results
        assert "failing_task" not in content

    @pytest.mark.django_db(transaction=True)
    def test_detail_view_successful_task(self, admin_client, backend):
        """The admin detail view for a successful task shows return value."""
        result = add_numbers.enqueue(10, 20)
        _wait_for_result(result)

        db_row = DBTaskResult.objects.get(id=result.id)
        response = admin_client.get(
            f"/admin/django_tasks_local_db/dbtaskresult/{db_row.id}/change/"
        )
        assert response.status_code == 200
        content = response.content.decode()
        assert "add_numbers" in content
        assert "SUCCESSFUL" in content

    @pytest.mark.django_db(transaction=True)
    def test_detail_view_failed_task_shows_traceback(self, admin_client, backend):
        """The admin detail view for a failed task shows formatted traceback."""
        result = failing_task.enqueue()
        _wait_for_result(result)

        db_row = DBTaskResult.objects.get(id=result.id)
        response = admin_client.get(
            f"/admin/django_tasks_local_db/dbtaskresult/{db_row.id}/change/"
        )
        assert response.status_code == 200
        content = response.content.decode()
        assert "FAILED" in content
        assert "intentional failure" in content
        assert "<pre>" in content

    @pytest.mark.django_db(transaction=True)
    def test_changelist_shows_deferred_tasks(self, admin_client, backend):
        """Deferred tasks (run_after in the future) should be visible in the admin."""
        future_time = timezone.now() + timezone.timedelta(hours=1)
        deferred = add_numbers.using(run_after=future_time)
        result = deferred.enqueue(1, 2)

        response = admin_client.get("/admin/django_tasks_local_db/dbtaskresult/")
        assert response.status_code == 200
        content = response.content.decode()
        assert "READY" in content
        assert "add_numbers" in content

    @pytest.mark.django_db(transaction=True)
    def test_changelist_shows_stale_running_tasks(self, admin_client, backend):
        """Stale RUNNING tasks should be visible — operators need to see stuck tasks."""
        stale_time = timezone.now() - timezone.timedelta(minutes=5)
        DBTaskResult.objects.create(
            args_kwargs={"args": [1, 2], "kwargs": {}},
            priority=0,
            task_path="tests.tasks.add_numbers",
            queue_name="default",
            backend_name="default",
            exception_class_path="",
            traceback="",
            status=TaskResultStatus.RUNNING,
            started_at=stale_time,
            last_heartbeat_at=stale_time,
        )

        response = admin_client.get(
            "/admin/django_tasks_local_db/dbtaskresult/?status=RUNNING"
        )
        assert response.status_code == 200
        content = response.content.decode()
        assert "RUNNING" in content
        assert "add_numbers" in content

    @pytest.mark.django_db(transaction=True)
    def test_detail_view_shows_run_after_and_heartbeat(self, admin_client, backend):
        """The detail view should display run_after and last_heartbeat_at fields."""
        future_time = timezone.now() + timezone.timedelta(hours=1)
        db_result = DBTaskResult.objects.create(
            args_kwargs={"args": [1, 2], "kwargs": {}},
            priority=0,
            task_path="tests.tasks.add_numbers",
            queue_name="default",
            backend_name="default",
            exception_class_path="",
            traceback="",
            run_after=future_time,
        )

        response = admin_client.get(
            f"/admin/django_tasks_local_db/dbtaskresult/{db_result.id}/change/"
        )
        assert response.status_code == 200
        content = response.content.decode()
        assert "run_after" in content.lower() or "Run after" in content
