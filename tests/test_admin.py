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
    def test_requeue_action_listed_for_superuser(self, admin_client, backend):
        result = add_numbers.enqueue(1, 2)
        _wait_for_result(result)
        response = admin_client.get("/admin/django_tasks_local_db/dbtaskresult/")
        assert response.status_code == 200
        assert 'value="requeue"' in response.content.decode()

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


class TestRequeueAction:
    """Tests for the 'Re-enqueue as a new task' admin action."""

    CHANGELIST_URL = "/admin/django_tasks_local_db/dbtaskresult/"

    @pytest.fixture
    def admin_client(self):
        user = User.objects.create_superuser("admin", "admin@test.com", "password")
        client = Client()
        client.force_login(user)
        return client

    def _post_requeue(self, client, db_ids):
        return client.post(
            self.CHANGELIST_URL,
            {"action": "requeue", "_selected_action": [str(i) for i in db_ids]},
            follow=True,
        )

    @staticmethod
    def _make_failed_row(task_path="tests.tasks.add_numbers", args=None):
        return DBTaskResult.objects.create(
            args_kwargs={"args": args if args is not None else [1, 2], "kwargs": {}},
            priority=0,
            task_path=task_path,
            queue_name="default",
            backend_name="default",
            exception_class_path="builtins.ValueError",
            traceback="Traceback: intentional failure",
            status=TaskResultStatus.FAILED,
            started_at=timezone.now(),
            finished_at=timezone.now(),
        )

    @pytest.mark.django_db(transaction=True)
    def test_requeue_failed_creates_new_task(self, admin_client, backend):
        """A FAILED row is re-enqueued as a fresh task; the original is untouched."""
        failed = self._make_failed_row()
        response = self._post_requeue(admin_client, [failed.id])
        assert response.status_code == 200
        assert "Re-enqueued" in response.content.decode()

        rows = DBTaskResult.objects.filter(task_path="tests.tasks.add_numbers")
        assert rows.count() == 2
        failed.refresh_from_db()
        assert failed.status == TaskResultStatus.FAILED  # original untouched

        new_row = rows.exclude(id=failed.id).get()
        assert new_row.args_kwargs == failed.args_kwargs
        assert new_row.priority == failed.priority
        assert new_row.queue_name == failed.queue_name

    @pytest.mark.django_db(transaction=True)
    def test_requeued_task_executes(self, admin_client, backend):
        """The re-enqueued task actually runs to completion."""
        failed = self._make_failed_row(args=[10, 20])
        self._post_requeue(admin_client, [failed.id])

        new_row = DBTaskResult.objects.filter(
            task_path="tests.tasks.add_numbers"
        ).exclude(id=failed.id).get()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            new_row.refresh_from_db()
            if new_row.status == TaskResultStatus.SUCCESSFUL:
                break
            time.sleep(0.05)
        assert new_row.status == TaskResultStatus.SUCCESSFUL
        assert new_row.return_value == 30

    @pytest.mark.django_db(transaction=True)
    def test_requeue_skips_non_failed(self, admin_client, backend):
        """Rows that are not FAILED are skipped with a warning."""
        result = add_numbers.enqueue(1, 2)
        _wait_for_result(result)
        db_row = DBTaskResult.objects.get(id=result.id)
        assert db_row.status == TaskResultStatus.SUCCESSFUL

        response = self._post_requeue(admin_client, [db_row.id])
        content = response.content.decode()
        assert "Skipped 1 task(s)" in content
        assert DBTaskResult.objects.count() == 1  # nothing new enqueued

    @pytest.mark.django_db(transaction=True)
    def test_requeue_unimportable_task_path_errors(self, admin_client, backend):
        """A FAILED row whose task_path no longer imports reports an error."""
        failed = self._make_failed_row(task_path="tests.tasks.does_not_exist")
        response = self._post_requeue(admin_client, [failed.id])
        content = response.content.decode()
        assert "Could not re-enqueue" in content
        assert DBTaskResult.objects.count() == 1

    @pytest.mark.django_db(transaction=True)
    def test_requeue_requires_change_permission(self, backend):
        """A user with only view permission does not get the requeue action."""
        from django.contrib.auth.models import Permission

        user = User.objects.create_user(
            "viewer", "viewer@test.com", "password", is_staff=True
        )
        user.user_permissions.add(
            Permission.objects.get(codename="view_dbtaskresult")
        )
        client = Client()
        client.force_login(user)

        failed = self._make_failed_row()
        response = client.get(self.CHANGELIST_URL)
        assert response.status_code == 200
        assert 'value="requeue"' not in response.content.decode()

        # And posting the action anyway must not enqueue anything.
        client.post(
            self.CHANGELIST_URL,
            {"action": "requeue", "_selected_action": [str(failed.id)]},
            follow=True,
        )
        assert DBTaskResult.objects.count() == 1
