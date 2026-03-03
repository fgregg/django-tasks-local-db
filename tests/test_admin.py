"""Tests for the DBTaskResult admin interface."""

import time

import pytest
from django.contrib.admin.sites import AdminSite
from django.tasks import TaskResultStatus, task_backends

from django_tasks_local_db.admin import DBTaskResultAdmin
from django_tasks_local_db.models import DBTaskResult, _get_date_max

from .tasks import add_numbers, failing_task


@pytest.fixture
def backend():
    return task_backends["default"]


@pytest.fixture
def admin_instance():
    return DBTaskResultAdmin(model=DBTaskResult, admin_site=AdminSite())


def _wait_for_result(result, timeout=5):
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
