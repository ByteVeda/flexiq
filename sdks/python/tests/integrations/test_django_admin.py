"""Tests for the Django admin integration (issue #261).

Regression coverage: the admin views render four templates via
``TemplateResponse``; before this fix those templates were never shipped, so
every admin page raised ``TemplateDoesNotExist``. These tests render each view
end-to-end and assert a 200 with real content, which fails loudly if a template
(or its packaging) goes missing again.

Views are exercised directly (not through ``admin_view``) so no auth database or
migrations are needed — ``each_context`` short-circuits for an anonymous user.
"""

from __future__ import annotations

import pytest

# Skip the whole module when Django isn't installed (it's an optional extra).
pytest.importorskip("django")

from django.conf import settings

if not settings.configured:
    settings.configure(
        DEBUG=True,
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
            "django.contrib.auth",
            "django.contrib.messages",
            "django.contrib.sessions",
            "django.contrib.admin",
            "flexiq.contrib.django",
        ],
        TEMPLATES=[
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
            }
        ],
        ROOT_URLCONF=__name__,
        MIDDLEWARE=[],
        SECRET_KEY="test-only",
        USE_TZ=True,
    )
    import django

    django.setup()

from collections.abc import Callable

from django.contrib import admin
from django.contrib.auth.models import AnonymousUser
from django.test import RequestFactory
from django.urls import path, reverse

import flexiq.contrib.django.settings as tk_settings
from flexiq import Queue
from flexiq.contrib.django import admin as tk_admin

# Wire the four flexiq routes onto the default admin site so reverse() resolves
# them, then expose that site as this module's URLconf (ROOT_URLCONF above).
tk_admin.register_flexiq_admin()
urlpatterns = [path("admin/", admin.site.urls)]


@pytest.fixture
def rf() -> RequestFactory:
    return RequestFactory()


@pytest.fixture
def patched_queue(queue: Queue, monkeypatch: pytest.MonkeyPatch) -> Queue:
    """Point the Django integration's singleton accessor at the temp queue."""
    monkeypatch.setattr(tk_settings, "get_queue", lambda: queue)
    return queue


def _render(view: Callable[..., object], request: object, *args: object) -> object:
    request.user = AnonymousUser()  # type: ignore[attr-defined]
    response = view(request, admin.site, *args)
    response.render()  # type: ignore[attr-defined]
    return response


def test_dashboard_renders(rf: RequestFactory, patched_queue: Queue) -> None:
    response = _render(tk_admin._dashboard_view, rf.get("/admin/flexiq/"))
    assert response.status_code == 200  # type: ignore[attr-defined]
    assert b"FlexiQ Dashboard" in response.content  # type: ignore[attr-defined]


def test_jobs_view_lists_enqueued_job(rf: RequestFactory, patched_queue: Queue) -> None:
    @patched_queue.task()
    def add(x: int, y: int) -> int:
        return x + y

    job_id = add.delay(1, 2).id

    response = _render(tk_admin._jobs_view, rf.get("/admin/flexiq/jobs/"))
    assert response.status_code == 200  # type: ignore[attr-defined]
    assert job_id.encode() in response.content  # type: ignore[attr-defined]


def test_job_detail_renders(rf: RequestFactory, patched_queue: Queue) -> None:
    @patched_queue.task()
    def noop() -> None:
        return None

    job_id = noop.delay().id

    response = _render(tk_admin._job_detail_view, rf.get(f"/admin/flexiq/jobs/{job_id}/"), job_id)
    assert response.status_code == 200  # type: ignore[attr-defined]
    assert job_id.encode() in response.content  # type: ignore[attr-defined]


def test_dead_letters_empty_renders(rf: RequestFactory, patched_queue: Queue) -> None:
    response = _render(tk_admin._dead_letters_view, rf.get("/admin/flexiq/dead-letters/"))
    assert response.status_code == 200  # type: ignore[attr-defined]
    assert b"No dead letters" in response.content  # type: ignore[attr-defined]


def test_dead_letters_retry_posts_to_queue(
    rf: RequestFactory, patched_queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    retried: list[str] = []

    def record_retry(dead_id: str) -> str:
        retried.append(dead_id)
        return "new"

    monkeypatch.setattr(patched_queue, "retry_dead", record_retry)

    request = rf.post("/admin/flexiq/dead-letters/", {"action": "retry", "dead_id": "abc123"})
    _render(tk_admin._dead_letters_view, request)
    assert retried == ["abc123"]


def test_register_flexiq_admin_wires_all_routes() -> None:
    # Parity with FlexiQAdminSite — all four named routes must resolve.
    assert reverse("admin:flexiq_dashboard")
    assert reverse("admin:flexiq_jobs")
    assert reverse("admin:flexiq_dead_letters")
    assert reverse("admin:flexiq_job_detail", args=["xyz"])
