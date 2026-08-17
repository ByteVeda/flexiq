"""Django admin views for flexiq queue inspection.

Registers custom admin views for browsing jobs, dead letters, and queue stats.
Uses flexiq's Python API directly — no Django ORM models needed.
"""

from __future__ import annotations

from typing import Any

try:
    from django.contrib import admin
    from django.http import HttpRequest, HttpResponse
    from django.template.response import TemplateResponse
    from django.urls import path, reverse
except ImportError as e:
    raise ImportError(
        "Django integration requires 'django'. Install with: pip install flexiq[django]"
    ) from e


def _base_context(request: HttpRequest, site: Any, **extra: Any) -> dict[str, Any]:
    """Build the shared template context for every flexiq admin view.

    Resolves the nav URLs against the *current* admin site's namespace
    (``site.name``) so the templates work on both the default admin site and a
    custom :class:`FlexiQAdminSite`. ``job_detail_name`` is passed as a view
    name (not a resolved URL) so templates can reverse it per-row with the job
    id via ``{% url %}``.
    """
    namespace = site.name
    context: dict[str, Any] = {
        **site.each_context(request),
        "flexiq_urls": {
            "dashboard": reverse(f"{namespace}:flexiq_dashboard"),
            "jobs": reverse(f"{namespace}:flexiq_jobs"),
            "dead_letters": reverse(f"{namespace}:flexiq_dead_letters"),
            "job_detail_name": f"{namespace}:flexiq_job_detail",
        },
    }
    context.update(extra)
    return context


def _dashboard_view(request: HttpRequest, site: Any) -> HttpResponse:
    from flexiq.contrib.django.settings import get_queue

    queue = get_queue()
    context = _base_context(request, site, stats=queue.stats(), title="FlexiQ Dashboard")
    return TemplateResponse(request, "flexiq/admin/dashboard.html", context)


def _jobs_view(request: HttpRequest, site: Any) -> HttpResponse:
    from flexiq.contrib.django.settings import get_queue

    queue = get_queue()
    status = request.GET.get("status")
    queue_name = request.GET.get("queue")
    task_name = request.GET.get("task_name")
    try:
        page = int(request.GET.get("page", "1"))
    except (ValueError, TypeError):
        page = 1
    page = max(page, 1)
    from django.conf import settings as django_settings

    per_page = getattr(django_settings, "FLEXIQ_ADMIN_PER_PAGE", 50)

    try:
        jobs = queue.list_jobs(
            status=status,
            queue=queue_name,
            task_name=task_name,
            limit=per_page,
            offset=(page - 1) * per_page,
        )
    except Exception:
        import logging

        logging.getLogger(__name__).exception("Failed to list jobs")
        jobs = []
    context = _base_context(
        request,
        site,
        jobs=[j.to_dict() for j in jobs],
        filters={"status": status, "queue": queue_name, "task_name": task_name},
        page=page,
        has_next=len(jobs) == per_page,
        title="FlexiQ Jobs",
    )
    return TemplateResponse(request, "flexiq/admin/jobs.html", context)


def _job_detail_view(request: HttpRequest, site: Any, job_id: str) -> HttpResponse:
    from flexiq.contrib.django.settings import get_queue

    queue = get_queue()
    job = queue.get_job(job_id)
    errors = queue.job_errors(job_id) if job else []
    context = _base_context(
        request,
        site,
        job=job.to_dict() if job else None,
        errors=errors,
        title=f"Job {job_id}",
    )
    return TemplateResponse(request, "flexiq/admin/job_detail.html", context)


def _dead_letters_view(request: HttpRequest, site: Any) -> HttpResponse:
    from flexiq.contrib.django.settings import get_queue

    queue = get_queue()

    if request.method == "POST":
        action = request.POST.get("action")
        dead_id = request.POST.get("dead_id")
        if action == "retry" and dead_id:
            queue.retry_dead(dead_id)

    try:
        page = int(request.GET.get("page", "1"))
    except (ValueError, TypeError):
        page = 1
    page = max(page, 1)
    from django.conf import settings as django_settings

    per_page = getattr(django_settings, "FLEXIQ_ADMIN_PER_PAGE", 50)
    dead = queue.dead_letters(limit=per_page, offset=(page - 1) * per_page)
    context = _base_context(
        request,
        site,
        dead_letters=dead,
        page=page,
        has_next=len(dead) == per_page,
        title="FlexiQ Dead Letters",
    )
    return TemplateResponse(request, "flexiq/admin/dead_letters.html", context)


def _get_admin_setting(name: str, default: str) -> str:
    from django.conf import settings as django_settings

    return str(getattr(django_settings, name, default))


class FlexiQAdminSite(admin.AdminSite):
    """Custom admin site with flexiq queue views.

    Reads ``FLEXIQ_ADMIN_TITLE`` and ``FLEXIQ_ADMIN_HEADER`` from Django
    settings to customize the admin site branding.
    """

    @property
    def site_header(self) -> str:
        return _get_admin_setting("FLEXIQ_ADMIN_HEADER", "FlexiQ Admin")

    @property
    def site_title(self) -> str:
        return _get_admin_setting("FLEXIQ_ADMIN_TITLE", "FlexiQ")

    def get_urls(self) -> list:
        urls = super().get_urls()
        custom = [
            path("flexiq/", self.admin_view(self.dashboard_view), name="flexiq_dashboard"),
            path("flexiq/jobs/", self.admin_view(self.jobs_view), name="flexiq_jobs"),
            path(
                "flexiq/jobs/<str:job_id>/",
                self.admin_view(self.job_detail_view),
                name="flexiq_job_detail",
            ),
            path(
                "flexiq/dead-letters/",
                self.admin_view(self.dead_letters_view),
                name="flexiq_dead_letters",
            ),
        ]
        return custom + urls  # type: ignore[no-any-return]

    def dashboard_view(self, request: HttpRequest) -> HttpResponse:
        return _dashboard_view(request, self)

    def jobs_view(self, request: HttpRequest) -> HttpResponse:
        return _jobs_view(request, self)

    def job_detail_view(self, request: HttpRequest, job_id: str) -> HttpResponse:
        return _job_detail_view(request, self, job_id)

    def dead_letters_view(self, request: HttpRequest) -> HttpResponse:
        return _dead_letters_view(request, self)


def register_flexiq_admin(site: Any = None) -> None:
    """Register flexiq views on an existing admin site.

    Call this in your project's ``admin.py`` or ``urls.py``::

        from flexiq.contrib.django.admin import register_flexiq_admin
        register_flexiq_admin()
    """
    target = site or admin.site

    def dashboard_view(request: HttpRequest) -> HttpResponse:
        return _dashboard_view(request, target)

    def jobs_view(request: HttpRequest) -> HttpResponse:
        return _jobs_view(request, target)

    def job_detail_view(request: HttpRequest, job_id: str) -> HttpResponse:
        return _job_detail_view(request, target, job_id)

    def dead_letters_view(request: HttpRequest) -> HttpResponse:
        return _dead_letters_view(request, target)

    original_get_urls = target.get_urls

    def patched_get_urls() -> list:
        urls = original_get_urls()
        custom = [
            path("flexiq/", target.admin_view(dashboard_view), name="flexiq_dashboard"),
            path("flexiq/jobs/", target.admin_view(jobs_view), name="flexiq_jobs"),
            path(
                "flexiq/jobs/<str:job_id>/",
                target.admin_view(job_detail_view),
                name="flexiq_job_detail",
            ),
            path(
                "flexiq/dead-letters/",
                target.admin_view(dead_letters_view),
                name="flexiq_dead_letters",
            ),
        ]
        return custom + urls  # type: ignore[no-any-return]

    target.get_urls = patched_get_urls
