"""Management command to start the flexiq web dashboard."""

from __future__ import annotations

try:
    from django.core.management.base import BaseCommand
except ImportError as e:
    raise ImportError(
        "Django integration requires 'django'. Install with: pip install flexiq[django]"
    ) from e


class Command(BaseCommand):
    help = "Start the flexiq web dashboard"

    def add_arguments(self, parser):  # type: ignore[no-untyped-def]
        from django.conf import settings

        default_host = getattr(settings, "FLEXIQ_DASHBOARD_HOST", "127.0.0.1")
        default_port = getattr(settings, "FLEXIQ_DASHBOARD_PORT", 8080)
        default_auth = getattr(settings, "FLEXIQ_DASHBOARD_AUTH", False)

        parser.add_argument(
            "--host",
            default=default_host,
            help=f"Bind address (default: {default_host})",
        )
        parser.add_argument(
            "--port",
            type=int,
            default=default_port,
            help=f"Bind port (default: {default_port})",
        )
        parser.add_argument(
            "--auth",
            action="store_true",
            default=default_auth,
            help="Enable session authentication; off by default",
        )

    def handle(self, **options):  # type: ignore[no-untyped-def]
        from flexiq.contrib.django.settings import get_queue
        from flexiq.dashboard import serve_dashboard

        queue = get_queue()
        serve_dashboard(
            queue,
            host=options["host"],
            port=options["port"],
            auth_enabled=options["auth"],
        )
