"""Management command to show taskito queue statistics."""

from __future__ import annotations

import time

try:
    from django.core.management.base import BaseCommand
except ImportError as e:
    raise ImportError(
        "Django integration requires 'django'. Install with: pip install taskito[django]"
    ) from e


class Command(BaseCommand):
    help = "Show taskito queue statistics and registered tasks"

    def add_arguments(self, parser):  # type: ignore[no-untyped-def]
        parser.add_argument(
            "--watch",
            action="store_true",
            default=False,
            help="Continuously refresh stats every 2 seconds",
        )

    def handle(self, **options):  # type: ignore[no-untyped-def]
        from taskito.contrib.django.settings import get_queue

        queue = get_queue()

        if options["watch"]:
            self._watch(queue)
        else:
            self._print(queue)

    def _print(self, queue):  # type: ignore[no-untyped-def]
        stats = queue.stats()
        self.stdout.write("taskito queue statistics")
        self.stdout.write("-" * 30)
        for key in ("pending", "running", "completed", "failed", "dead", "cancelled"):
            self.stdout.write(f"  {key:<12} {stats.get(key, 0)}")
        total = sum(stats.values())
        self.stdout.write("-" * 30)
        self.stdout.write(f"  {'total':<12} {total}")

    def _watch(self, queue):  # type: ignore[no-untyped-def]
        from django.conf import settings

        interval = getattr(settings, "FLEXIQ_WATCH_INTERVAL", 2)

        prev_completed = 0
        try:
            while True:
                self.stdout.write("\033[2J\033[H", ending="")
                stats = queue.stats()
                completed = stats.get("completed", 0)
                throughput = (completed - prev_completed) / float(interval)
                prev_completed = completed

                self._print(queue)
                if throughput > 0:
                    self.stdout.write(f"\n  throughput   {throughput:.1f} jobs/s")
                self.stdout.write(f"\nRefreshing every {interval}s... (Ctrl+C to stop)")
                time.sleep(interval)
        except KeyboardInterrupt:
            pass
