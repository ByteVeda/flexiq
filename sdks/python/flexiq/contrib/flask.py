"""Flask integration for flexiq.

Requires the ``flask`` extra::

    pip install flexiq[flask]

Usage::

    from flask import Flask
    from flexiq.contrib.flask import FlexiQ

    app = Flask(__name__)
    app.config["FLEXIQ_DB_PATH"] = ".flexiq/flexiq.db"
    flexiq = FlexiQ(app)

    # or with the factory pattern:
    flexiq = FlexiQ()
    flexiq.init_app(app)

    # Access the queue:
    flexiq.queue  # or app.extensions["flexiq"].queue
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from flexiq.app import Queue

if TYPE_CHECKING:
    import flask


class FlexiQ:
    """Flask extension that provides a configured :class:`~flexiq.app.Queue`.

    Reads configuration from ``app.config``:

    - ``FLEXIQ_DB_PATH`` — SQLite database path (default: ``.flexiq/flexiq.db``)
    - ``FLEXIQ_BACKEND`` — ``"sqlite"`` or ``"postgres"`` (default: ``"sqlite"``)
    - ``FLEXIQ_DB_URL`` — PostgreSQL connection URL
    - ``FLEXIQ_WORKERS`` — Number of worker threads (default: 0 = auto)
    - ``FLEXIQ_SCHEMA`` — PostgreSQL schema name (default: ``"flexiq"``)
    - ``FLEXIQ_DEFAULT_RETRY`` — Default retry count (default: 3)
    - ``FLEXIQ_DEFAULT_TIMEOUT`` — Default timeout in seconds (default: 300)
    - ``FLEXIQ_DEFAULT_PRIORITY`` — Default priority (default: 0)
    - ``FLEXIQ_RESULT_TTL`` — Result TTL in seconds (default: None)
    - ``FLEXIQ_DRAIN_TIMEOUT`` — Drain timeout in seconds (default: 30)

    Args:
        app: Optional Flask application instance.
        cli_group: Name for the CLI command group (default ``"flexiq"``).
    """

    def __init__(self, app: flask.Flask | None = None, cli_group: str = "flexiq"):
        self.queue: Any = None
        self._cli_group = cli_group
        if app is not None:
            self.init_app(app)

    def init_app(self, app: flask.Flask) -> None:
        """Initialize the extension with a Flask app."""
        self.queue = Queue(
            db_path=app.config.get("FLEXIQ_DB_PATH", ".flexiq/flexiq.db"),
            workers=app.config.get("FLEXIQ_WORKERS", 0),
            default_retry=app.config.get("FLEXIQ_DEFAULT_RETRY", 3),
            default_timeout=app.config.get("FLEXIQ_DEFAULT_TIMEOUT", 300),
            default_priority=app.config.get("FLEXIQ_DEFAULT_PRIORITY", 0),
            result_ttl=app.config.get("FLEXIQ_RESULT_TTL", None),
            backend=app.config.get("FLEXIQ_BACKEND", "sqlite"),
            db_url=app.config.get("FLEXIQ_DB_URL", None),
            schema=app.config.get("FLEXIQ_SCHEMA", "flexiq"),
            drain_timeout=app.config.get("FLEXIQ_DRAIN_TIMEOUT", 30),
        )

        app.extensions["flexiq"] = self

        self._register_cli(app)

    def _register_cli(self, app: flask.Flask) -> None:
        """Register Flask CLI commands."""
        import click

        @app.cli.group(self._cli_group)
        def flexiq_cli() -> None:
            """FlexiQ task queue commands."""

        @flexiq_cli.command("worker")
        @click.option("--queues", default=None, help="Comma-separated queue names")
        def worker_cmd(queues: str | None) -> None:
            """Start a flexiq worker."""
            queue_list = queues.split(",") if queues else None
            self.queue.run_worker(queues=queue_list)

        @flexiq_cli.command("info")
        @click.option(
            "--format",
            "output_format",
            type=click.Choice(["table", "json"]),
            default="table",
            help="Output format (default: table)",
        )
        def info_cmd(output_format: str) -> None:
            """Show queue statistics."""
            stats = self.queue.stats()
            if output_format == "json":
                click.echo(json.dumps(stats, indent=2))
            else:
                click.echo("flexiq queue statistics")
                click.echo("-" * 30)
                for key in ("pending", "running", "completed", "failed", "dead", "cancelled"):
                    click.echo(f"  {key:<12} {stats.get(key, 0)}")
                total = sum(stats.values())
                click.echo("-" * 30)
                click.echo(f"  {'total':<12} {total}")
