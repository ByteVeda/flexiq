"""Withholding migrations from an application built during import.

``taskito migrate`` reaches its queue by importing the application module, which
constructs a :class:`~taskito.app.Queue` — and a queue that migrates on open
would apply the schema before the command ever runs, defeating the gate and
leaving the command's report describing work it did not do.

So the command sets this variable before the import. It only ever withholds
migrations: an application that already passes ``auto_migrate=False`` is
unaffected, and nothing here can turn migrations back on.
"""

from __future__ import annotations

import os

__all__ = ["MIGRATION_GATE_ENV", "migrations_withheld", "withhold_migrations"]

#: Set by ``taskito migrate`` before it imports the application.
MIGRATION_GATE_ENV = "FLEXIQ_WITHHOLD_MIGRATIONS"


def migrations_withheld() -> bool:
    """Whether a queue built now must open without applying schema changes."""
    return os.environ.get(MIGRATION_GATE_ENV) == "1"


def withhold_migrations() -> None:
    """Withhold migrations from every queue built after this call."""
    os.environ[MIGRATION_GATE_ENV] = "1"
