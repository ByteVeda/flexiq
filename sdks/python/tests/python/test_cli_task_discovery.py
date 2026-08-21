"""Loading an app by path claims the deferred declarations that import leaves.

``Queue.__init__`` was the only claim point on these paths, and it runs while
the app module is still executing — so a task module imported below it was
declared and never claimed. ``flexiq executor`` advertises the registry it
reads, and the scheduler routes a name only to executors that advertised it, so
the missing tasks' jobs park until ``placement_timeout`` with nothing to point
at. Each prefork child imports the app in its own interpreter and is the second
place the same omission bites.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from flexiq.cli import _build_parser, _load_queue, run_executor
from flexiq.detached import DETACHED_ENV
from flexiq.prefork.child import _import_queue

from .conftest import PackageWriter

# The order that hides the declarations: the constructor drains an empty
# registry, and the task module is imported after it.
APP = """
from flexiq import Queue

queue = Queue(db_path={db})


@queue.task()
def legacy_task():
    return "legacy"


import <PKG>.deferred  # noqa: E402,F401
"""

# The same, for an app whose tasks are all deferred: the registry ends up empty
# rather than short.
APP_WITHOUT_BOUND_TASKS = """
from flexiq import Queue

queue = Queue(db_path={db})

import <PKG>.deferred  # noqa: E402,F401
"""

DEFERRED = """
from flexiq import task


@task()
def send_invoice(user_id):
    return f"sent:{user_id}"


@task()
def build_report():
    return "report"
"""


def _write_app(write_package: PackageWriter, tmp_path: Path, app: str, db: str) -> str:
    """Write an app package whose task module is imported after the queue."""
    return write_package(
        {
            "__init__.py": "",
            # repr, not str: a Windows path would drop \U escapes into the source.
            "app.py": app.format(db=repr(str(tmp_path / db))),
            "deferred.py": DEFERRED,
        }
    )


def test_load_queue_claims_tasks_declared_after_the_queue(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """A task module imported below the ``Queue`` is on the returned queue."""
    pkg = _write_app(write_package, tmp_path, APP, "partial.db")

    queue = _load_queue(f"{pkg}.app:queue")

    assert sorted(queue._task_registry) == [
        f"{pkg}.app.legacy_task",
        f"{pkg}.deferred.build_report",
        f"{pkg}.deferred.send_invoice",
    ]


def test_load_queue_claims_when_the_queue_declares_nothing_itself(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """The louder half of the same bug: an app whose tasks are all deferred.

    ``flexiq executor`` refuses to attach on an empty registry, but the message
    blames the app for having no tasks when it has plenty.
    """
    pkg = _write_app(write_package, tmp_path, APP_WITHOUT_BOUND_TASKS, "empty.db")

    queue = _load_queue(f"{pkg}.app:queue")

    assert sorted(queue._task_registry) == [
        f"{pkg}.deferred.build_report",
        f"{pkg}.deferred.send_invoice",
    ]


def test_run_executor_advertises_the_deferred_tasks(
    write_package: PackageWriter,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The list handed to the executor is the one the scheduler routes on.

    Recorded at the constructor; the handshake itself is covered end to end in
    ``tests/worker/test_executor_attach.py``.
    """
    pkg = _write_app(write_package, tmp_path, APP, "advertised.db")
    advertised: list[list[str]] = []

    def _record(
        address: str,
        app_path: str,
        tasks: list[str],
        slots: int,
        token: str | None = None,
        executor_id: str | None = None,
    ) -> object:
        advertised.append(tasks)
        # What an unreachable scheduler raises, so the CLI exits here rather
        # than entering its poll loop.
        raise RuntimeError("could not reach the scheduler")

    monkeypatch.setattr("flexiq.cli._Executor", _record)
    # ``run_executor`` sets this itself; recorded here so it is unset again and
    # a later test's ``Queue`` still opens storage.
    monkeypatch.setenv(DETACHED_ENV, "0")

    args = _build_parser().parse_args(
        ["executor", "--app", f"{pkg}.app:queue", "--attach", "127.0.0.1:7777"]
    )
    with pytest.raises(SystemExit):
        run_executor(args)

    assert advertised == [
        [
            f"{pkg}.app.legacy_task",
            f"{pkg}.deferred.build_report",
            f"{pkg}.deferred.send_invoice",
        ]
    ]
    assert "could not reach the scheduler" in capsys.readouterr().err


def test_a_repeat_drain_after_loading_does_not_duplicate_the_task(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """Claiming stays idempotent, so an app that called ``autodiscover`` pays nothing."""
    pkg = _write_app(write_package, tmp_path, APP, "repeat.db")
    queue = _load_queue(f"{pkg}.app:queue")

    queue._drain_pending_tasks()

    names = [c.name for c in queue._task_configs]
    assert names.count(f"{pkg}.deferred.send_invoice") == 1


def test_a_prefork_child_claims_them_in_its_own_interpreter(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """The parent's drain says nothing about a child, which imports the app itself.

    A task missing from the child's registry fails its job as ``not
    registered``, and non-retryably — so it dead-letters rather than parking.
    """
    pkg = _write_app(write_package, tmp_path, APP, "child.db")

    queue = _import_queue(f"{pkg}.app:queue")

    assert f"{pkg}.deferred.send_invoice" in queue._task_registry
