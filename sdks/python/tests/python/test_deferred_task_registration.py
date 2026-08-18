"""``@flexiq.task`` registers without a Queue; ``autodiscover`` drains it into one.

The regression that matters is the first one: a task module that never imports
the module constructing the ``Queue``. Under ``@queue.task()`` that import is
mandatory, and the queue module importing the task modules back is the cycle
this feature exists to remove.
"""

from __future__ import annotations

import importlib
import sys
import threading
import uuid
from collections.abc import Callable, Generator
from pathlib import Path

import pytest

from flexiq import DuplicateTaskError, Queue, TaskNotBoundError, registry

PackageWriter = Callable[[dict[str, str]], str]


@pytest.fixture(autouse=True)
def _isolate_pending_registry() -> Generator[None]:
    """Snapshot and restore the module-global pending registry.

    The registry is a process global by design — that is what lets a task
    module register without importing the module that builds the ``Queue``.
    Tests have to put it back, or a stray ``@task`` from one drains into every
    ``Queue`` built by the next.
    """
    snapshot = dict(registry._PENDING)
    try:
        yield
    finally:
        registry._PENDING.clear()
        registry._PENDING.update(snapshot)


@pytest.fixture
def write_package(tmp_path: Path) -> Generator[PackageWriter]:
    """Write an importable package tree into ``tmp_path`` and return its name.

    The package name is unique per call so a second test writing the same
    module layout is not served the first one out of ``sys.modules``.
    """
    created: list[str] = []
    sys.path.insert(0, str(tmp_path))

    def _write(modules: dict[str, str]) -> str:
        pkg = f"fq_discovery_{uuid.uuid4().hex[:8]}"
        created.append(pkg)
        for rel, source in modules.items():
            path = tmp_path / pkg / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source.replace("<PKG>", pkg))
        return pkg

    try:
        yield _write
    finally:
        sys.path.remove(str(tmp_path))
        for pkg in created:
            for name in [m for m in sys.modules if m == pkg or m.startswith(f"{pkg}.")]:
                del sys.modules[name]


APP = """
from flexiq import DuplicateTaskError, Queue, TaskNotBoundError

queue = Queue(db_path="{db}", workers=2)
queue.autodiscover("<PKG>.tasks")
"""

INVOICES = """
from flexiq import task


@task()
def send_invoice(user_id):
    return f"sent:{user_id}"
"""


def test_task_module_registers_without_importing_the_queue_module(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """The whole point: tasks/ imports flexiq, never the module holding the Queue."""
    pkg = write_package(
        {
            "__init__.py": "",
            "app.py": APP.format(db=tmp_path / "circular.db"),
            "tasks/__init__.py": "",
            "tasks/invoices.py": INVOICES,
        }
    )

    app = importlib.import_module(f"{pkg}.app")
    invoices = importlib.import_module(f"{pkg}.tasks.invoices")

    assert f"{pkg}.tasks.invoices.send_invoice" in app.queue._task_registry

    job = invoices.send_invoice.delay(7)
    threading.Thread(target=app.queue.run_worker, daemon=True).start()
    try:
        assert job.result(timeout=10) == "sent:7"
    finally:
        app.queue.shutdown()


def test_autodiscover_walks_nested_subpackages(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """Every module under the package is imported, not just the top level."""
    pkg = write_package(
        {
            "__init__.py": "",
            "tasks/__init__.py": "",
            "tasks/billing/__init__.py": "",
            "tasks/billing/invoices.py": INVOICES,
        }
    )

    queue = Queue(db_path=str(tmp_path / "nested.db"))
    names = queue.autodiscover(f"{pkg}.tasks")

    assert f"{pkg}.tasks.billing.invoices.send_invoice" in names


def test_autodiscover_reraises_a_broken_task_module(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """A module that fails to import is fatal — pkgutil swallows it by default.

    A swallowed import is the "worker discovered 11 of 12 modules" failure: the
    dispatcher treats an unregistered task as fatal and non-retryable, so the
    twelfth module's jobs dead-letter silently.
    """
    pkg = write_package(
        {
            "__init__.py": "",
            "tasks/__init__.py": "",
            "tasks/broken.py": "raise RuntimeError('boom')\n",
        }
    )

    queue = Queue(db_path=str(tmp_path / "broken.db"))
    with pytest.raises(RuntimeError, match="boom"):
        queue.autodiscover(f"{pkg}.tasks")


DUP_A = """
from flexiq import task


@task(name="reports.build")
def build_a():
    return "a"
"""

DUP_B = """
from flexiq import task


@task(name="reports.build")
def build_b():
    return "b"
"""


def test_two_modules_claiming_one_name_raise_at_import(
    write_package: PackageWriter,
) -> None:
    """A name collision is a conflict, not a silent overwrite.

    The loser would keep accepting submissions that dispatch to the winner's
    function, which is the failure mode an implicit registry makes possible.
    """
    pkg = write_package(
        {
            "__init__.py": "",
            "a.py": DUP_A,
            "b.py": DUP_B,
        }
    )

    importlib.import_module(f"{pkg}.a")
    with pytest.raises(DuplicateTaskError, match=r"reports\.build"):
        importlib.import_module(f"{pkg}.b")


def test_deferred_task_colliding_with_a_bound_task_raises_at_drain(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """``@queue.task(name=...)`` already owning the name is a conflict too.

    The queue is built first on purpose: constructing it drains whatever is
    already pending, so a deferred task has to arrive afterwards to reach the
    drain-time collision rather than being claimed and then overwritten.
    """
    pkg = write_package({"__init__.py": "", "a.py": DUP_A})
    queue = Queue(db_path=str(tmp_path / "collide.db"))

    @queue.task(name="reports.build")
    def build_here() -> str:
        return "here"

    importlib.import_module(f"{pkg}.a")

    with pytest.raises(DuplicateTaskError, match=r"reports\.build"):
        queue.autodiscover(pkg)


def test_reimporting_the_same_module_replaces_rather_than_conflicts(
    write_package: PackageWriter,
) -> None:
    """``importlib.reload`` re-runs the decorator — same origin, not a collision."""
    pkg = write_package({"__init__.py": "", "a.py": DUP_A})
    module = importlib.import_module(f"{pkg}.a")

    importlib.reload(module)  # must not raise


OPTIONS = """
from flexiq import task


@task(rate_limit="10/s", max_retries=7, timeout=42, priority=3, queue="reports")
def build_report(day):
    return day
"""


def test_options_survive_the_deferral(write_package: PackageWriter, tmp_path: Path) -> None:
    """Options recorded at decoration are replayed into Queue.task() verbatim."""
    pkg = write_package({"__init__.py": "", "reports.py": OPTIONS})
    queue = Queue(db_path=str(tmp_path / "options.db"))
    queue.autodiscover(pkg)

    name = f"{pkg}.reports.build_report"
    config = next(c for c in queue._task_configs if c.name == name)
    assert config.rate_limit == "10/s"
    assert config.max_retries == 7
    assert config.timeout == 42
    assert config.priority == 3
    assert config.queue == "reports"

    handle = importlib.import_module(f"{pkg}.reports").build_report
    assert handle.default_max_retries == 7


def test_unknown_option_raises_at_decoration(write_package: PackageWriter) -> None:
    """A typo surfaces with the decorator in the traceback, not at drain time."""
    pkg = write_package(
        {
            "__init__.py": "",
            "typo.py": "from flexiq import task\n\n@task(retires=3)\ndef f():\n    pass\n",
        }
    )

    with pytest.raises(TypeError, match="retires"):
        importlib.import_module(f"{pkg}.typo")


def test_task_requires_parentheses(write_package: PackageWriter) -> None:
    """Bare ``@task`` gets a message naming the fix, not an arity error."""
    pkg = write_package(
        {
            "__init__.py": "",
            "bare.py": "from flexiq import task\n\n@task\ndef f():\n    pass\n",
        }
    )

    with pytest.raises(TypeError, match="requires parentheses"):
        importlib.import_module(f"{pkg}.bare")


def test_a_second_queue_gets_the_same_tasks_and_takes_the_binding(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """Draining is idempotent, and the most recent drain owns submissions."""
    pkg = write_package({"__init__.py": "", "tasks.py": INVOICES})
    handle = importlib.import_module(f"{pkg}.tasks").send_invoice

    first = Queue(db_path=str(tmp_path / "first.db"))
    first.autodiscover(pkg)
    second = Queue(db_path=str(tmp_path / "second.db"))
    second.autodiscover(pkg)

    name = f"{pkg}.tasks.send_invoice"
    assert name in first._task_registry
    assert name in second._task_registry
    assert handle._queue is second


def test_redraining_the_same_queue_rebinds_and_does_not_duplicate(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """A repeat autodiscover is a no-op, and wins the binding back."""
    pkg = write_package({"__init__.py": "", "tasks.py": INVOICES})
    handle = importlib.import_module(f"{pkg}.tasks").send_invoice

    first = Queue(db_path=str(tmp_path / "first.db"))
    first.autodiscover(pkg)
    second = Queue(db_path=str(tmp_path / "second.db"))
    second.autodiscover(pkg)

    name = f"{pkg}.tasks.send_invoice"
    assert first.autodiscover(pkg) == [name]
    assert [c.name for c in first._task_configs].count(name) == 1
    assert handle._queue is first


def test_a_queue_built_after_the_import_claims_the_task(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """Importing task modules first needs no autodiscover call at all."""
    pkg = write_package({"__init__.py": "", "tasks.py": INVOICES})
    handle = importlib.import_module(f"{pkg}.tasks").send_invoice

    queue = Queue(db_path=str(tmp_path / "after.db"))

    assert f"{pkg}.tasks.send_invoice" in queue._task_registry
    assert handle._queue is queue


def test_submitting_before_any_queue_drained_raises(
    write_package: PackageWriter,
) -> None:
    """An unbound handle names the fix instead of failing on a NoneType."""
    pkg = write_package({"__init__.py": "", "tasks.py": INVOICES})
    handle = importlib.import_module(f"{pkg}.tasks").send_invoice

    assert not handle.bound
    with pytest.raises(TaskNotBoundError, match="autodiscover"):
        handle.delay(1)


def test_an_unbound_task_still_calls_through_to_the_function(
    write_package: PackageWriter,
) -> None:
    """The decorated function stays directly callable, queue or no queue."""
    pkg = write_package({"__init__.py": "", "tasks.py": INVOICES})
    handle = importlib.import_module(f"{pkg}.tasks").send_invoice

    assert handle(3) == "sent:3"
    assert handle.__name__ == "send_invoice"


def test_autodiscover_reraises_a_broken_subpackage(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """A subpackage whose ``__init__`` fails is fatal, and takes its subtree with it.

    ``walk_packages`` imports subpackages itself to recurse into them, and
    swallows ``ImportError`` from that import unless ``onerror`` re-raises. A
    swallowed one skips every module beneath it — the silent half-registration
    that dead-letters a task's whole backlog.
    """
    pkg = write_package(
        {
            "__init__.py": "",
            "tasks/__init__.py": "",
            "tasks/billing/__init__.py": "raise ImportError('no such dependency')\n",
            "tasks/billing/invoices.py": INVOICES,
        }
    )

    queue = Queue(db_path=str(tmp_path / "broken_pkg.db"))
    with pytest.raises(ImportError, match="no such dependency"):
        queue.autodiscover(f"{pkg}.tasks")


SAGA = """
from flexiq import task


@task()
def refund_charge(order_id):
    return f"refunded:{order_id}"


@task(compensates=refund_charge)
def charge_card(order_id):
    return f"charged:{order_id}"
"""


def test_a_deferred_task_works_as_a_saga_compensator(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """``compensates=`` branches on ``isinstance(x, TaskWrapper)``.

    That check is why ``DeferredTask`` subclasses ``TaskWrapper`` rather than
    proxying one: a handle that is not a ``TaskWrapper`` is rejected by canvas,
    sagas, and the workflow builder.
    """
    pkg = write_package({"__init__.py": "", "orders.py": SAGA})
    queue = Queue(db_path=str(tmp_path / "saga.db"))
    queue.autodiscover(pkg)

    assert queue._task_compensates[f"{pkg}.orders.charge_card"] == (f"{pkg}.orders.refund_charge")


def test_a_bound_deferred_task_makes_signatures(
    write_package: PackageWriter, tmp_path: Path
) -> None:
    """``.s()`` on a drained handle produces a usable canvas signature."""
    pkg = write_package({"__init__.py": "", "tasks.py": INVOICES})
    queue = Queue(db_path=str(tmp_path / "canvas.db"))
    queue.autodiscover(pkg)

    handle = importlib.import_module(f"{pkg}.tasks").send_invoice
    signature = handle.s(9)

    assert signature.task is handle
    assert signature.args == (9,)
