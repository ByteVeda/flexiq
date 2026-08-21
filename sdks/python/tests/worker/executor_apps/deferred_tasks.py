"""Deferred declarations, which never import the module holding the Queue."""

from __future__ import annotations

from flexiq import task


@task(max_retries=0)
def send_invoice(user_id: int) -> str:
    return f"sent:{user_id}"


@task(max_retries=0)
def build_report() -> str:
    return "report"
