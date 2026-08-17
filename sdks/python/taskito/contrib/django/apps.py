"""Django AppConfig for taskito."""

from __future__ import annotations

try:
    from django.apps import AppConfig
except ImportError as e:
    raise ImportError(
        "Django integration requires 'django'. Install with: pip install taskito[django]"
    ) from e


class TaskitoConfig(AppConfig):
    """Django application configuration for taskito."""

    name = "taskito.contrib.django"
    verbose_name = "Taskito"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        """Auto-discover task modules in all installed apps.

        The module name defaults to ``"tasks"`` but can be overridden via the
        ``FLEXIQ_AUTODISCOVER_MODULE`` Django setting.
        """
        from django.conf import settings
        from django.utils.module_loading import autodiscover_modules

        module_name = getattr(settings, "FLEXIQ_AUTODISCOVER_MODULE", "tasks")
        autodiscover_modules(module_name)
