"""Django AppConfig for flexiq."""

from __future__ import annotations

try:
    from django.apps import AppConfig
except ImportError as e:
    raise ImportError(
        "Django integration requires 'django'. Install with: pip install flexiq[django]"
    ) from e


class FlexiQConfig(AppConfig):
    """Django application configuration for flexiq."""

    name = "flexiq.contrib.django"
    verbose_name = "FlexiQ"
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
