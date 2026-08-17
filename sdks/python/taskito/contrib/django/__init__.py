"""Django integration for taskito.

Add ``"taskito.contrib.django"`` to your ``INSTALLED_APPS`` and configure
via Django settings::

    INSTALLED_APPS = [
        ...
        "taskito.contrib.django",
    ]

    FLEXIQ_DB_PATH = ".flexiq/flexiq.db"
    FLEXIQ_BACKEND = "sqlite"       # or "postgres"
    FLEXIQ_DB_URL = None            # required for postgres
    FLEXIQ_WORKERS = 0              # 0 = auto-detect
    FLEXIQ_DEFAULT_RETRY = 3
    FLEXIQ_DEFAULT_TIMEOUT = 300
    FLEXIQ_DEFAULT_PRIORITY = 0
    FLEXIQ_RESULT_TTL = None        # seconds, or None to disable

Requires the ``django`` optional dependency::

    pip install taskito[django]
"""

default_app_config = "taskito.contrib.django.apps.TaskitoConfig"
