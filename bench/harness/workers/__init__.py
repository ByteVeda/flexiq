"""Worker and producer entry points, one module per framework.

These are imported by *their* CLIs — `celery -A …`, `dramatiq …`, an RQ worker
unpickling a function reference — so each one has to stand alone on
``PYTHONPATH`` and read its configuration from the environment.
"""
