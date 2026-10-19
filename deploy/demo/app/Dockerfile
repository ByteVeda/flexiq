# The demo's Python side: the seeder and the attached executor share it.
#
# `FLEXIQ_PIP` is the SDK requirement; the release pin moves with the version.
FROM public.ecr.aws/docker/library/python:3.12-slim
ARG FLEXIQ_PIP="flexiq==2.1.0"
RUN pip install --no-cache-dir "${FLEXIQ_PIP}"
WORKDIR /app
COPY demo_app.py seed.py ./
# The executor is SIGINT-graceful; compose's default SIGTERM would cut it off.
STOPSIGNAL SIGINT
