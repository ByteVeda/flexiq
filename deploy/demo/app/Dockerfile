# The demo's Python side: the seeder and the attached executor share it.
#
# `FLEXIQ_PIP_SPEC` pins the SDK, e.g. `==2.1.0`; unset installs the latest.
FROM public.ecr.aws/docker/library/python:3.12-slim
ARG FLEXIQ_PIP_SPEC=""
RUN pip install --no-cache-dir "flexiq${FLEXIQ_PIP_SPEC}"
WORKDIR /app
COPY demo_app.py seed.py ./
# The executor is SIGINT-graceful; compose's default SIGTERM would cut it off.
STOPSIGNAL SIGINT
