#!/usr/bin/env sh
# Wipe the demo back to its seeded state: drop the job data, re-seed, restart.
# Caddy and its certificates are left alone. Run from cron, e.g. hourly:
#   0 * * * *  /srv/flexiq/deploy/demo/reset.sh
set -eu
cd "$(dirname "$0")"
docker compose rm --stop --force seed server executor
docker volume rm flexiq-demo-data
docker compose up -d
