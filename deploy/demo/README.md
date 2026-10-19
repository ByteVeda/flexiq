# FlexiQ public demo

A `flexiq-server` anyone can look at: a read-only dashboard seeded with a job in
every state, and one button that enqueues a job so a visitor can watch it move.

```
             ┌───────── Caddy :80/:443 (the only published port) ─────────┐
 browser ──▶ │  /t/demo ─▶ server:9090 trigger    everything else ─▶ :8080 │
             └─────────────────────────────────────────────────────────────┘
 server  (scheduler + read-only dashboard + trigger + attach :7777)
   ▲ attach
 executor (python, runs demo_app.py)        seed (one-shot, before server)
```

## What a visitor sees

| State | Job | How it got there |
| --- | --- | --- |
| Succeeded | `demo.hello` ×5 | Ran on the executor |
| Retrying | `demo.flaky` | Fails every attempt, retried every few minutes until the reset |
| Dead-lettered | `demo.doomed` | Fails with `max_retries=0` |
| Sleeping | `demo.nap` | Parked in `step.sleep("1h")` |
| Running workflow | `release` | `build` done, waiting at the `approve` gate |

Every job runs a task from [`app/demo_app.py`](app/demo_app.py). The button
adds one more `demo.hello`.

## Threat model

It is a public listener, so it gets the treatment a production one would:

- **Read-only dashboard.** `FLEXIQ_DASHBOARD_AUTH=public` makes every visitor an
  anonymous viewer. Every write is answered `403 read_only` before it is
  audited, and the token inventory and audit trail stay closed. There is no login
  to attack.
- **No producer door.** The gRPC door is off. The single way in is the `demo`
  trigger, and a trigger cannot choose its task: it is fixed to `demo.hello` in
  [`triggers.json`](triggers.json), so the endpoint runs no submitted code.
- **Hard rate limit.** `20/m`, held in storage and shared by every visitor.
  A request over the limit gets `429` and enqueues nothing. Bodies are capped
  at 1 KiB.
- **The trigger secret is public.** The button sends `?token=flexiq-public-demo`,
  so anyone can read it. Triggers have no anonymous mode on purpose, and
  publishing this value keeps that rule intact. The fixed task and the rate
  bucket are what protect the endpoint.
- **Only Caddy is published.** The attach listener (token required), the trigger
  port's `/healthz` and the dashboard's own port stay on the compose network.
  `/metrics` needs `FLEXIQ_DASHBOARD_METRICS_TOKEN`, and that is left unset.
- **Bounded growth.** At most `20/m` jobs arrive. `reset.sh` wipes the data and
  re-seeds.

## Run it

Put the attach token in `.env` so `reset.sh` finds it under cron too:

```sh
cd deploy/demo
echo "FLEXIQ_ATTACH_TOKEN=$(openssl rand -base64 32)" > .env
docker compose up -d --build
```

Open <http://localhost>. To host it, set `FLEXIQ_DEMO_SITE=demo.example.com`
in `.env` and point DNS at the host. Caddy then obtains a certificate itself.

The server image builds from this checkout. Public mode is newer than the
latest release, and the musl build takes several minutes. The Python side
installs `flexiq` from PyPI; pin it with `--build-arg FLEXIQ_PIP_SPEC===2.1.0`.

## Reset on a timer

```cron
0 * * * *  /srv/flexiq/deploy/demo/reset.sh
```

`reset.sh` drops the `flexiq-demo-data` volume and runs `docker compose up -d`,
which re-seeds before the server starts. Caddy's certificates live in their own
volume and survive the reset.
