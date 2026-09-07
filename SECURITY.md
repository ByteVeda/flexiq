# Security Policy

flexiq runs a network listener, authenticates with bearer tokens and scopes, serves an HTTPS
admission webhook, and calls out to URLs you configure. The attack surface is real, so reports
about it are welcome and there is a private channel for them.

## Supported versions

One version number covers the whole workspace: the Python, Node and Java SDKs, the crates, the
server image and the Helm chart all ship in lock-step off the version in the root `Cargo.toml`. A
security fix ships as a new patch on the current minor. There is no LTS line and nothing is
backported.

| Version | Supported |
| --- | --- |
| 2.0.x | Yes |
| 1.0.x | No — upgrade to 2.0.x |
| 0.x | No |

## Reporting a vulnerability

Report privately through GitHub:
**[Open a security advisory](https://github.com/ByteVeda/flexiq/security/advisories/new)**.

Private vulnerability reporting is enabled on this repository. The thread is visible only to you
and the maintainers, and it is the only channel watched for this. Do not use a public issue, a pull
request, or a discussion — those disclose the finding to everyone the moment you post.

A report is easiest to act on when it carries the version — one number, shared by every SDK — plus
which SDK, the storage backend (SQLite, PostgreSQL and Redis differ enough to decide whether a bug
reproduces at all), whether it is embedded or server mode, a minimal reproduction, and what an
attacker gains.

## What to expect

- **Acknowledgement within 3 business days.** Someone has read it and it is not lost.
- **An assessment within 10 business days**: severity, whether it is accepted as a vulnerability or
  falls outside the scope below, and a rough fix timeline.
- **A note when the fix ships**, naming the version it went out in.

These are the intervals a small maintainer team can actually meet, which is why they are business
days and not hours. If one passes in silence, comment on the advisory thread — that is a prompt,
not a nuisance.

## Scope

In scope:

- **The server** (`flexiq-server`): the HTTP and gRPC listeners, the dashboard API and its session
  authentication, scoped bearer tokens on the producer and executor doors, the admission webhook,
  and the readiness endpoint.
- **Outbound webhooks**: HMAC signing and the SSRF guard on subscription URLs.
- **The core and the SDKs**: job payload decoding (the native, MessagePack and CBOR envelope),
  the worker frame protocol and the attach door, dispatch lease tokens, storage query construction
  on all three backends, and namespace isolation between tenants.
- **Shipped artifacts**: the container image and the Helm chart, including defaults that are unsafe
  without saying so.

Out of scope — documented behaviour, not vulnerabilities:

- **`FLEXIQ_DASHBOARD_AUTH=off`.** That is the default, and an unauthenticated dashboard exposes
  every operate action. Bound off-host it is refused outright unless `FLEXIQ_ALLOW_INSECURE=1` says
  the network already restricts access. Choosing that is a deployment decision.
- **Other explicit opt-outs**, on the same reasoning: `FLEXIQ_WEBHOOKS_ALLOW_PRIVATE` disabling the
  SSRF guard, `FLEXIQ_DASHBOARD_INSECURE_COOKIES`, `FLEXIQ_DASHBOARD_PUBLIC_READINESS`.
- **Code you enqueue.** A worker executes the task functions you register; a producer holding a
  valid token running arbitrary work is the product, not a privilege escalation.
- **Findings that already require admin credentials**, a valid scoped token, or direct database
  access — an operator doing what the credential permits.
- **Resource exhaustion from limits you did not configure**: no rate limit, no `max_in_flight`, no
  queue depth cap.
- **Dependency advisories with no reachable path from flexiq.** Dependabot already opens pull
  requests for the reachable ones.

Hardening a deployment is a documentation question rather than a report — see
[the security guide](https://docs.byteveda.org/flexiq/python/operate/security).

## Disclosure

Disclosure is coordinated. The embargo ends the day the fix ships or 90 days after
acknowledgement, whichever comes first, and it shortens if a vulnerability is being exploited. The
advisory is published on GitHub at that point either way: a report still unfixed at 90 days is
published with what is known and whatever mitigation exists, because an embargo with no end is how
a finding gets buried rather than fixed. A CVE is requested through GitHub where one is warranted.
If you want to publish earlier, ask on the thread.

## Credit

Reporters are named in the published advisory and in the release notes for the version that carries
the fix, unless you ask not to be. There is no bug bounty — credit is the whole of it, and saying
so up front is fairer than letting you find out afterwards.

## What already runs

[CodeQL](.github/workflows/codeql.yml) scans every push and pull request, Dependabot security
updates are enabled, and so is GitHub secret scanning. That is a floor, not a policy: automated
scanning does not read a report or answer one, which is what this document is for.
