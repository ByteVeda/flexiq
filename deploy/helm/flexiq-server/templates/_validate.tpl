{{/*
Fail at template time on combinations the server would reject at boot.

A chart that installs cleanly and then CrashLoopBackOffs teaches nothing; `helm
install` refusing with the reason teaches everything. Each check mirrors a
guard in crates/flexiq-server/src/config.
*/}}
{{- define "flexiq-server.validate" -}}

{{- if not (or .Values.attach.enabled .Values.dashboard.enabled .Values.webhook.enabled .Values.grpc.enabled .Values.push.enabled) -}}
{{- fail "flexiq-server: nothing to run. Enable at least one of attach.enabled, dashboard.enabled, webhook.enabled, grpc.enabled or push.enabled." -}}
{{- end -}}

{{/* Only the webhook runs without storage. */}}
{{- if or .Values.attach.enabled .Values.dashboard.enabled .Values.grpc.enabled .Values.push.enabled -}}
{{- if not (or .Values.storage.dsn .Values.storage.existingSecret) -}}
{{- fail "flexiq-server: storage.dsn or storage.existingSecret is required unless the release runs webhook.enabled alone." -}}
{{- end -}}
{{- end -}}

{{/*
A Worker holds exactly one dispatcher — the server itself refuses to start
with both FLEXIQ_LISTEN and FLEXIQ_PUSH_TARGET_URL set, so the chart fails at
template time instead of letting the pod CrashLoopBackOff on it.
*/}}
{{- if and .Values.push.enabled .Values.attach.enabled -}}
{{- fail "flexiq-server: push.enabled and attach.enabled cannot both be set — a Worker holds exactly one dispatcher. Enable one or the other." -}}
{{- end -}}

{{/* Mirrors config/push.rs: a push target announces no slots and no guard of its own. */}}
{{- if .Values.push.enabled -}}
{{- if not .Values.push.url -}}
{{- fail "flexiq-server: push.enabled requires push.url — where the scheduler POSTs a claimed job." -}}
{{- end -}}
{{- if not .Values.push.capacity -}}
{{- fail "flexiq-server: push.enabled requires push.capacity — a push target announces no slots of its own, so nothing here can infer one." -}}
{{- end -}}
{{- if not .Values.push.allow -}}
{{- fail "flexiq-server: push.enabled requires push.allow — a guard whose default is derived from the value it guards is not a guard. List every host and CIDR the scheduler may dispatch a job to." -}}
{{- end -}}
{{/* Mirrors Config::from_map: a 202 is a hand-off to somewhere, and the
     executor door is that somewhere. Refused at render rather than letting the
     pod crash-loop on the same check at boot. */}}
{{- if and (ne (.Values.push.settle | default "off") "off") (not .Values.grpc.enabled) -}}
{{- fail "flexiq-server: push.settle=grpc needs grpc.enabled — the executor door is what a push target reports a later outcome through. Enable grpc, or set push.settle to off." -}}
{{- end -}}
{{/*
A push shutdown spends push.drain twice — once waiting for in-flight
dispatches, again for each to settle once abandoned — so anything at or under
2 × push.drain lets Kubernetes SIGKILL the pod mid-abandonment, and every lease
still open at that instant goes to the stale-job reaper instead of settling.
An unset value is left alone here: deployment.yaml computes a safe one. 0 is
also left alone: it is a deliberate "kill immediately" override, not a
mistaken guess at a sufficient number, and the chart already lets it through.
*/}}
{{- if eq (include "flexiq-server.terminationGraceIsSet" .) "true" -}}
{{- $drain := .Values.push.drain | int64 -}}
{{- $minGrace := mul 2 $drain -}}
{{- $grace := .Values.terminationGracePeriodSeconds | int64 -}}
{{- if and (ne $grace 0) (not (gt $grace $minGrace)) -}}
{{- fail (printf "flexiq-server: terminationGracePeriodSeconds=%d does not clear push's shutdown budget — a push shutdown spends push.drain (%ds) twice before Kubernetes sends SIGKILL, so terminationGracePeriodSeconds must be greater than 2 × push.drain (%ds) while push.enabled. Raise terminationGracePeriodSeconds past %ds, or lower push.drain so 2 × it fits under the value you set." $grace $drain $minGrace $minGrace) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
SQLite is a local file with one writer. This chart mounts no volume for it, so
the database would live on the container filesystem and vanish with the pod —
and a second replica would not see the first one's jobs at all.
*/}}
{{- if .Values.storage.dsn -}}
{{- $dsn := .Values.storage.dsn -}}
{{- if not (or (hasPrefix "postgres://" $dsn) (hasPrefix "postgresql://" $dsn) (hasPrefix "redis://" $dsn) (hasPrefix "rediss://" $dsn)) -}}
{{- fail "flexiq-server: storage.dsn does not name Postgres or Redis. A SQLite database is a local file this chart mounts no volume for — it would be lost with the pod, and a second replica would not share it. Use postgres:// or redis:// on a cluster." -}}
{{- end -}}
{{- end -}}

{{/*
The attach port binds 0.0.0.0 so executors in other pods can reach it, and an
attach connection dispatches code — the server refuses that bind without a
token.
*/}}
{{- if and .Values.attach.enabled (not (or .Values.attach.token .Values.attach.existingSecret)) -}}
{{- fail "flexiq-server: attach.token or attach.existingSecret is required — the attach port dispatches code and binds beyond loopback. Generate one with `openssl rand -base64 32`." -}}
{{- end -}}

{{- if and .Values.attach.token (lt (len .Values.attach.token) 16) -}}
{{- fail "flexiq-server: attach.token must be at least 16 characters — the server rejects a guessable one." -}}
{{- end -}}

{{- if .Values.dashboard.enabled -}}
{{- if not (has .Values.dashboard.auth (list "off" "session")) -}}
{{- fail (printf "flexiq-server: dashboard.auth must be 'off' or 'session', got '%s'." .Values.dashboard.auth) -}}
{{- end -}}
{{- if and (eq .Values.dashboard.auth "off") (not .Values.dashboard.allowInsecure) -}}
{{- fail "flexiq-server: dashboard.auth=off exposes every operate action to anyone who reaches the Service. Set dashboard.auth=session, or dashboard.allowInsecure=true if the network already restricts access." -}}
{{- end -}}
{{- end -}}

{{/*
The gRPC credential is no longer a chart value. Tokens live in the database, so
a release that still sets one is configuring something that no longer exists —
fail rather than start a door the operator believes is credentialled by a value
nothing reads.
*/}}
{{- if or .Values.grpc.token .Values.grpc.existingSecret .Values.grpc.existingSecretKey -}}
{{- fail "flexiq-server: grpc.token, grpc.existingSecret and grpc.existingSecretKey are gone — the gRPC door now accepts scoped API tokens stored in the database. Remove the value and mint one with `kubectl exec deploy/<release>-flexiq-server -- flexiq-server token create --name <name> --scope produce`, or from the dashboard." -}}
{{- end -}}

{{/*
The gRPC door serves exactly one namespace and refuses to start without one:
an unset namespace means "every namespace" to a read and "only the unnamespaced
rows" to a dequeue, and neither is a thing to put on a network port.
*/}}
{{- if and .Values.grpc.enabled (not .Values.namespace) -}}
{{- fail "flexiq-server: grpc.enabled requires namespace. The gRPC door serves one named namespace, and the server refuses to start without FLEXIQ_NAMESPACE." -}}
{{- end -}}

{{- if and .Values.webhook.enabled .Values.webhook.certManager.enabled (not (.Capabilities.APIVersions.Has "cert-manager.io/v1")) -}}
{{- fail "flexiq-server: webhook.certManager.enabled is set but cert-manager.io/v1 is not installed in this cluster. Install cert-manager, or leave it false to use a chart-generated certificate." -}}
{{- end -}}

{{- end -}}
