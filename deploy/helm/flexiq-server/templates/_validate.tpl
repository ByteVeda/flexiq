{{/*
Fail at template time on combinations the server would reject at boot.

A chart that installs cleanly and then CrashLoopBackOffs teaches nothing; `helm
install` refusing with the reason teaches everything. Each check mirrors a
guard in crates/flexiq-server/src/config.
*/}}
{{- define "flexiq-server.validate" -}}

{{- if not (or .Values.attach.enabled .Values.dashboard.enabled .Values.webhook.enabled .Values.grpc.enabled) -}}
{{- fail "flexiq-server: nothing to run. Enable at least one of attach.enabled, dashboard.enabled, webhook.enabled or grpc.enabled." -}}
{{- end -}}

{{/* Only the webhook runs without storage. */}}
{{- if or .Values.attach.enabled .Values.dashboard.enabled .Values.grpc.enabled -}}
{{- if not (or .Values.storage.dsn .Values.storage.existingSecret) -}}
{{- fail "flexiq-server: storage.dsn or storage.existingSecret is required unless the release runs webhook.enabled alone." -}}
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
The gRPC door now carries the producer service — enqueue, read, cancel — and
nothing on it asks for a credential yet. So the server refuses a non-loopback
bind, and a pod is only ever a non-loopback bind: kubelet dials the pod IP for
both the `grpc` and the `tcpSocket` probe, so a loopback listener would never
pass readiness either.

Refusing here rather than rendering a deployment that CrashLoopBackOffs is the
same trade the checks above make. It lifts when the door has a credential.
*/}}
{{- if .Values.grpc.enabled -}}
{{- fail "flexiq-server: grpc.enabled is not available yet. The gRPC door now serves the flexiq.v1 producer service — enqueue, read and cancel — and it authenticates no caller, so the server refuses to bind it anywhere but loopback. A pod cannot use a loopback bind: kubelet probes the pod IP. This value works again in the release that adds a gRPC credential." -}}
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
