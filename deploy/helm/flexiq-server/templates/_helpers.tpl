{{/*
Names and labels, plus the two secret lookups every template shares.
*/}}

{{- define "flexiq-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "flexiq-server.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "flexiq-server.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "flexiq-server.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "flexiq-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flexiq-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "flexiq-server.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "flexiq-server.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Secret this release creates for the values that were given inline. */}}
{{- define "flexiq-server.secretName" -}}
{{- printf "%s-config" (include "flexiq-server.fullname" .) -}}
{{- end -}}

{{- define "flexiq-server.webhookSecretName" -}}
{{- printf "%s-webhook-tls" (include "flexiq-server.fullname" .) -}}
{{- end -}}

{{/*
Whether the release has to create a Secret at all: only when a sensitive value
was supplied inline rather than pointed at an existing Secret.
*/}}
{{- define "flexiq-server.createsSecret" -}}
{{- $create := false -}}
{{- if and .Values.storage.dsn (not .Values.storage.existingSecret) -}}{{- $create = true -}}{{- end -}}
{{- if and .Values.attach.token (not .Values.attach.existingSecret) -}}{{- $create = true -}}{{- end -}}
{{- if and .Values.dashboard.adminPassword (not .Values.dashboard.existingSecret) -}}{{- $create = true -}}{{- end -}}
{{- if .Values.dashboard.metricsToken -}}{{- $create = true -}}{{- end -}}
{{- if $create -}}true{{- end -}}
{{- end -}}

{{/*
Whether terminationGracePeriodSeconds was explicitly set — including to 0.

`with` and a bare truthiness test are both falsey on 0, same trap either way:
0 is a value Kubernetes accepts (kill immediately, no grace) and has to read
as "set", not "unset", so an explicit nil/"" test is the only correct guard.
Shared by deployment.yaml (what to render) and _validate.tpl (what to check).
*/}}
{{- define "flexiq-server.terminationGraceIsSet" -}}
{{- if not (or (kindIs "invalid" .Values.terminationGracePeriodSeconds) (eq (toString .Values.terminationGracePeriodSeconds) "")) -}}true{{- end -}}
{{- end -}}

{{/*
The webhook's serving certificate, as `caCert` / `tlsCert` / `tlsKey` in
base64.

Generated at most once per render and memoised in `.Values`, which every
template shares. Without that, each caller would get a *different* CA —
`genCA` is random — and the caBundle on the MutatingWebhookConfiguration would
not sign the certificate the pod actually serves, so every admission call would
fail TLS verification.

An existing Secret wins over generating, so `helm upgrade` does not mint a new
CA and leave the API server trusting the old one.
*/}}
{{- define "flexiq-server.webhookCert" -}}
{{- $cached := index .Values "__webhookCert" -}}
{{- if not $cached -}}
  {{- $name := include "flexiq-server.webhookSecretName" . -}}
  {{- $existing := lookup "v1" "Secret" .Release.Namespace $name -}}
  {{- if and $existing $existing.data (index $existing.data "ca.crt") -}}
    {{- $cached = dict
          "caCert" (index $existing.data "ca.crt")
          "tlsCert" (index $existing.data "tls.crt")
          "tlsKey" (index $existing.data "tls.key") -}}
  {{- else -}}
    {{- $service := printf "%s-webhook" (include "flexiq-server.fullname" .) -}}
    {{- $altNames := list
          (printf "%s.%s.svc" $service .Release.Namespace)
          (printf "%s.%s.svc.cluster.local" $service .Release.Namespace) -}}
    {{- $days := int .Values.webhook.certValidityDays -}}
    {{- $ca := genCA (printf "%s-ca" $service) $days -}}
    {{- $cert := genSignedCert $service nil $altNames $days $ca -}}
    {{- $cached = dict
          "caCert" ($ca.Cert | b64enc)
          "tlsCert" ($cert.Cert | b64enc)
          "tlsKey" ($cert.Key | b64enc) -}}
  {{- end -}}
  {{- $_ := set .Values "__webhookCert" $cached -}}
{{- end -}}
caCert: {{ $cached.caCert }}
tlsCert: {{ $cached.tlsCert }}
tlsKey: {{ $cached.tlsKey }}
{{- end -}}

{{/*
TLS on a credentialled listener (attach or gRPC), from a Secret mounted at
/etc/flexiq/<dir>-tls and, for mTLS, a client-CA Secret at
/etc/flexiq/<dir>-client-ca. The server re-reads both when a renewal swaps the
files, so neither needs a restart or a checksum annotation.

Called with (dict "prefix" "FLEXIQ_GRPC" "dir" "grpc" "tls" .Values.grpc.tls).
*/}}
{{- define "flexiq-server.tlsEnv" -}}
{{- if .tls.secretName }}
- name: {{ .prefix }}_TLS_CERT
  value: /etc/flexiq/{{ .dir }}-tls/tls.crt
- name: {{ .prefix }}_TLS_KEY
  value: /etc/flexiq/{{ .dir }}-tls/tls.key
{{- if .tls.clientCaSecretName }}
- name: {{ .prefix }}_TLS_CLIENT_CA
  value: /etc/flexiq/{{ .dir }}-client-ca/{{ .tls.clientCaKey }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "flexiq-server.tlsMounts" -}}
{{- if .tls.secretName }}
- name: {{ .dir }}-tls
  mountPath: /etc/flexiq/{{ .dir }}-tls
  readOnly: true
{{- if .tls.clientCaSecretName }}
- name: {{ .dir }}-client-ca
  mountPath: /etc/flexiq/{{ .dir }}-client-ca
  readOnly: true
{{- end }}
{{- end }}
{{- end -}}

{{/*
The audit window in days: audit.retentionDays, else the legacy
grpc.auditRetentionDays. `_validate.tpl` has already refused a value that is
not a whole number of at least 1.
*/}}
{{- define "flexiq-server.auditRetentionDays" -}}
{{- if kindIs "invalid" .Values.audit.retentionDays -}}
{{- .Values.grpc.auditRetentionDays | int64 -}}
{{- else -}}
{{- .Values.audit.retentionDays | int64 -}}
{{- end -}}
{{- end -}}

{{/* "true" when `.` is a whole number of at least 1, as a number or a decimal string. */}}
{{- define "flexiq-server.wholeDays" -}}
{{- $v := . -}}
{{- if kindIs "string" $v -}}
{{- if and (regexMatch "^[1-9][0-9]*$" $v) (eq (toString (int64 $v)) $v) }}true{{ end -}}
{{- else if or (kindIs "int" $v) (kindIs "int64" $v) (kindIs "float64" $v) -}}
{{- if and (eq (float64 $v) (float64 (int64 $v))) (ge (int64 $v) 1) }}true{{ end -}}
{{- end -}}
{{- end -}}

{{- define "flexiq-server.tlsVolumes" -}}
{{- if .tls.secretName }}
- name: {{ .dir }}-tls
  secret:
    secretName: {{ .tls.secretName }}
{{- if .tls.clientCaSecretName }}
- name: {{ .dir }}-client-ca
  secret:
    secretName: {{ .tls.clientCaSecretName }}
{{- end }}
{{- end }}
{{- end -}}
