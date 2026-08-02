{{/*
Names and labels, plus the two secret lookups every template shares.
*/}}

{{- define "taskito-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "taskito-server.fullname" -}}
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

{{- define "taskito-server.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "taskito-server.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "taskito-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "taskito-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "taskito-server.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "taskito-server.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Secret this release creates for the values that were given inline. */}}
{{- define "taskito-server.secretName" -}}
{{- printf "%s-config" (include "taskito-server.fullname" .) -}}
{{- end -}}

{{- define "taskito-server.webhookSecretName" -}}
{{- printf "%s-webhook-tls" (include "taskito-server.fullname" .) -}}
{{- end -}}

{{/*
Whether the release has to create a Secret at all: only when a sensitive value
was supplied inline rather than pointed at an existing Secret.
*/}}
{{- define "taskito-server.createsSecret" -}}
{{- $create := false -}}
{{- if and .Values.storage.dsn (not .Values.storage.existingSecret) -}}{{- $create = true -}}{{- end -}}
{{- if and .Values.attach.token (not .Values.attach.existingSecret) -}}{{- $create = true -}}{{- end -}}
{{- if and .Values.dashboard.adminPassword (not .Values.dashboard.existingSecret) -}}{{- $create = true -}}{{- end -}}
{{- if .Values.dashboard.metricsToken -}}{{- $create = true -}}{{- end -}}
{{- if $create -}}true{{- end -}}
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
{{- define "taskito-server.webhookCert" -}}
{{- $cached := index .Values "__webhookCert" -}}
{{- if not $cached -}}
  {{- $name := include "taskito-server.webhookSecretName" . -}}
  {{- $existing := lookup "v1" "Secret" .Release.Namespace $name -}}
  {{- if and $existing $existing.data (index $existing.data "ca.crt") -}}
    {{- $cached = dict
          "caCert" (index $existing.data "ca.crt")
          "tlsCert" (index $existing.data "tls.crt")
          "tlsKey" (index $existing.data "tls.key") -}}
  {{- else -}}
    {{- $service := printf "%s-webhook" (include "taskito-server.fullname" .) -}}
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
