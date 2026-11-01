{{/*
Expand the name of the chart.
*/}}
{{- define "sigbot.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "sigbot.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "sigbot.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "sigbot.labels" -}}
helm.sh/chart: {{ include "sigbot.chart" . }}
{{ include "sigbot.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: sigbot
{{- end }}

{{/*
Selector labels
*/}}
{{- define "sigbot.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sigbot.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "sigbot.serviceAccountName" -}}
{{- if .Values.sigbot.controller.serviceAccount.create }}
{{- default (include "sigbot.fullname" .) .Values.sigbot.controller.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.sigbot.controller.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* All Sigbot images share registry/tag/digest handling and CN registry enforcement. */}}
{{- define "sigbot.image" -}}
{{- $image := .image -}}
{{- $repository := $image.repository -}}
{{- if $image.registry -}}
{{- $repository = printf "%s/%s" $image.registry $repository -}}
{{- end -}}
{{- if eq (default "global" .root.Values.global.imageSource) "aliyun" -}}
{{- if not (hasSuffix ".aliyuncs.com" (first (splitList "/" $repository))) -}}
{{- fail "IMAGE_SOURCE=aliyun requires aliyuncs.com images; render deploy/images.py --format helm" -}}
{{- end -}}
{{- end -}}
{{- if $image.digest -}}
{{- printf "%s@%s" $repository $image.digest -}}
{{- else -}}
{{- printf "%s:%s" $repository (default .root.Chart.AppVersion $image.tag) -}}
{{- end -}}
{{- end -}}

{{/* Authoritative defaults returned by the Worker API and used for CN-aware tenant registrations. */}}
{{- define "sigbot.tenantMiddlewareEnvironment" -}}
{{- range $name := list "redis" "pgvector" "timescaledb" "emqx" }}
{{- $item := index $.Values.sigbot.controller.middleware $name }}
- {name: {{ printf "SIGBOT__SERVICES__CONTROLLER__MIDDLEWARE__%s__IMAGE" (upper $name) }}, value: {{ $item.image | quote }}}
- {name: {{ printf "SIGBOT__SERVICES__CONTROLLER__MIDDLEWARE__%s__RESOURCES__CPU_MILLIS" (upper $name) }}, value: {{ $item.resources.cpu_millis | quote }}}
- {name: {{ printf "SIGBOT__SERVICES__CONTROLLER__MIDDLEWARE__%s__RESOURCES__MEMORY_MB" (upper $name) }}, value: {{ $item.resources.memory_mb | quote }}}
- {name: {{ printf "SIGBOT__SERVICES__CONTROLLER__MIDDLEWARE__%s__RESOURCES__STORAGE_GB" (upper $name) }}, value: {{ $item.resources.storage_gb | quote }}}
{{- end }}
{{- end -}}

{{/* Map the one runtime Secret into the typed central configuration. */}}
{{- define "sigbot.secretEnvironment" -}}
- name: SIGBOT__STORAGE__POSTGRES__PASSWORD
  valueFrom:
    secretKeyRef: {name: {{ required "secrets.existingSecret is required" .Values.secrets.existingSecret }}, key: POSTGRES_PASSWORD}
- name: SIGBOT__CACHE__REDIS__PASSWORD
  valueFrom:
    secretKeyRef: {name: {{ required "secrets.existingSecret is required" .Values.secrets.existingSecret }}, key: REDIS_PASSWORD}
{{- end -}}

{{/* Non-sensitive Helm overrides for the single typed Sigbot configuration. */}}
{{- define "sigbot.configEnvironment" -}}
- {name: SIGBOT_CFG_PATH, value: /etc/sigbot/sigbot.yaml}
- {name: SIGBOT__STORAGE__PROVIDER, value: Postgres}
- {name: SIGBOT__STORAGE__POSTGRES__HOST, value: {{ printf "%s-postgres" (include "sigbot.fullname" .) | quote }}}
- {name: SIGBOT__STORAGE__POSTGRES__PORT, value: "5432"}
- {name: SIGBOT__STORAGE__POSTGRES__USERNAME, value: {{ .Values.middleware.postgres.auth.username | quote }}}
- {name: SIGBOT__STORAGE__POSTGRES__DATABASE, value: {{ .Values.middleware.postgres.auth.database | quote }}}
- {name: SIGBOT__CACHE__PROVIDER, value: Redis}
- {name: SIGBOT__CACHE__REDIS__NODES, value: {{ printf "redis://%s-redis:6379" (include "sigbot.fullname" .) | quote }}}
{{- end -}}

{{/* Every database-owning service waits for and applies the checksum-guarded baseline. */}}
{{- define "sigbot.migrationInitContainer" -}}
- name: migrate
  image: {{ include "sigbot.image" (dict "root" .root "image" .image) | quote }}
  imagePullPolicy: {{ .root.Values.sigbot.image.pullPolicy }}
  command: ["python3", "/app/migration/migrate.py", "--provider", "postgres"]
  env:
    - {name: PGHOST, value: {{ printf "%s-postgres" (include "sigbot.fullname" .root) | quote }}}
    - {name: PGPORT, value: "5432"}
    - {name: PGUSER, value: {{ .root.Values.middleware.postgres.auth.username | quote }}}
    - {name: PGDATABASE, value: {{ .root.Values.middleware.postgres.auth.database | quote }}}
    - name: PGPASSWORD
      valueFrom:
        secretKeyRef:
          name: {{ required "secrets.existingSecret is required" .root.Values.secrets.existingSecret }}
          key: POSTGRES_PASSWORD
  securityContext:
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    capabilities: {drop: [ALL]}
{{- end -}}
