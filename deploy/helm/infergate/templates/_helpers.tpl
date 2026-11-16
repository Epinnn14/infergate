{{- define "infergate.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "infergate.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* selector labels: (dict "ctx" $ "component" "gateway") */}}
{{- define "infergate.selector" -}}
app.kubernetes.io/name: {{ .ctx.Chart.Name }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* image reference: (dict "ctx" $ "image" .Values.images.gateway) */}}
{{- define "infergate.image" -}}
{{- printf "%s:%s" .image.repository (default .ctx.Chart.AppVersion .image.tag) -}}
{{- end -}}

{{- define "infergate.secret" -}}{{ include "infergate.fullname" . }}{{- end -}}

{{/* a generated secret value that survives upgrades: (dict "ctx" $ "key" "admin-token" "value" .Values.registry.adminToken "len" 32) */}}
{{- define "infergate.keep" -}}
{{- if .value -}}
{{- .value -}}
{{- else -}}
{{- $old := lookup "v1" "Secret" .ctx.Release.Namespace (include "infergate.secret" .ctx) -}}
{{- if and $old $old.data (hasKey $old.data .key) -}}
{{- index $old.data .key | b64dec -}}
{{- else -}}
{{- randAlphaNum (int .len) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "infergate.redisHost" -}}
{{- if .Values.redis.enabled -}}
{{- printf "%s-redis" (include "infergate.fullname" .) -}}
{{- else -}}
{{- required "redis.external.addr is required when redis.enabled=false" .Values.redis.external.addr | splitList ":" | first -}}
{{- end -}}
{{- end -}}

{{- define "infergate.redisPort" -}}
{{- if .Values.redis.enabled -}}6379{{- else -}}{{- $p := splitList ":" .Values.redis.external.addr -}}{{- if gt (len $p) 1 -}}{{ last $p }}{{- else -}}6379{{- end -}}{{- end -}}
{{- end -}}

{{/* env var IG_REDIS_PASSWORD, from our Secret or an existing one */}}
{{- define "infergate.redisPasswordEnv" -}}
- name: IG_REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      {{- if and (not .Values.redis.enabled) .Values.redis.external.existingSecret }}
      name: {{ .Values.redis.external.existingSecret }}
      key: {{ .Values.redis.external.passwordKey }}
      {{- else }}
      name: {{ include "infergate.secret" . }}
      key: redis-password
      {{- end }}
{{- end -}}

{{- define "infergate.databaseUrlEnv" -}}
{{- if .Values.postgres.enabled }}
- name: PG_PASSWORD
  valueFrom: { secretKeyRef: { name: {{ include "infergate.secret" . }}, key: postgres-password } }
- name: IG_DATABASE_URL
  value: {{ printf "postgresql://%s:$(PG_PASSWORD)@%s-postgres:5432/%s" .Values.postgres.user (include "infergate.fullname" .) .Values.postgres.database | quote }}
{{- else }}
- name: IG_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ default (include "infergate.secret" .) .Values.postgres.external.existingSecret }}
      key: {{ if .Values.postgres.external.existingSecret }}{{ .Values.postgres.external.urlKey }}{{ else }}database-url{{ end }}
{{- end }}
{{- end -}}

{{/* the registry's bootstrap document, from models and apiKeys */}}
{{- define "infergate.bootstrap" -}}
{{- $models := list -}}
{{- range .Values.models -}}
{{- $versions := list -}}
{{- range .versions -}}
{{- $versions = append $versions (dict "name" .name "weight" (int .weight) "backend" (default "fake" .backend)) -}}
{{- end -}}
{{- $models = append $models (dict "name" .name "description" (default "" .description) "versions" $versions) -}}
{{- end -}}
{{- $keys := list -}}
{{- range .Values.apiKeys -}}
{{- $keys = append $keys (dict "name" .name "key" .key "rpm" (int (default 0 .rpm)) "tpm" (int (default 0 .tpm)) "models" (default list .models)) -}}
{{- end -}}
{{- dict "models" $models "keys" $keys | toJson -}}
{{- end -}}

{{/* extra pod labels for the components that join the mesh (not Redis/Postgres: KEDA reads Redis from outside it) */}}
{{- define "infergate.meshLabels" -}}
{{- if .Values.istio.enabled }}
sidecar.istio.io/inject: "true"
{{- end }}
{{- end -}}
