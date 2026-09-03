{{/* Chart name, overridable with nameOverride. */}}
{{- define "dcontroller.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Name prefix for the release's objects. The default is the plain chart name
rather than the usual "<release>-<chart>": the documentation, the API server
kubeconfig profiles and the CRD all address the manager as "dcontroller", and a
second release in one cluster would collide on the cluster-scoped Operator CRD
in any case.
*/}}
{{- define "dcontroller.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "dcontroller.name" . | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "dcontroller.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "dcontroller.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "dcontroller.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dcontroller.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
control-plane: {{ include "dcontroller.fullname" . }}
{{- end -}}

{{- define "dcontroller.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (printf "%s-account" (include "dcontroller.fullname" .)) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Container image reference; the tag falls back to the chart appVersion. */}}
{{- define "dcontroller.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
