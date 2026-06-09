{{- define "swedish-financial-regulation-mcp.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "swedish-financial-regulation-mcp.labels" -}}
app.kubernetes.io/name: swedish-financial-regulation-mcp
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: ansvar-mcp-fleet
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "swedish-financial-regulation-mcp.selectorLabels" -}}
app.kubernetes.io/name: swedish-financial-regulation-mcp
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
