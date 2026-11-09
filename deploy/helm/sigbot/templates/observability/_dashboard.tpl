{{- define "sigbot.grafanaDashboard" -}}
{
  "annotations": {"list": []},
  "editable": true,
  "graphTooltip": 1,
  "panels": [
    {
      "datasource": {"type": "prometheus", "uid": {{ .root.Values.sigbot.monitoring.grafana.datasourceUid | toJson }}},
      "fieldConfig": {"defaults": {"mappings": [], "thresholds": {"mode": "absolute", "steps": [{"color": "red"}, {"color": "green", "value": 1}]}}, "overrides": []},
      "gridPos": {"h": 4, "w": 24, "x": 0, "y": 0},
      "id": 1,
      "options": {"colorMode": "value", "graphMode": "area", "justifyMode": "auto", "orientation": "auto", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false}, "textMode": "auto"},
      "targets": [{"expr": {{ printf "sum(up{namespace=~\"$namespace\",pod=~\".*%s.*\"})" .component | toJson }}, "legendFormat": "ready targets", "refId": "A"}],
      "title": "Ready scrape targets",
      "type": "stat"
    }{{- range $index, $panel := .panels }},
    {
      "datasource": {"type": "prometheus", "uid": {{ $.root.Values.sigbot.monitoring.grafana.datasourceUid | toJson }}},
      "fieldConfig": {"defaults": {"unit": {{ default "short" $panel.unit | toJson }}}, "overrides": []},
      "gridPos": {"h": 8, "w": 24, "x": 0, "y": {{ add 4 (mul $index 8) }}},
      "id": {{ add 2 $index }},
      "options": {"legend": {"calcs": ["lastNotNull"], "displayMode": "table", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [{"expr": {{ $panel.expr | toJson }}, "legendFormat": {{ default "{{label}}" $panel.legend | toJson }}, "refId": "A"}],
      "title": {{ $panel.title | toJson }},
      "type": "timeseries"
    }{{- end }}
  ],
  "refresh": "30s",
  "schemaVersion": 39,
  "tags": ["sigbot", {{ .component | toJson }}],
  "templating": {"list": [{"allValue": ".*", "current": {"text": "All", "value": "$__all"}, "datasource": {"type": "prometheus", "uid": {{ .root.Values.sigbot.monitoring.grafana.datasourceUid | toJson }}}, "definition": "label_values(up, namespace)", "includeAll": true, "label": "Namespace", "name": "namespace", "query": {"query": "label_values(up, namespace)", "refId": "PrometheusVariableQueryEditor-VariableQuery"}, "refresh": 1, "type": "query"}]},
  "time": {"from": "now-6h", "to": "now"},
  "timezone": "browser",
  "title": {{ printf "Sigbot / %s" .title | toJson }},
  "uid": {{ printf "sigbot-%s" .component | toJson }},
  "version": 1
}
{{- end -}}
