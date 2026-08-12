{{- define "kronosdb.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "kronosdb.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "kronosdb.labels" -}}
app.kubernetes.io/name: {{ include "kronosdb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "kronosdb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kronosdb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "kronosdb.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{/* Stable DNS peer list: "1=<sts>-0.<hl>.<ns>.svc.<domain>:50051,…".
     Voter node IDs are ordinal+1; addresses are persisted in Raft
     membership, so these names must never change across upgrades. */}}
{{- define "kronosdb.peers" -}}
{{- $ctx := . -}}
{{- $parts := list -}}
{{- range $i := until (int .Values.replicas) -}}
{{- $parts = append $parts (printf "%d=%s-%d.%s-hl.%s.svc.%s:50051" (add $i 1) (include "kronosdb.fullname" $ctx) $i (include "kronosdb.fullname" $ctx) $ctx.Release.Namespace $ctx.Values.clusterDomain) -}}
{{- end -}}
{{- join "," $parts -}}
{{- end -}}

{{/* Learner IDs start at 101 so they can never collide with voter IDs. */}}
{{- define "kronosdb.learnerPeers" -}}
{{- $ctx := . -}}
{{- $parts := list -}}
{{- range $i := until (int .Values.learners.replicas) -}}
{{- $parts = append $parts (printf "%d=%s-learner-%d.%s-hl.%s.svc.%s:50051" (add $i 101) (include "kronosdb.fullname" $ctx) $i (include "kronosdb.fullname" $ctx) $ctx.Release.Namespace $ctx.Values.clusterDomain) -}}
{{- end -}}
{{- join "," $parts -}}
{{- end -}}

{{/* Shared container spec for voters and learners. Pass a dict with
     "root" (chart context) and "nodeType" ("standard"|"passive-backup")
     and "idOffset" (1 for voters, 101 for learners). */}}
{{- define "kronosdb.container" -}}
{{- $root := .root -}}
name: kronosdb
image: {{ include "kronosdb.image" $root }}
imagePullPolicy: {{ $root.Values.image.pullPolicy }}
# The node ID is derived from the pod ordinal at startup — StatefulSet pods
# have stable names, so IDs are stable across restarts and reschedules.
command: ["tini", "--", "/bin/sh", "-c"]
args:
  - |
    export KRONOSDB_CLUSTER_NODE_ID=$(( ${HOSTNAME##*-} + {{ .idOffset }} ))
    exec kronosdb-server
ports:
  - { name: grpc, containerPort: 50051 }
  - { name: admin, containerPort: 9240 }
env:
  - name: KRONOSDB_NODE_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: KRONOSDB_CLUSTER_NODE_TYPE
    value: {{ .nodeType | quote }}
  - name: KRONOSDB_CLUSTER_PEERS
    value: {{ include "kronosdb.peers" $root | quote }}
  {{- if gt (int $root.Values.learners.replicas) 0 }}
  - name: KRONOSDB_CLUSTER_LEARNERS
    value: {{ include "kronosdb.learnerPeers" $root | quote }}
  {{- end }}
  - name: KRONOSDB_ACK_MODE
    value: {{ $root.Values.config.ackMode | quote }}
  - name: KRONOSDB_DRAIN_DEADLINE
    value: {{ $root.Values.config.drainDeadlineSecs | quote }}
  {{- if $root.Values.contexts }}
  - name: KRONOSDB_MANIFEST
    value: /etc/kronosdb/manifest.toml
  {{- end }}
  {{- if $root.Values.backup.url }}
  - name: KRONOSDB_BACKUP_URL
    value: {{ $root.Values.backup.url | quote }}
  - name: KRONOSDB_BACKUP_INTERVAL_SECS
    value: {{ $root.Values.backup.intervalSecs | quote }}
  {{- end }}
  {{- if or $root.Values.auth.accessToken $root.Values.auth.existingSecret }}
  - name: KRONOSDB_ACCESS_TOKEN
    valueFrom:
      secretKeyRef:
        name: {{ $root.Values.auth.existingSecret | default (printf "%s-auth" (include "kronosdb.fullname" $root)) }}
        key: access-token
  {{- end }}
  - name: KRONOSDB_ADMIN_AUTH_MODE
    value: {{ $root.Values.admin.authMode | quote }}
  {{- if eq $root.Values.admin.authMode "token" }}
  - name: KRONOSDB_ADMIN_TOKEN
    valueFrom:
      secretKeyRef:
        name: {{ $root.Values.admin.existingSecret | default (printf "%s-auth" (include "kronosdb.fullname" $root)) }}
        key: admin-token
  {{- end }}
  {{- if $root.Values.tls.secretName }}
  - name: KRONOSDB_TLS_CERT
    value: /etc/kronosdb/tls/tls.crt
  - name: KRONOSDB_TLS_KEY
    value: /etc/kronosdb/tls/tls.key
  {{- end }}
  {{- with $root.Values.config.extraEnv }}
  {{- toYaml . | nindent 2 }}
  {{- end }}
{{- if $root.Values.backup.credentialsSecret }}
envFrom:
  - secretRef:
      name: {{ $root.Values.backup.credentialsSecret }}
{{- end }}
volumeMounts:
  - { name: data, mountPath: /data }
  {{- if $root.Values.contexts }}
  - { name: manifest, mountPath: /etc/kronosdb/manifest.toml, subPath: manifest.toml, readOnly: true }
  {{- end }}
  {{- if $root.Values.tls.secretName }}
  - { name: tls, mountPath: /etc/kronosdb/tls, readOnly: true }
  {{- end }}
resources: {{- toYaml $root.Values.resources | nindent 2 }}
livenessProbe:
  httpGet: { path: /health, port: admin }
readinessProbe:
  httpGet: { path: /ready, port: admin }
startupProbe:
  httpGet: { path: /ready, port: admin }
  failureThreshold: {{ $root.Values.startupProbe.failureThreshold }}
  periodSeconds: {{ $root.Values.startupProbe.periodSeconds }}
{{- end -}}
