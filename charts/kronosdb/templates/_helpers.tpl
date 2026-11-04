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
{{/* Fails unless .value is a whole number: int64 alone turns true into 1
     and "30s" into 0. Pass a dict with "name" and "value". */}}
{{- define "kronosdb.wholeNumber" -}}
{{- $v := .value -}}
{{- if kindIs "string" $v -}}
{{- if not (regexMatch "^[0-9]+$" $v) -}}
{{- fail (printf "%s must be a whole number, not %q." .name $v) -}}
{{- end -}}
{{- else if kindIs "float64" $v -}}
{{- if ne $v (float64 (int64 $v)) -}}
{{- fail (printf "%s must be a whole number, not %v." .name $v) -}}
{{- end -}}
{{- else if not (or (kindIs "int" $v) (kindIs "int64" $v)) -}}
{{- fail (printf "%s must be a whole number, not %v." .name $v) -}}
{{- end -}}
{{- end -}}

{{- define "kronosdb.container" -}}
{{- $root := .root -}}
{{- $nodeType := .nodeType -}}
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
  - { name: client, containerPort: 50051 }
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
    value: {{ include "kronosdb.wholeNumber" (dict "name" "config.drainDeadlineSecs" "value" $root.Values.config.drainDeadlineSecs) }}{{ int64 $root.Values.config.drainDeadlineSecs | quote }}
  {{- if $root.Values.contexts }}
  - name: KRONOSDB_MANIFEST
    value: /etc/kronosdb/manifest.toml
  {{- end }}
  {{- /* Numbers go through int64: YAML numbers reach templates as
         float64, and quote renders 500000000000 as "5e+11". */}}
  {{- with $root.Values.archive }}
  {{- if and .maxLocalBytes (not .url) }}
  {{- fail "archive.maxLocalBytes is set but archive.url is empty: evicted segments are read back from the archive, so a cap needs one. Set archive.url, or unset archive.maxLocalBytes." }}
  {{- end }}
  {{- if .url }}
  {{- include "kronosdb.wholeNumber" (dict "name" "archive.intervalSecs" "value" .intervalSecs) }}
  {{- if lt (int64 .intervalSecs) 1 }}
  {{- fail "archive.intervalSecs must be at least 1." }}
  {{- end }}
  - name: KRONOSDB_ARCHIVE_URL
    value: {{ .url | quote }}
  - name: KRONOSDB_ARCHIVE_INTERVAL_SECS
    value: {{ int64 .intervalSecs | quote }}
  {{- /* Learners are kept for backup: they keep every segment, so the
         archive is never the only copy of the oldest ones. */}}
  {{- if and .maxLocalBytes (ne $nodeType "passive-backup") }}
  {{- include "kronosdb.wholeNumber" (dict "name" "archive.maxLocalBytes" "value" .maxLocalBytes) }}
  {{- if lt (int64 .maxLocalBytes) 1 }}
  {{- fail (printf "archive.maxLocalBytes must be a whole number of bytes above 0, not %v; leave it empty to keep every segment." .maxLocalBytes) }}
  {{- end }}
  - name: KRONOSDB_ARCHIVE_MAX_LOCAL_BYTES
    value: {{ int64 .maxLocalBytes | quote }}
  {{- end }}
  {{- end }}
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
  {{- if $root.Values.tls.clientAuth }}
  - name: KRONOSDB_TLS_CA
    value: /etc/kronosdb/tls/ca.crt
  - name: KRONOSDB_TLS_CLIENT_AUTH
    value: {{ $root.Values.tls.clientAuth | quote }}
  {{- end }}
  {{- end }}
  {{- if include "kronosdb.identityEnabled" $root }}
  - name: KRONOSDB_CONFIG
    value: /etc/kronosdb/config/kronosdb.toml
  {{- end }}
  {{- with $root.Values.config.extraEnv }}
  {{- toYaml . | nindent 2 }}
  {{- end }}
{{- if $root.Values.archive.credentialsSecret }}
envFrom:
  - secretRef:
      name: {{ $root.Values.archive.credentialsSecret }}
{{- end }}
volumeMounts:
  - { name: data, mountPath: /data }
  {{- if $root.Values.contexts }}
  - { name: manifest, mountPath: /etc/kronosdb/manifest.toml, subPath: manifest.toml, readOnly: true }
  {{- end }}
  {{- if $root.Values.tls.secretName }}
  - { name: tls, mountPath: /etc/kronosdb/tls, readOnly: true }
  {{- end }}
  {{- if include "kronosdb.identityEnabled" $root }}
  - { name: config, mountPath: /etc/kronosdb/config, readOnly: true }
  {{- end }}
  {{- if $root.Values.identity.tokenSecret }}
  - { name: identity-tokens, mountPath: /etc/kronosdb/tokens, readOnly: true }
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


{{/* Non-empty when any identity-layer list is set. */}}
{{- define "kronosdb.identityEnabled" -}}
{{- if or .Values.identity.issuers .Values.identity.grants .Values.identity.tokens -}}true{{- end -}}
{{- end -}}

{{/* Pod annotations that roll the pods when boot-time config changes. */}}
{{- define "kronosdb.podAnnotations" -}}
{{- if and .Values.contexts .Values.rollOnManifestChange }}
checksum/manifest: {{ include (print $.Template.BasePath "/configmap-manifest.yaml") . | sha256sum }}
{{- end }}
{{- if include "kronosdb.identityEnabled" . }}
checksum/config: {{ include (print $.Template.BasePath "/configmap-config.yaml") . | sha256sum }}
{{- end }}
{{- end -}}

{{/* Pod volumes, shared by the voter and learner StatefulSets. */}}
{{- define "kronosdb.volumes" -}}
{{- if .Values.contexts }}
- name: manifest
  configMap:
    name: {{ include "kronosdb.fullname" . }}-manifest
{{- end }}
{{- if .Values.tls.secretName }}
- name: tls
  secret:
    secretName: {{ .Values.tls.secretName }}
{{- end }}
{{- if include "kronosdb.identityEnabled" . }}
- name: config
  configMap:
    name: {{ include "kronosdb.fullname" . }}-config
{{- end }}
{{- if .Values.identity.tokenSecret }}
- name: identity-tokens
  secret:
    secretName: {{ .Values.identity.tokenSecret }}
{{- end }}
{{- end -}}
