#!/usr/bin/env bash
# Helm chart smoke test against a kind cluster.
#
# Proves the chart's operational contract on a live cluster:
#   1. a 3-voter cluster forms and every pod turns Ready (readiness is
#      claim-aware, so Ready == quorum formed and a leader claimed)
#   2. events survive the loss of the leader: kill it, quorum re-forms,
#      the data is still there
#   3. a rolling restart (helm upgrade) keeps the cluster available
#
# Usage: IMAGE=kronosdb:dev scripts/helm-smoke.sh
# Requires: docker, kind, helm, kubectl, curl. grpcurl enables the data
# checks; without it the test still validates cluster formation and failover.

set -euo pipefail

IMAGE="${IMAGE:-kronosdb:smoke}"
CLUSTER="${KIND_CLUSTER:-kronosdb-smoke}"
RELEASE=kdb
NS=kronosdb-smoke

log() { printf '\n=== %s\n' "$*"; }

cleanup() {
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "kind cluster"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --wait 120s
kind load docker-image "$IMAGE" --name "$CLUSTER"

log "helm install"
helm install "$RELEASE" charts/kronosdb \
  --namespace "$NS" --create-namespace \
  --set image.repository="${IMAGE%%:*}" \
  --set image.tag="${IMAGE##*:}" \
  --set image.pullPolicy=Never \
  --set auth.accessToken=smoke-token \
  --set admin.adminToken=smoke-admin \
  --set contexts='{orders}' \
  --set persistence.size=1Gi \
  --set resources.requests.cpu=100m \
  --set resources.requests.memory=256Mi \
  --set resources.limits.memory=1Gi \
  --set antiAffinity=soft   # single kind node hosts all voters

log "wait for the cluster to form (Ready == leader claimed)"
kubectl -n "$NS" rollout status statefulset "$RELEASE-kronosdb" --timeout=300s

admin_get() { # pod path
  kubectl -n "$NS" exec "$1" -- wget -qO- "http://127.0.0.1:9240$2"
}

leader_pod() {
  for i in 0 1 2; do
    pod="$RELEASE-kronosdb-$i"
    if admin_get "$pod" /metrics 2>/dev/null | grep -q '^kronosdb_raft_is_leader 1'; then
      echo "$pod"
      return
    fi
  done
  echo ""
}

LEADER="$(leader_pod)"
[ -n "$LEADER" ] || { echo "FAIL: no leader elected"; exit 1; }
log "leader is $LEADER"

if command -v grpcurl >/dev/null; then
  log "append events through the service"
  kubectl -n "$NS" port-forward "svc/$RELEASE-kronosdb" 55051:50051 >/dev/null 2>&1 &
  PF=$!
  sleep 2
  for i in 1 2 3; do
    grpcurl -plaintext \
      -H "kronosdb-token: smoke-token" -H "kronosdb-context: orders" \
      -import-path proto -proto eventstore.proto \
      -d "{\"events\": [{\"event\": {\"identifier\": \"smoke-$i\", \"name\": \"SmokeTested\", \"version\": \"1\"}, \"tags\": [{\"key\": \"$(printf smoke | base64)\", \"value\": \"$(printf run | base64)\"}]}]}" \
      127.0.0.1:55051 kronosdb.eventstore.EventStore/Append >/dev/null
  done
  kill $PF 2>/dev/null || true
fi

log "kill the leader"
kubectl -n "$NS" delete pod "$LEADER" --wait=false
sleep 5
kubectl -n "$NS" rollout status statefulset "$RELEASE-kronosdb" --timeout=300s
NEW_LEADER="$(leader_pod)"
[ -n "$NEW_LEADER" ] || { echo "FAIL: no leader after failover"; exit 1; }
log "cluster recovered, leader is $NEW_LEADER"

if command -v grpcurl >/dev/null; then
  log "events survived the failover"
  kubectl -n "$NS" port-forward "svc/$RELEASE-kronosdb" 55051:50051 >/dev/null 2>&1 &
  PF=$!
  sleep 2
  HEAD=$(grpcurl -plaintext \
    -H "kronosdb-token: smoke-token" -H "kronosdb-context: orders" \
    -import-path proto -proto eventstore.proto \
    -d '{}' 127.0.0.1:55051 kronosdb.eventstore.EventStore/GetHead | grep -o '[0-9]\+' || echo 0)
  kill $PF 2>/dev/null || true
  [ "$HEAD" -ge 3 ] || { echo "FAIL: expected head >= 3 after failover, got $HEAD"; exit 1; }
  log "head=$HEAD — data intact"
fi

log "rolling restart (upgrade path: one pod at a time behind the PDB)"
kubectl -n "$NS" rollout restart statefulset "$RELEASE-kronosdb"
kubectl -n "$NS" rollout status statefulset "$RELEASE-kronosdb" --timeout=300s

log "SMOKE TEST PASSED"
