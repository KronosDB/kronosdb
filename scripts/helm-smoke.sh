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
# Requires: docker, kind, helm, kubectl, curl. The kronos CLI ($KRONOS,
# default `kronos` on PATH) enables the data checks; without it the test
# still validates cluster formation and failover.

set -euo pipefail

IMAGE="${IMAGE:-kronosdb:smoke}"
CLUSTER="${KIND_CLUSTER:-kronosdb-smoke}"
RELEASE=kdb
NS=kronosdb-smoke

log() { printf '\n=== %s\n' "$*"; }

KRONOS="${KRONOS:-kronos}"
# A throwaway CLI profile pointing at the port-forward.
export KRONOS_CONFIG="$(mktemp -d)/kronos.toml"
kronos() { "$KRONOS" --profile smoke --context orders "$@"; }
have_cli() { command -v "$KRONOS" >/dev/null; }

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

if have_cli; then
  log "append events through the service"
  "$KRONOS" profile set smoke --endpoint http://127.0.0.1:55051 --token smoke-token >/dev/null
  kubectl -n "$NS" port-forward "svc/$RELEASE-kronosdb" 55051:50051 >/dev/null 2>&1 &
  PF=$!
  sleep 2
  for i in 1 2 3; do
    kronos events append --type SmokeTested --id "smoke-$i" --tag smoke=run >/dev/null
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

if have_cli; then
  log "events survived the failover"
  kubectl -n "$NS" port-forward "svc/$RELEASE-kronosdb" 55051:50051 >/dev/null 2>&1 &
  PF=$!
  sleep 2
  COUNT=$(kronos -o ndjson query '{"tags": {"smoke": "run"}}' | wc -l | tr -d ' ')
  kill $PF 2>/dev/null || true
  [ "$COUNT" -ge 3 ] || { echo "FAIL: expected 3 smoke events after failover, got $COUNT"; exit 1; }
  log "$COUNT smoke events — data intact"
fi

log "rolling restart (upgrade path: one pod at a time behind the PDB)"
kubectl -n "$NS" rollout restart statefulset "$RELEASE-kronosdb"
kubectl -n "$NS" rollout status statefulset "$RELEASE-kronosdb" --timeout=300s

log "SMOKE TEST PASSED"
