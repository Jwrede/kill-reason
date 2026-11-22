#!/usr/bin/env bash
# End-to-end test: a kind cluster whose kube-apiserver sends audit events to
# kill-reason, then one scenario per termination cause. Every check reads the
# Kubernetes Events that kill-reason created, as a user would.
# Needs docker, kind, kubectl, jq, python3, go. KEEP_CLUSTER=1 keeps the cluster.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=${CLUSTER:-kill-reason-e2e}
NODE_IMAGE=${NODE_IMAGE:-}
SVC_IP=10.96.123.123
TOKEN=e2e-$(date +%s)-$RANDOM
NS=e2e
CP=$CLUSTER-control-plane
WORKER=$CLUSTER-worker

log() { printf '\n=== %s\n' "$*"; }
dump() {
  set +e
  kubectl -n kill-reason describe pods | tail -40
  kubectl -n kill-reason logs deploy/kill-reason --tail=200
  kubectl get events -A --field-selector source=kill-reason
  kubectl get pods -A -o wide
  echo "--- from the control-plane node to $SVC_IP"
  docker exec "$CP" curl -sS -m 5 "http://$SVC_IP/metrics"
  echo "--- kube-apiserver log lines about audit"
  docker exec "$CP" sh -c 'crictl logs $(crictl ps -q --name kube-apiserver) 2>&1 | grep -i audit | tail -20'
}
fail() { echo "FAIL: $*" >&2; dump; exit 1; }
cleanup() { [ "${KEEP_CLUSTER:-}" = 1 ] || kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

wait_for() {
  local t=$1 what=$2; shift 2
  local end=$((SECONDS + t))
  until "$@" >/dev/null 2>&1; do
    [ $SECONDS -lt $end ] || fail "timeout after ${t}s: $what"
    sleep 3
  done
  echo "ok: $what"
}

# has_verdict <pod-prefix> <cause> <substring>: kill-reason created a matching Event.
has_verdict() {
  kubectl -n "$NS" get events --field-selector source=kill-reason -o json |
    jq -e --arg p "$1" --arg r "$2" --arg s "$3" \
      '[.items[] | select((.involvedObject.name | startswith($p)) and .reason == $r and (.message | contains($s)))] | length > 0'
}
expect() {
  wait_for "${4:-120}" "$1: $2 ($3)" has_verdict "$1" "$2" "$3"
  kubectl -n "$NS" get events --field-selector source=kill-reason -o json |
    jq -r --arg p "$1" --arg r "$2" '.items[] | select((.involvedObject.name | startswith($p)) and .reason == $r) | "    " + .involvedObject.name + "  " + .reason + "  " + .message' | head -3
}

log "cluster"
cat > /tmp/kind-$CLUSTER.yaml <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
EOF
kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --config /tmp/kind-$CLUSTER.yaml ${NODE_IMAGE:+--image "$NODE_IMAGE"} --wait 120s
kubectl version

log "audit webhook on the kube-apiserver (edits the static pod manifest)"
mkdir -p /tmp/$CLUSTER-audit
cp deploy/audit-policy.yaml /tmp/$CLUSTER-audit/policy.yaml
sed -e "s#CLUSTER_IP#$SVC_IP#" -e "s#AUDIT_TOKEN#$TOKEN#" deploy/audit-webhook.yaml > /tmp/$CLUSTER-audit/webhook.yaml
docker exec "$CP" mkdir -p /etc/kubernetes/audit
docker cp /tmp/$CLUSTER-audit/policy.yaml "$CP":/etc/kubernetes/audit/policy.yaml
docker cp /tmp/$CLUSTER-audit/webhook.yaml "$CP":/etc/kubernetes/audit/webhook.yaml
if ! docker exec "$CP" grep -q audit-webhook-config-file /etc/kubernetes/manifests/kube-apiserver.yaml; then
  docker cp "$CP":/etc/kubernetes/manifests/kube-apiserver.yaml /tmp/$CLUSTER-audit/apiserver.yaml
  python3 e2e/patch-apiserver.py /tmp/$CLUSTER-audit/apiserver.yaml
  # Move the manifest out and back in so the kubelet restarts the static pod.
  docker exec "$CP" mv /etc/kubernetes/manifests/kube-apiserver.yaml /etc/kubernetes/kube-apiserver.yaml.orig
  sleep 10
  docker cp /tmp/$CLUSTER-audit/apiserver.yaml "$CP":/etc/kubernetes/manifests/kube-apiserver.yaml
fi
wait_for 180 "kube-apiserver back with audit flags" sh -c "kubectl get --raw /readyz && kubectl -n kube-system get pod kube-apiserver-$CP -o yaml | grep -q audit-webhook-config-file"

log "kill-reason"
docker build -q -t kill-reason:e2e .
kind load docker-image kill-reason:e2e --name "$CLUSTER"
# The kube-apiserver runs in the host network and cannot resolve Service names,
# so the Service gets a fixed ClusterIP that the audit kubeconfig points to.
# kill-reason runs on the control-plane node so that drain and taint tests on
# the worker do not touch it.
python3 e2e/e2e-manifest.py "$SVC_IP" < deploy/kill-reason.yaml > /tmp/$CLUSTER-kill-reason.yaml
kubectl create namespace kill-reason --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kill-reason create secret generic kill-reason-audit-token --from-literal=token="$TOKEN" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f /tmp/$CLUSTER-kill-reason.yaml
kubectl -n kill-reason rollout status deploy/kill-reason --timeout=180s || fail "kill-reason did not become ready"
# Read the counter from the control-plane node, the same network path the
# kube-apiserver uses for the audit webhook.
audit_received() {
  docker exec "$CP" curl -sS -m 5 "http://$SVC_IP/metrics" | grep -E '^kill_reason_audit_records_received [1-9]'
}

log "test users: alice may edit namespace $NS, bob is cluster-admin (for drain)"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create rolebinding alice-edit --clusterrole=edit --user=alice --dry-run=client -o yaml | kubectl apply -f -
kubectl create clusterrolebinding bob-admin --clusterrole=cluster-admin --user=bob --dry-run=client -o yaml | kubectl apply -f -

log "scenarios that run on their own"
kubectl -n "$NS" apply -f e2e/workloads.yaml
expect oom OOMKilled "memory limit 24Mi" 180
expect liveness LivenessProbeFailed "liveness probe failed: Liveness probe failed" 180
expect deadline DeadlineExceeded "activeDeadlineSeconds (8) reached" 180
expect disk EvictedByKubelet "ephemeral local storage usage exceeds the total limit" 240
expect crash ContainerExited "process exited with code 3" 180

log "audit records arrive"
kubectl -n "$NS" rollout status deploy/web --timeout=120s
kubectl -n "$NS" rollout status deploy/web2 --timeout=120s
POD=$(kubectl -n "$NS" get pods -l app=web -o jsonpath='{.items[0].metadata.name}')
kubectl -n "$NS" delete pod "$POD" --as alice --wait=false
wait_for 60 "kill-reason received audit records" audit_received
expect "$POD" Deleted "deleted through the API by kubectl; actor: alice (impersonated by"

log "scale-down and rollout by alice"
kubectl -n "$NS" rollout status deploy/web --timeout=120s
kubectl -n "$NS" scale deploy/web --replicas=1 --as alice
expect web- ScaleDown "removed by a scale-down of Deployment web to 1 replicas (change made with kubectl); actor: alice (impersonated by"
kubectl -n "$NS" rollout status deploy/web --timeout=120s
kubectl -n "$NS" rollout restart deploy/web --as alice
expect web- Rollout "replaced during a rollout of Deployment web (now at revision 2) (change made with kubectl); actor: alice (impersonated by"
kubectl -n "$NS" rollout status deploy/web --timeout=120s

log "owner deleted (background cascade)"
kubectl -n "$NS" delete deploy/web2 --as alice --cascade=background --wait=false
expect web2- OwnerDeleted "deleted by the garbage collector because its owner ReplicaSet/web2-"

log "drain by bob"
kubectl drain "$WORKER" --as bob --ignore-daemonsets --delete-emptydir-data --force --timeout=180s --grace-period=5
expect web- EvictedByAPI "evicted through the Eviction API while node $WORKER was cordoned (node drain) by kubectl; actor: bob"
kubectl uncordon "$WORKER"
kubectl -n "$NS" rollout status deploy/web --timeout=180s

log "NoExecute taint"
kubectl -n "$NS" run taintme --image=busybox:1.37 --restart=Never --overrides='{"spec":{"terminationGracePeriodSeconds":1}}' -- sleep 3600
kubectl -n "$NS" wait --for=condition=Ready pod/taintme --timeout=120s
kubectl taint node "$WORKER" e2e=now:NoExecute
expect taintme TaintEviction "node $WORKER has NoExecute taint e2e=now:NoExecute that the pod does not tolerate"
kubectl taint node "$WORKER" e2e=now:NoExecute-
kubectl -n "$NS" rollout status deploy/web --timeout=180s

log "preemption"
ALLOC=$(kubectl get node "$WORKER" -o jsonpath='{.status.allocatable.cpu}')
case "$ALLOC" in *m) MILLI=${ALLOC%m} ;; *) MILLI=$((ALLOC * 1000)) ;; esac
REQ=$((MILLI * 6 / 10))m
echo "worker allocatable cpu $ALLOC, each pod requests $REQ"
sed "s/CPU_REQUEST/$REQ/g; s/WORKER/$WORKER/g" e2e/preemption.yaml | kubectl -n "$NS" apply -f -
kubectl -n "$NS" wait --for=condition=Ready pod/victim --timeout=120s
sed "s/CPU_REQUEST/$REQ/g; s/WORKER/$WORKER/g" e2e/preemption-vip.yaml | kubectl -n "$NS" apply -f -
expect victim Preempted "preempted by the scheduler for higher-priority pod $NS/vip on node $WORKER" 180

log "explain CLI"
go build -o /tmp/kill-reason-cli ./cmd/kill-reason
/tmp/kill-reason-cli explain -n "$NS" | tee /tmp/$CLUSTER-explain.txt
for c in OOMKilled LivenessProbeFailed DeadlineExceeded EvictedByKubelet ContainerExited Deleted ScaleDown Rollout OwnerDeleted EvictedByAPI TaintEviction Preempted; do
  grep -q " $c " /tmp/$CLUSTER-explain.txt || fail "explain output lacks $c"
done
/tmp/kill-reason-cli explain -n "$NS" victim | grep -q Preempted || fail "explain <pod> filter"
echo "ok: explain lists all causes"

log "metrics"
kubectl get --raw "/api/v1/namespaces/kill-reason/services/http:kill-reason:80/proxy/metrics" | grep kill_reason_
echo
echo "PASS"
