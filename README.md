# kill-reason

kill-reason is a small Kubernetes controller that writes down why a container or pod ended and, when the API server's audit log can tell, who caused it. Each termination becomes one Kubernetes Event on the pod with a classified reason:

```
$ kill-reason explain -n e2e        # from the end-to-end test in CI (kind, Kubernetes v1.35.0)
TIME                  NAMESPACE  POD                    CAUSE                DETAIL
2026-11-22T05:59:18Z  e2e        oom                    OOMKilled            container hog: killed by the kernel OOM killer (memory limit 24Mi)
2026-11-22T05:59:21Z  e2e        crash                  ContainerExited      container app: process exited with code 3
2026-11-22T05:59:25Z  e2e        deadline               DeadlineExceeded     activeDeadlineSeconds (8) reached
2026-11-22T05:59:26Z  e2e        liveness               LivenessProbeFailed  container app: restarted by the kubelet after the liveness probe failed: Liveness probe failed: Get "http://10.244.1.3:8080/healthz": dial tcp 10.244.1.3:8080: connect: connection refused
2026-11-22T05:59:34Z  e2e        disk                   EvictedByKubelet     evicted by the kubelet on node kill-reason-e2e-worker: Pod ephemeral local storage usage exceeds the total limit of containers 1Mi.
2026-11-22T05:59:43Z  e2e        web-676577cd5f-bbcqb   Deleted              deleted through the API by kubectl; actor: alice (impersonated by kubernetes-admin)
2026-11-22T05:59:52Z  e2e        web-676577cd5f-5m5xw   ScaleDown            removed by a scale-down of Deployment web to 1 replicas (change made with kubectl); actor: alice (impersonated by kubernetes-admin)
2026-11-22T06:00:03Z  e2e        web-676577cd5f-z2vbb   Rollout              replaced during a rollout of Deployment web (now at revision 2) (change made with kubectl); actor: alice (impersonated by kubernetes-admin)
2026-11-22T06:00:14Z  e2e        web2-6784fd68bf-2w7mz  OwnerDeleted         deleted by the garbage collector because its owner ReplicaSet/web2-6784fd68bf was deleted; actor: system:serviceaccount:kube-system:generic-garbage-collector
2026-11-22T06:00:24Z  e2e        web-7d5747cf5b-g5c5g   EvictedByAPI         evicted through the Eviction API while node kill-reason-e2e-worker was cordoned (node drain) by kubectl; actor: bob (impersonated by kubernetes-admin)
2026-11-22T06:00:35Z  e2e        taintme                TaintEviction        deleted by the taint eviction controller: node kill-reason-e2e-worker has NoExecute taint e2e=now:NoExecute that the pod does not tolerate
2026-11-22T06:00:47Z  e2e        victim                 Preempted            preempted by the scheduler for higher-priority pod e2e/vip on node kill-reason-e2e-worker (own priority class e2e-low)
```

The same text is the message of a Kubernetes Event on the pod (`kubectl get events --field-selector source=kill-reason`), and each verdict is also printed as a JSON line with its evidence and counted in `/metrics`.

## Why

Kubernetes spreads the reason for a termination over several places, and some of them disappear with the pod:

- the kubelet's `Killing` event says `Stopping container app` for every deletion, whether it came from a rollout, a scale-down, a drain or a person;
- `OOMKilled` and probe failures appear in the container status and in separate `Unhealthy` events, but not in the `Killing` event;
- evictions, preemptions and taint evictions set the `DisruptionTarget` condition, which is lost once the pod object is deleted;
- who issued a delete or eviction is only recorded in the kube-apiserver audit log.

The upstream issue [kubernetes/kubernetes#81723 "Give the proper reason for killing pods"](https://github.com/kubernetes/kubernetes/issues/81723) asks for this. It has been open since August 2019, has 328 reactions (321 thumbs up) and 91 comments (counted on 2026-11-22), most of them `/remove-lifecycle stale` to keep it from being closed. Comments in the thread ask for the kill reason in the pod events and report pods that were stopped with reason `Killing` "out of the sudden" without an explanation.

Existing tools cover part of it. [k8s-pod-restart-info-collector](https://github.com/airwallex/k8s-pod-restart-info-collector) (Airwallex) sends restart reasons, events and logs to Slack when a container restarts; it does not look at pod deletions, evictions or preemptions and does not use the audit log. kill-reason covers both container restarts and pod terminations and records the actor.

## What it classifies

| Cause | When | Main evidence |
|---|---|---|
| `OOMKilled` | container hit its memory limit | container status |
| `LivenessProbeFailed`, `StartupProbeFailed` | kubelet restarted the container after a probe failure | `Killing` and `Unhealthy` events |
| `ContainerExited` | process exited by itself (exit code and signal) | container status |
| `EvictedByKubelet` | node pressure or ephemeral-storage limit | pod status, `DisruptionTarget` |
| `NodeShutdown` | graceful node shutdown | `DisruptionTarget` |
| `DeadlineExceeded` | `activeDeadlineSeconds` reached | pod status |
| `EvictedByAPI` | Eviction API (drain, descheduler, autoscaler); notes a cordoned node | `DisruptionTarget`, audit `pods/eviction` |
| `Preempted` | scheduler preemption, with the preempting pod | `Preempted` event |
| `TaintEviction` | NoExecute taint the pod does not tolerate, with the taint | `DisruptionTarget`, node taints |
| `PodGarbageCollected` | pod GC, for example after the node was removed | `DisruptionTarget`, audit |
| `Rollout` | ReplicaSet/StatefulSet controller replaced an old revision | audit, ReplicaSet revisions, StatefulSet `updateRevision` |
| `ScaleDown` | controller removed a current pod after a scale change (kubectl, HPA) | audit `deployments/scale`, workload spec |
| `OwnerDeleted` | garbage collector deleted the pod after its owner was deleted | audit |
| `Deleted` | any other delete through the API, with user and client | audit |

For `Rollout` and `ScaleDown` the actor is whoever last changed the workload's spec or scale (for example the user who ran `kubectl rollout restart`, or the HPA service account), taken from the audit log; the controller that performed the delete is listed as evidence.

## How it works

- Informers watch pods, events, nodes, ReplicaSets, Deployments and StatefulSets.
- A container restart (rising `restartCount`), a failed container, a new `deletionTimestamp` or a pod that the kubelet failed (evicted, deadline, shutdown) starts a timer. After `--settle` (default 40 s) the pod's evidence is classified once.
- The kube-apiserver's audit webhook backend posts batches to `/audit`. kill-reason keeps pod `delete`, `deletecollection` and `pods/eviction` requests, and `update`/`patch` of Deployments, ReplicaSets, StatefulSets and DaemonSets (including `/scale`) for 15 minutes. Updates by kube-system controllers other than the HPA are ignored. The deletion request time is `deletionTimestamp - deletionGracePeriodSeconds`; records within 15 s of it match.
- Without the audit webhook, everything except the actor still works; deletions by workload controllers are then marked `(inferred, no audit record)`.

## Installation

Build the image (`docker build -t <registry>/kill-reason .`) or `go install github.com/Jwrede/kill-reason/cmd/kill-reason@latest`, then:

```sh
kubectl create namespace kill-reason
kubectl -n kill-reason create secret generic kill-reason-audit-token --from-literal=token="$(openssl rand -hex 24)"
# replace REGISTRY in deploy/kill-reason.yaml with your registry first
kubectl apply -f deploy/kill-reason.yaml
kubectl -n kill-reason get service kill-reason -o jsonpath='{.spec.clusterIP}'
```

Audit webhook (self-managed control planes, for example kubeadm, kind, k3s, RKE2): copy `deploy/audit-policy.yaml` and `deploy/audit-webhook.yaml` to the control-plane nodes, replace `CLUSTER_IP` and `AUDIT_TOKEN`, and add to the kube-apiserver:

```
--audit-policy-file=/etc/kubernetes/audit/policy.yaml
--audit-webhook-config-file=/etc/kubernetes/audit/webhook.yaml
```

plus a hostPath mount of that directory. The kube-apiserver runs in the host network and does not resolve Service DNS names, so the webhook kubeconfig uses the ClusterIP; give the Service a fixed `clusterIP` if you prefer. Over plain http the kube-apiserver did not send a bearer token in the end-to-end test (kill-reason answered 401), so the token is the last path segment of the webhook URL (`http://<ClusterIP>/audit/<token>`); a bearer header is accepted as well. The token ends up in the webhook kubeconfig on the control-plane nodes, and the traffic between kube-apiserver and kill-reason is not encrypted. `--settle` must be longer than `--audit-webhook-batch-max-wait` (default 30 s). If you already have an audit policy, merge the rules from `deploy/audit-policy.yaml` before your catch-all rule.

Managed control planes (EKS, GKE, AKS) do not let you add an audit webhook; there kill-reason runs without actors.

## Usage

```sh
kill-reason explain -n shop              # all recorded terminations in a namespace
kill-reason explain -n shop web-7d9f-x   # one pod (also after it was deleted)
kill-reason explain -A
curl http://<service>/api/v1/terminations?namespace=shop
curl http://<service>/metrics            # kill_reason_terminations_total{kind,namespace,cause}
```

Flags of `serve`: `--listen` (default `:8080`), `--settle`, `--namespace` (watch one namespace), `--audit-token-file`, `--audit-retention`, `--no-events`, `--kubeconfig`.

## Tests

- `go test -race ./...`: classification table tests for every cause, the audit webhook handler with a real `audit.k8s.io/v1` EventList payload (token check, impersonation, ignored controller updates, retention), and a controller test against the client-go fake clientset (OOM restart, then a rollout delete attributed to the user who patched the Deployment, no duplicate verdicts).
- `e2e/run.sh` (in CI on every push and weekly; last run with kind v0.31.0 and Kubernetes v1.35.0): creates a kind cluster, adds the audit webhook flags to the real kube-apiserver static pod, deploys kill-reason and checks the Event for each of these scenarios: OOM kill, liveness probe failure, exit code 3, `activeDeadlineSeconds`, kubelet eviction for an ephemeral-storage limit, `kubectl delete --as alice`, `kubectl scale --as alice`, `kubectl rollout restart --as alice`, background cascade delete of a Deployment, `kubectl drain --as bob`, a NoExecute taint, and scheduler preemption between two priority classes. It also checks `kill-reason explain` and `/metrics`.

## Limits

- Events expire with the cluster's event TTL (1 h by default). Keep the JSON log or the metrics if you need history.
- One replica; verdicts for terminations that happen while kill-reason is down are lost. Pods deleted during that time are not reported after a restart.
- Without the audit webhook there is no actor. `Rollout` versus `ScaleDown` is then inferred from revisions, and a manual `kubectl delete` of a controller-owned pod can be misread as a scale-down.
- The actor of a rollout or scale-down is the last spec or scale change within 10 minutes before the deletion. Two changes close together are not separated.
- DaemonSet rollouts, Job pod deletions and node-pressure evictions caused by memory or disk on the node are classified from the generic evidence; only the ephemeral-storage eviction is covered by the end-to-end test. `NodeShutdown` and `PodGarbageCollected` are covered by unit tests only.
- Init containers and ephemeral containers are not inspected.
- A container that the kernel OOM killer hits in a child process can be reported as `ContainerExited` with exit code 137 when the runtime does not mark it `OOMKilled`; this happened once in the e2e runs with `stress`.
- No published image; build it from the Dockerfile.
- Not tested on managed control planes or with more than one kube-apiserver.

## License

MIT
