# kill-reason

kill-reason is a small Kubernetes controller that writes down why a container or pod ended and, when the API server's audit log can tell, who caused it. Each termination becomes one Kubernetes Event on the pod with a classified reason:

```
$ kubectl -n e2e get events --field-selector source=kill-reason
LAST SEEN   TYPE      REASON                OBJECT                MESSAGE
...         Warning   OOMKilled             pod/oom               container hog: killed by the kernel OOM killer (memory limit 24Mi)
...         Warning   LivenessProbeFailed   pod/liveness          container app: restarted by the kubelet after the liveness probe failed: Liveness probe failed: Get "http://10.244.1.4:8080/healthz": dial tcp 10.244.1.4:8080: connect: connection refused
...         Normal    Rollout               pod/web-6d8c7-2kq9x   replaced during a rollout of Deployment web (now at revision 2) (change made with kubectl); actor: alice
...         Warning   EvictedByAPI          pod/web-5f7b9-xq2lm   evicted through the Eviction API while node kill-reason-e2e-worker was cordoned (node drain) by kubectl; actor: bob
...         Warning   Preempted             pod/victim            preempted by the scheduler for higher-priority pod e2e/vip on node kill-reason-e2e-worker (own priority class e2e-low)
```

The same verdicts are printed as JSON lines, counted in `/metrics` and listed by `kill-reason explain`.

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
# edit the image in deploy/kill-reason.yaml first
kubectl apply -f deploy/kill-reason.yaml
kubectl -n kill-reason get service kill-reason -o jsonpath='{.spec.clusterIP}'
```

Audit webhook (self-managed control planes, for example kubeadm, kind, k3s, RKE2): copy `deploy/audit-policy.yaml` and `deploy/audit-webhook.yaml` to the control-plane nodes, replace `CLUSTER_IP` and `AUDIT_TOKEN`, and add to the kube-apiserver:

```
--audit-policy-file=/etc/kubernetes/audit/policy.yaml
--audit-webhook-config-file=/etc/kubernetes/audit/webhook.yaml
```

plus a hostPath mount of that directory. The kube-apiserver runs in the host network and does not resolve Service DNS names, so the webhook kubeconfig uses the ClusterIP; give the Service a fixed `clusterIP` if you prefer. `--settle` must be longer than `--audit-webhook-batch-max-wait` (default 30 s). If you already have an audit policy, merge the rules from `deploy/audit-policy.yaml` before your catch-all rule.

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
- `e2e/run.sh` (in CI on every push and weekly): creates a kind cluster, adds the audit webhook flags to the real kube-apiserver static pod, deploys kill-reason and checks the Event for each of these scenarios: OOM kill, liveness probe failure, exit code 3, `activeDeadlineSeconds`, kubelet eviction for an ephemeral-storage limit, `kubectl delete --as alice`, `kubectl scale --as alice`, `kubectl rollout restart --as alice`, background cascade delete of a Deployment, `kubectl drain --as bob`, a NoExecute taint, and scheduler preemption between two priority classes. It also checks `kill-reason explain` and `/metrics`.

## Limits

- Events expire with the cluster's event TTL (1 h by default). Keep the JSON log or the metrics if you need history.
- One replica; verdicts for terminations that happen while kill-reason is down are lost. Pods deleted during that time are not reported after a restart.
- Without the audit webhook there is no actor. `Rollout` versus `ScaleDown` is then inferred from revisions, and a manual `kubectl delete` of a controller-owned pod can be misread as a scale-down.
- The actor of a rollout or scale-down is the last spec or scale change within 10 minutes before the deletion. Two changes close together are not separated.
- DaemonSet rollouts, Job pod deletions and node-pressure evictions caused by memory or disk on the node are classified from the generic evidence; only the ephemeral-storage eviction is covered by the end-to-end test. `NodeShutdown` and `PodGarbageCollected` are covered by unit tests only.
- Init containers and ephemeral containers are not inspected.
- Not tested on managed control planes or with more than one kube-apiserver.

## License

MIT
