package classify

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var t0 = time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

func pod(mod ...func(*corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "web-7d9f-abcde", UID: "u1"},
		Spec:       corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "app"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, m := range mod {
		m(p)
	}
	return p
}

func deleting(p *corev1.Pod) {
	dt := metav1.NewTime(t0.Add(30 * time.Second))
	g := int64(30)
	p.DeletionTimestamp, p.DeletionGracePeriodSeconds = &dt, &g
}

func cond(reason, msg string) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: reason, Message: msg})
	}
}

func term(reason string, code int32) corev1.ContainerStateTerminated {
	return corev1.ContainerStateTerminated{Reason: reason, ExitCode: code, StartedAt: metav1.NewTime(t0.Add(-time.Minute)), FinishedAt: metav1.NewTime(t0)}
}

func TestContainer(t *testing.T) {
	cases := []struct {
		name   string
		in     ContainerInput
		cause  Cause
		detail string
	}{
		{"oom with limit", ContainerInput{Pod: pod(), Container: "app", Terminated: term("OOMKilled", 137), MemoryLimit: "64Mi"}, OOMKilled, "memory limit 64Mi"},
		{"oom without limit", ContainerInput{Pod: pod(), Container: "app", Terminated: term("OOMKilled", 137)}, OOMKilled, "no memory limit"},
		{"liveness", ContainerInput{Pod: pod(), Container: "app", Terminated: term("Error", 137), Events: []Event{
			{Reason: "Unhealthy", Message: "Liveness probe failed: HTTP probe failed with statuscode: 500", Time: t0.Add(-20 * time.Second)},
			{Reason: "Unhealthy", Message: "Liveness probe failed: Get \"http://10.0.0.1:8080/healthz\": context deadline exceeded", Time: t0.Add(-10 * time.Second)},
			{Reason: "Killing", Message: "Container app failed liveness probe, will be restarted", Time: t0.Add(-10 * time.Second)},
		}}, LivenessProbe, "context deadline exceeded"},
		{"startup", ContainerInput{Pod: pod(), Container: "app", Terminated: term("Error", 143), Events: []Event{
			{Reason: "Killing", Message: "Container app failed startup probe, will be restarted", Time: t0},
		}}, StartupProbe, "startup probe failed"},
		{"probe event for another container", ContainerInput{Pod: pod(), Container: "app", Terminated: term("Error", 1), Events: []Event{
			{Reason: "Killing", Message: "Container sidecar failed liveness probe, will be restarted", Time: t0},
		}}, ContainerExited, "code 1"},
		{"old probe event outside window", ContainerInput{Pod: pod(), Container: "app", Terminated: term("Error", 137), Events: []Event{
			{Reason: "Killing", Message: "Container app failed liveness probe, will be restarted", Time: t0.Add(-time.Hour)},
		}}, ContainerExited, "SIGKILL"},
		{"exit with message", ContainerInput{Pod: pod(), Container: "app", Terminated: corev1.ContainerStateTerminated{ExitCode: 2, Reason: "Error", Message: "panic: boom\nstack", FinishedAt: metav1.NewTime(t0)}}, ContainerExited, "panic: boom"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := Container(c.in)
			if v.Cause != c.cause || !strings.Contains(v.Detail, c.detail) {
				t.Fatalf("got %s %q, want %s containing %q", v.Cause, v.Detail, c.cause, c.detail)
			}
		})
	}
}

func rec(verb, sub, user string, at time.Time) AuditRecord {
	return AuditRecord{Verb: verb, Resource: "pods", Subresource: sub, Namespace: "shop", Name: "web-7d9f-abcde", User: user, UserAgent: "kubectl/v1.35.0 (linux/amd64) kubernetes/66452049", Code: 200, Time: at}
}

func TestPod(t *testing.T) {
	cordoned := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Spec: corev1.NodeSpec{Unschedulable: true}}
	tainted := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Spec: corev1.NodeSpec{Taints: []corev1.Taint{
		{Key: "maintenance", Value: "now", Effect: corev1.TaintEffectNoExecute},
		{Key: "node.kubernetes.io/not-ready", Effect: corev1.TaintEffectNoExecute},
	}}}
	tolerating := func(p *corev1.Pod) {
		p.Spec.Tolerations = []corev1.Toleration{{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute}}
	}
	hpa := rec("patch", "scale", saPrefix+"horizontal-pod-autoscaler", t0.Add(-5*time.Second))
	hpa.Resource = "deployments"
	alice := rec("patch", "", "alice", t0.Add(-3*time.Second))
	alice.Resource = "deployments"

	cases := []struct {
		name   string
		in     PodInput
		cause  Cause
		detail string
		actor  string
	}{
		{"drain by bob", PodInput{Pod: pod(deleting, cond("EvictionByEvictionAPI", "Eviction API: evicting")), Node: cordoned,
			Audit: []AuditRecord{rec("create", "eviction", "bob", t0)}}, APIEviction, "node node-a was cordoned (node drain) by kubectl", "bob"},
		{"eviction without audit", PodInput{Pod: pod(deleting, cond("EvictionByEvictionAPI", ""))}, APIEviction, "Eviction API", ""},
		{"failed eviction is ignored, then delete", PodInput{Pod: pod(deleting), Audit: []AuditRecord{
			func() AuditRecord { r := rec("create", "eviction", "bob", t0); r.Code = 429; return r }(),
			rec("delete", "", "carol", t0.Add(time.Second)),
		}}, Deleted, "deleted through the API by kubectl", "carol"},
		{"kubelet delete only is not an actor", PodInput{Pod: pod(deleting), Audit: []AuditRecord{rec("delete", "", "system:node:node-a", t0)}}, Deleted, "unknown client", ""},
		{"preempted", PodInput{Pod: pod(deleting, cond("PreemptionByScheduler", "")), Events: []Event{{Reason: "Preempted", Message: "Preempted by pod 9f1c on node node-a"}},
			ResolvePod: func(uid string) string { return map[string]string{"9f1c": "batch/important-0"}[uid] }}, Preempted, "higher-priority pod batch/important-0 on node node-a", ""},
		{"taint", PodInput{Pod: pod(deleting, tolerating, cond("DeletionByTaintManager", "Taint manager: deleting due to NoExecute taint")), Node: tainted},
			TaintEviction, "maintenance=now:NoExecute that the pod does not tolerate", ""},
		{"kubelet eviction", PodInput{Pod: pod(func(p *corev1.Pod) {
			p.Status.Phase, p.Status.Reason = corev1.PodFailed, "Evicted"
			p.Status.Message = "Pod ephemeral local storage usage exceeds the total limit of containers 1Mi. "
		}, cond("TerminationByKubelet", "Pod was evicted"))}, KubeletEviction, "ephemeral local storage usage exceeds the total limit of containers 1Mi", ""},
		{"node shutdown", PodInput{Pod: pod(func(p *corev1.Pod) {
			p.Status.Phase, p.Status.Reason = corev1.PodFailed, "Terminated"
			p.Status.Message = "Pod was terminated in response to imminent node shutdown."
		}, cond("TerminationByKubelet", "Pod was terminated in response to imminent node shutdown."))}, NodeShutdown, "node-a is shutting down", ""},
		{"pod gc", PodInput{Pod: pod(deleting, cond("DeletionByPodGC", "PodGC: node no longer exists"))}, PodGC, "node no longer exists", ""},
		{"deadline", PodInput{Pod: pod(func(p *corev1.Pod) {
			s := int64(5)
			p.Spec.ActiveDeadlineSeconds = &s
			p.Status.Phase, p.Status.Reason = corev1.PodFailed, "DeadlineExceeded"
		})}, DeadlineExceeded, "(5) reached", ""},
		{"rollout by alice", PodInput{Pod: pod(deleting), Audit: []AuditRecord{rec("delete", "", saPrefix+"replicaset-controller", t0)},
			Owner: &Owner{Kind: "ReplicaSet", Name: "web-7d9f", Workload: "Deployment/web", Outdated: true, Revision: "3", Replicas: 2, Change: &alice}},
			Rollout, "rollout of Deployment web (now at revision 3) (change made with kubectl)", "alice"},
		{"hpa scale-down", PodInput{Pod: pod(deleting), Audit: []AuditRecord{rec("delete", "", saPrefix+"replicaset-controller", t0)},
			Owner: &Owner{Kind: "ReplicaSet", Name: "web-7d9f", Workload: "Deployment/web", Replicas: 1, Change: &hpa}},
			ScaleDown, "scale-down of Deployment web to 1 replicas by the HorizontalPodAutoscaler", saPrefix + "horizontal-pod-autoscaler"},
		{"scale-down inferred without audit", PodInput{Pod: pod(deleting), Owner: &Owner{Kind: "StatefulSet", Name: "db", Replicas: 1}},
			ScaleDown, "scale-down of StatefulSet db to 1 replicas", ""},
		{"owner deleted", PodInput{Pod: pod(deleting, func(p *corev1.Pod) {
			p.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-7d9f"}}
		}), Audit: []AuditRecord{rec("delete", "", saPrefix+"generic-garbage-collector", t0)}}, OwnerDeleted, "owner ReplicaSet/web-7d9f was deleted", saPrefix + "generic-garbage-collector"},
		{"impersonated delete", PodInput{Pod: pod(deleting), Audit: []AuditRecord{rec("delete", "", "alice (impersonated by kubernetes-admin)", t0)}},
			Deleted, "deleted through the API by kubectl", "alice (impersonated by kubernetes-admin)"},
		{"deletecollection", PodInput{Pod: pod(deleting), Audit: []AuditRecord{func() AuditRecord { r := rec("deletecollection", "", "dave", t0); r.Name = ""; return r }()}},
			Deleted, "(delete collection)", "dave"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := Pod(c.in)
			if v.Cause != c.cause || !strings.Contains(v.Detail, c.detail) || v.Actor != c.actor {
				t.Fatalf("got %s %q actor %q, want %s containing %q actor %q\nevidence: %v", v.Cause, v.Detail, v.Actor, c.cause, c.detail, c.actor, v.Evidence)
			}
		})
	}
}

func TestRequestTimeAndMessage(t *testing.T) {
	p := pod(deleting)
	if got := RequestTime(p); !got.Equal(t0) {
		t.Fatalf("request time %v, want %v", got, t0)
	}
	v := Verdict{Container: "app", Detail: "x", Actor: "bob", Inferred: true}
	if m := v.Message(); m != "container app: x; actor: bob (inferred, no audit record)" {
		t.Fatalf("message %q", m)
	}
	if Rollout.Warning() || !OOMKilled.Warning() {
		t.Fatal("warning classes wrong")
	}
}
