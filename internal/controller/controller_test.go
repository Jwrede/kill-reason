package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	authnv1 "k8s.io/api/authentication/v1"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"

	"github.com/Jwrede/kill-reason/internal/audit"
	"github.com/Jwrede/kill-reason/internal/classify"
)

type sink struct {
	mu sync.Mutex
	vs []classify.Verdict
}

func (s *sink) Report(_ context.Context, v classify.Verdict) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vs = append(s.vs, v)
}

func (s *sink) wait(t *testing.T, n int) []classify.Verdict {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		s.mu.Lock()
		if len(s.vs) >= n {
			out := append([]classify.Verdict(nil), s.vs...)
			s.mu.Unlock()
			return out
		}
		s.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d verdicts", n)
	return nil
}

func ptr[T any](v T) *T { return &v }

func TestRestartAndRollout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "web", UID: "d1"}, Spec: appsv1.DeploymentSpec{Replicas: ptr(int32(2))}}
	ctrl := true
	rs := func(name, rev string) *appsv1.ReplicaSet {
		return &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: name, UID: types.UID("rs-x" + name),
			Annotations:     map[string]string{"deployment.kubernetes.io/revision": rev},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: "d1", Controller: &ctrl}}},
			Spec: appsv1.ReplicaSetSpec{Replicas: ptr(int32(2))}}
	}
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "web-old-1", UID: "p1",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-old", UID: "rs-xweb-old", Controller: &ctrl}}},
		Spec: corev1.PodSpec{NodeName: "n1", Containers: []corev1.Container{{Name: "app"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
			{Name: "app", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}},
	}
	cs := fake.NewSimpleClientset(dep, rs("web-old", "1"), rs("web-new", "2"), p, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}})
	store := audit.NewStore(time.Hour, "")
	out := &sink{}
	c := New(cs, store, out, Options{Settle: 100 * time.Millisecond})
	go func() { _ = c.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)

	// Container restart after OOM.
	now := metav1.Now()
	p2 := p.DeepCopy()
	p2.Status.ContainerStatuses[0].RestartCount = 1
	p2.Status.ContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137, FinishedAt: now, ContainerID: "containerd://a"}
	if _, err := cs.CoreV1().Pods("shop").UpdateStatus(ctx, p2, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	vs := out.wait(t, 1)
	if vs[0].Kind != "container" || vs[0].Cause != classify.OOMKilled || vs[0].Container != "app" {
		t.Fatalf("container verdict %+v", vs[0])
	}

	// The ReplicaSet controller deletes the outdated pod during a rollout.
	p3 := p2.DeepCopy()
	dt := metav1.NewTime(time.Now().Add(30 * time.Second))
	p3.DeletionTimestamp, p3.DeletionGracePeriodSeconds = &dt, ptr(int64(30))
	store.Add(auditEvent("delete", "pods", "", "web-old-1", "system:serviceaccount:kube-system:replicaset-controller"))
	store.Add(auditEvent("patch", "deployments", "apps", "web", "alice"))
	if _, err := cs.CoreV1().Pods("shop").Update(ctx, p3, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	vs = out.wait(t, 2)
	v := vs[1]
	if v.Kind != "pod" || v.Cause != classify.Rollout || v.Actor != "alice" || v.Detail != "replaced during a rollout of Deployment web (now at revision 2) (change made with kubectl)" {
		t.Fatalf("pod verdict %+v", v)
	}
	// A second update and the final delete do not produce another verdict.
	if err := cs.CoreV1().Pods("shop").Delete(ctx, "web-old-1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if n := len(out.wait(t, 2)); n != 2 {
		t.Fatalf("%d verdicts, want 2", n)
	}
}

func auditEvent(verb, resource, group, name, user string) auditv1.Event {
	return auditv1.Event{
		Stage:                    auditv1.StageResponseComplete,
		Verb:                     verb,
		User:                     authnv1.UserInfo{Username: user},
		UserAgent:                "kubectl/v1.35.0 (linux/amd64) kubernetes/abc",
		ObjectRef:                &auditv1.ObjectReference{Resource: resource, APIGroup: group, Namespace: "shop", Name: name},
		ResponseStatus:           &metav1.Status{Code: 200},
		RequestReceivedTimestamp: metav1.NewMicroTime(time.Now()),
	}
}
