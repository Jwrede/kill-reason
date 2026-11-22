// Package controller watches pods, events, nodes and workloads, notices when a
// container or pod ends, waits briefly for late evidence (audit batches and
// kubelet events) and then classifies the termination.
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/Jwrede/kill-reason/internal/audit"
	"github.com/Jwrede/kill-reason/internal/classify"
)

// Sink receives verdicts.
type Sink interface {
	Report(ctx context.Context, v classify.Verdict)
}

// Options configure the controller.
type Options struct {
	// Settle is how long to wait after a termination before classifying it.
	// It must cover the audit webhook batch delay of the kube-apiserver.
	Settle time.Duration
	// Namespaces limits the watched namespaces; empty means all.
	Namespace string
}

// Controller is the termination watcher.
type Controller struct {
	client kubernetes.Interface
	audit  *audit.Store
	sink   Sink
	opts   Options

	nodes       corelisters.NodeLister
	replicasets appslisters.ReplicaSetLister
	deployments appslisters.DeploymentLister
	statefuls   appslisters.StatefulSetLister

	mu   sync.Mutex
	pods map[types.UID]*podState
	ctx  context.Context
}

type podState struct {
	pod      *corev1.Pod
	events   []classify.Event
	podDone  bool
	reported map[string]bool // container instance keys already reported
	gone     time.Time
}

// New builds a controller.
func New(client kubernetes.Interface, store *audit.Store, sink Sink, opts Options) *Controller {
	if opts.Settle <= 0 {
		opts.Settle = 40 * time.Second
	}
	return &Controller{client: client, audit: store, sink: sink, opts: opts, pods: map[types.UID]*podState{}}
}

// Run starts the informers and blocks until ctx is done.
func (c *Controller) Run(ctx context.Context) error {
	c.ctx = ctx
	var nsOpt []informers.SharedInformerOption
	if c.opts.Namespace != "" {
		nsOpt = append(nsOpt, informers.WithNamespace(c.opts.Namespace))
	}
	f := informers.NewSharedInformerFactoryWithOptions(c.client, 10*time.Minute, nsOpt...)
	cluster := informers.NewSharedInformerFactory(c.client, 10*time.Minute)

	podInf := f.Core().V1().Pods().Informer()
	evInf := f.Core().V1().Events().Informer()
	c.nodes = cluster.Core().V1().Nodes().Lister()
	c.replicasets = f.Apps().V1().ReplicaSets().Lister()
	c.deployments = f.Apps().V1().Deployments().Lister()
	c.statefuls = f.Apps().V1().StatefulSets().Lister()

	if _, err := podInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { c.podAdd(o.(*corev1.Pod)) },
		UpdateFunc: func(o, n any) { c.podUpdate(o.(*corev1.Pod), n.(*corev1.Pod)) },
		DeleteFunc: c.podDelete,
	}); err != nil {
		return err
	}
	if _, err := evInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { c.event(o.(*corev1.Event)) },
		UpdateFunc: func(_, n any) { c.event(n.(*corev1.Event)) },
	}); err != nil {
		return err
	}
	f.Start(ctx.Done())
	cluster.Start(ctx.Done())
	for t, ok := range f.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("cache sync failed for %v", t)
		}
	}
	for t, ok := range cluster.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("cache sync failed for %v", t)
		}
	}
	slog.Info("caches synced, watching terminations", "settle", c.opts.Settle.String())
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			c.gc()
		}
	}
}

func (c *Controller) state(p *corev1.Pod) *podState {
	s := c.pods[p.UID]
	if s == nil {
		s = &podState{reported: map[string]bool{}}
		c.pods[p.UID] = s
	}
	return s
}

func (c *Controller) podAdd(p *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state(p).pod = p
}

func (c *Controller) podUpdate(old, p *corev1.Pod) {
	c.mu.Lock()
	s := c.state(p)
	s.pod = p
	c.mu.Unlock()

	podEnding := p.DeletionTimestamp != nil || podFailedBySystem(p)
	oldStatus := map[string]corev1.ContainerStatus{}
	for _, cs := range old.Status.ContainerStatuses {
		oldStatus[cs.Name] = cs
	}
	for _, cs := range p.Status.ContainerStatuses {
		prev, ok := oldStatus[cs.Name]
		if !ok {
			continue
		}
		switch {
		case cs.RestartCount > prev.RestartCount && cs.LastTerminationState.Terminated != nil:
			c.scheduleContainer(p.UID, cs.Name, *cs.LastTerminationState.Terminated)
		case !podEnding && cs.State.Terminated != nil && prev.State.Terminated == nil &&
			(cs.State.Terminated.ExitCode != 0 || cs.State.Terminated.Reason == "OOMKilled"):
			c.scheduleContainer(p.UID, cs.Name, *cs.State.Terminated)
		}
	}
	if (p.DeletionTimestamp != nil && old.DeletionTimestamp == nil) ||
		(podFailedBySystem(p) && !podFailedBySystem(old)) {
		c.schedulePod(p.UID)
	}
}

// podFailedBySystem is true for pods the kubelet stopped without deleting them
// (eviction, node shutdown, active deadline).
func podFailedBySystem(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed && p.Status.Reason != ""
}

func (c *Controller) podDelete(o any) {
	p, ok := o.(*corev1.Pod)
	if !ok {
		t, ok := o.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		if p, ok = t.Obj.(*corev1.Pod); !ok {
			return
		}
	}
	c.mu.Lock()
	s := c.state(p)
	if s.pod == nil || s.pod.DeletionTimestamp == nil {
		s.pod = p
	}
	s.gone = time.Now()
	c.mu.Unlock()
	c.schedulePod(p.UID)
}

func (c *Controller) event(e *corev1.Event) {
	if e.InvolvedObject.Kind != "Pod" || e.Source.Component == "kill-reason" || e.ReportingController == "kill-reason" {
		return
	}
	switch e.Reason {
	case "Killing", "Unhealthy", "Preempted", "TaintManagerEviction", "Evicted":
	default:
		return
	}
	t := e.LastTimestamp.Time
	if t.IsZero() {
		t = e.EventTime.Time
	}
	if t.IsZero() {
		t = e.FirstTimestamp.Time
	}
	if e.Series != nil && !e.Series.LastObservedTime.IsZero() {
		t = e.Series.LastObservedTime.Time
	}
	src := e.Source.Component
	if src == "" {
		src = e.ReportingController
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.pods[e.InvolvedObject.UID]
	if s == nil {
		s = &podState{reported: map[string]bool{}}
		c.pods[e.InvolvedObject.UID] = s
	}
	s.events = append(s.events, classify.Event{Reason: e.Reason, Message: e.Message, Source: src, Time: t})
	if len(s.events) > 40 {
		s.events = s.events[len(s.events)-40:]
	}
}

func (c *Controller) scheduleContainer(uid types.UID, name string, t corev1.ContainerStateTerminated) {
	key := name + "/" + t.FinishedAt.UTC().Format(time.RFC3339) + "/" + t.ContainerID
	c.mu.Lock()
	s := c.pods[uid]
	if s == nil || s.reported[key] {
		c.mu.Unlock()
		return
	}
	s.reported[key] = true
	c.mu.Unlock()
	time.AfterFunc(c.opts.Settle, func() {
		c.mu.Lock()
		p := s.pod
		events := append([]classify.Event(nil), s.events...)
		c.mu.Unlock()
		v := classify.Container(classify.ContainerInput{Pod: p, Container: name, Terminated: t, MemoryLimit: memLimit(p, name), Events: events})
		c.sink.Report(c.ctx, v)
	})
}

func (c *Controller) schedulePod(uid types.UID) {
	c.mu.Lock()
	s := c.pods[uid]
	if s == nil || s.podDone {
		c.mu.Unlock()
		return
	}
	s.podDone = true
	c.mu.Unlock()
	time.AfterFunc(c.opts.Settle, func() { c.evaluatePod(s) })
}

func (c *Controller) evaluatePod(s *podState) {
	c.mu.Lock()
	p := s.pod.DeepCopy()
	events := append([]classify.Event(nil), s.events...)
	c.mu.Unlock()

	at := time.Now()
	if p.DeletionTimestamp != nil {
		at = classify.RequestTime(p)
	}
	in := classify.PodInput{
		Pod:        p,
		Events:     events,
		Audit:      c.audit.ForPod(p.Namespace, p.Name, at.Add(-15*time.Second), at.Add(15*time.Second)),
		ResolvePod: c.resolvePod,
		Now:        time.Now(),
	}
	if p.Spec.NodeName != "" {
		if n, err := c.nodes.Get(p.Spec.NodeName); err == nil {
			in.Node = n
		}
	}
	in.Owner = c.owner(p, at)
	c.sink.Report(c.ctx, classify.Pod(in))
}

func (c *Controller) resolvePod(uid string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.pods[types.UID(uid)]; s != nil && s.pod != nil {
		return s.pod.Namespace + "/" + s.pod.Name
	}
	return ""
}

// owner describes the controlling workload and whether the pod was outdated.
func (c *Controller) owner(p *corev1.Pod, at time.Time) *classify.Owner {
	ref := metav1.GetControllerOf(p)
	if ref == nil {
		return nil
	}
	o := &classify.Owner{Kind: ref.Kind, Name: ref.Name, Replicas: -1}
	window := func(resource, name string) *classify.AuditRecord {
		return c.audit.LastChange(resource, p.Namespace, name, at.Add(-10*time.Minute), at.Add(5*time.Second))
	}
	switch ref.Kind {
	case "ReplicaSet":
		rs, err := c.replicasets.ReplicaSets(p.Namespace).Get(ref.Name)
		if err != nil {
			return o
		}
		o.Replicas = derefOr(rs.Spec.Replicas, -1)
		o.Change = window("replicasets", rs.Name)
		dref := metav1.GetControllerOf(rs)
		if dref == nil || dref.Kind != "Deployment" {
			return o
		}
		o.Workload = "Deployment/" + dref.Name
		if d, err := c.deployments.Deployments(p.Namespace).Get(dref.Name); err == nil {
			o.Replicas = derefOr(d.Spec.Replicas, -1)
		}
		cur := c.currentRevision(p.Namespace, dref.UID)
		mine, _ := strconv.Atoi(rs.Annotations["deployment.kubernetes.io/revision"])
		if cur > 0 {
			o.Revision = strconv.Itoa(cur)
			o.Outdated = mine < cur
		}
		if ch := window("deployments", dref.Name); ch != nil {
			o.Change = ch
		}
	case "StatefulSet":
		sts, err := c.statefuls.StatefulSets(p.Namespace).Get(ref.Name)
		if err != nil {
			return o
		}
		o.Replicas = derefOr(sts.Spec.Replicas, -1)
		o.Outdated = sts.Status.UpdateRevision != "" && p.Labels[appsv1.ControllerRevisionHashLabelKey] != sts.Status.UpdateRevision
		o.Change = window("statefulsets", sts.Name)
	case "DaemonSet":
		o.Change = window("daemonsets", ref.Name)
	}
	return o
}

func (c *Controller) currentRevision(ns string, deployment types.UID) int {
	all, err := c.replicasets.ReplicaSets(ns).List(labels.Everything())
	if err != nil {
		return 0
	}
	max := 0
	for _, rs := range all {
		ref := metav1.GetControllerOf(rs)
		if ref == nil || ref.UID != deployment {
			continue
		}
		if r, err := strconv.Atoi(rs.Annotations["deployment.kubernetes.io/revision"]); err == nil && r > max {
			max = r
		}
	}
	return max
}

func (c *Controller) gc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	cut := time.Now().Add(-10 * time.Minute)
	for uid, s := range c.pods {
		if (!s.gone.IsZero() && s.gone.Before(cut)) || (s.pod == nil && len(s.events) > 0 && s.events[len(s.events)-1].Time.Before(cut)) {
			delete(c.pods, uid)
		}
	}
}

func memLimit(p *corev1.Pod, name string) string {
	if p == nil {
		return ""
	}
	for _, ct := range p.Spec.Containers {
		if ct.Name == name {
			if q, ok := ct.Resources.Limits[corev1.ResourceMemory]; ok {
				return q.String()
			}
		}
	}
	return ""
}

func derefOr(p *int32, d int32) int32 {
	if p == nil {
		return d
	}
	return *p
}
