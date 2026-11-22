// Package classify turns the evidence collected about a terminated container or
// pod into a single verdict: what ended it and, when known, who caused it.
// It has no Kubernetes client dependencies so that it can be tested with plain
// values.
package classify

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Cause is the classified reason for a termination.
type Cause string

const (
	OOMKilled        Cause = "OOMKilled"
	LivenessProbe    Cause = "LivenessProbeFailed"
	StartupProbe     Cause = "StartupProbeFailed"
	ContainerExited  Cause = "ContainerExited"
	KubeletEviction  Cause = "EvictedByKubelet"
	APIEviction      Cause = "EvictedByAPI"
	Preempted        Cause = "Preempted"
	TaintEviction    Cause = "TaintEviction"
	NodeShutdown     Cause = "NodeShutdown"
	PodGC            Cause = "PodGarbageCollected"
	Rollout          Cause = "Rollout"
	ScaleDown        Cause = "ScaleDown"
	OwnerDeleted     Cause = "OwnerDeleted"
	Deleted          Cause = "Deleted"
	DeadlineExceeded Cause = "DeadlineExceeded"
	Unknown          Cause = "Unknown"
)

// Warning reports whether the cause is unexpected for the workload (as opposed
// to a planned change such as a rollout).
func (c Cause) Warning() bool {
	switch c {
	case Rollout, ScaleDown, Deleted, OwnerDeleted, PodGC:
		return false
	}
	return true
}

// Verdict is the result for one terminated container or pod.
type Verdict struct {
	Kind      string    `json:"kind"` // "container" or "pod"
	Namespace string    `json:"namespace"`
	Pod       string    `json:"pod"`
	UID       string    `json:"uid"`
	Container string    `json:"container,omitempty"`
	Node      string    `json:"node,omitempty"`
	Cause     Cause     `json:"cause"`
	Detail    string    `json:"detail"`
	Actor     string    `json:"actor,omitempty"`
	Inferred  bool      `json:"inferred,omitempty"`
	Evidence  []string  `json:"evidence"`
	Time      time.Time `json:"time"`
}

// Message is the one-line text used for the Kubernetes Event.
func (v Verdict) Message() string {
	m := v.Detail
	if v.Container != "" {
		m = "container " + v.Container + ": " + m
	}
	if v.Actor != "" {
		m += "; actor: " + v.Actor
	}
	if v.Inferred {
		m += " (inferred, no audit record)"
	}
	return m
}

// Event is a Kubernetes Event about the pod, reduced to what matters here.
type Event struct {
	Reason  string
	Message string
	Source  string
	Time    time.Time
}

// AuditRecord is one API request taken from the kube-apiserver audit log.
type AuditRecord struct {
	Verb        string
	Resource    string
	Subresource string
	Namespace   string
	Name        string
	User        string
	UserAgent   string
	Code        int32
	Time        time.Time
}

// Owner describes the workload that controls the pod at the time it ended.
type Owner struct {
	Kind     string // ReplicaSet, StatefulSet, DaemonSet, Job, ...
	Name     string
	Workload string // e.g. "Deployment/web" for a ReplicaSet owned by a Deployment
	// Outdated is true when the pod belongs to an old revision of the workload.
	Outdated bool
	// Revision is the workload's current revision (Deployments) when known.
	Revision string
	// Replicas is the desired replica count of the workload now, -1 if unknown.
	Replicas int32
	// Change is the last spec or scale change of the workload from the audit
	// log, if any.
	Change *AuditRecord
}

// ContainerInput is the evidence for one terminated container instance.
type ContainerInput struct {
	Pod         *corev1.Pod
	Container   string
	Terminated  corev1.ContainerStateTerminated
	MemoryLimit string
	Events      []Event
}

var signals = map[int32]string{129: "SIGHUP", 130: "SIGINT", 131: "SIGQUIT", 134: "SIGABRT", 137: "SIGKILL", 139: "SIGSEGV", 143: "SIGTERM"}

// Container classifies the end of one container instance that the pod
// survived (restart) or that failed on its own.
func Container(in ContainerInput) Verdict {
	t := in.Terminated
	v := base("container", in.Pod)
	v.Container = in.Container
	v.Time = t.FinishedAt.Time
	v.Evidence = append(v.Evidence, fmt.Sprintf("containerStatus: reason=%s exitCode=%d finishedAt=%s", t.Reason, t.ExitCode, t.FinishedAt.UTC().Format(time.RFC3339)))

	if t.Reason == "OOMKilled" {
		v.Cause = OOMKilled
		v.Detail = "killed by the kernel OOM killer"
		if in.MemoryLimit != "" {
			v.Detail += " (memory limit " + in.MemoryLimit + ")"
		} else {
			v.Detail += " (no memory limit set; node memory exhausted)"
		}
		return v
	}

	from := t.StartedAt.Add(-time.Second)
	to := t.FinishedAt.Add(30 * time.Second)
	for _, e := range in.Events {
		if e.Reason != "Killing" || e.Time.Before(from) || e.Time.After(to) {
			continue
		}
		if !strings.Contains(e.Message, "container "+in.Container+" ") && !strings.Contains(e.Message, "Container "+in.Container+" ") {
			continue
		}
		var cause Cause
		var probe string
		switch {
		case strings.Contains(e.Message, "failed liveness probe"):
			cause, probe = LivenessProbe, "Liveness"
		case strings.Contains(e.Message, "failed startup probe"):
			cause, probe = StartupProbe, "Startup"
		default:
			continue
		}
		v.Cause = cause
		v.Evidence = append(v.Evidence, "event Killing: "+e.Message)
		v.Detail = "restarted by the kubelet after the " + strings.ToLower(probe) + " probe failed"
		if u := lastProbeFailure(in.Events, probe, from, to); u != "" {
			v.Detail += ": " + u
			v.Evidence = append(v.Evidence, "event Unhealthy: "+u)
		}
		return v
	}

	v.Cause = ContainerExited
	v.Detail = fmt.Sprintf("process exited with code %d", t.ExitCode)
	if s, ok := signals[t.ExitCode]; ok {
		v.Detail += " (" + s + ")"
	}
	if t.Reason != "" && t.Reason != "Error" && t.Reason != "Completed" {
		v.Detail += ", reason " + t.Reason
	}
	if t.Message != "" {
		v.Detail += ": " + firstLine(t.Message)
	}
	return v
}

func lastProbeFailure(events []Event, probe string, from, to time.Time) string {
	msg := ""
	var at time.Time
	for _, e := range events {
		if e.Reason == "Unhealthy" && strings.HasPrefix(e.Message, probe+" probe failed") && !e.Time.Before(from) && !e.Time.After(to) && !e.Time.Before(at) {
			msg, at = firstLine(e.Message), e.Time
		}
	}
	return msg
}

// PodInput is the evidence for one pod that is being deleted or has failed.
type PodInput struct {
	Pod   *corev1.Pod
	Node  *corev1.Node
	Owner *Owner
	// Audit holds delete and eviction requests for this pod.
	Audit  []AuditRecord
	Events []Event
	// ResolvePod maps a pod UID to "namespace/name" (for preemptors).
	ResolvePod func(uid string) string
	Now        time.Time
}

const saPrefix = "system:serviceaccount:kube-system:"

var preemptRe = regexp.MustCompile(`Preempted by (?:pod )?(\S+) on node (\S+)`)

// Pod classifies why a pod ended.
func Pod(in PodInput) Verdict {
	p := in.Pod
	v := base("pod", p)
	v.Time = in.Now
	if p.DeletionTimestamp != nil {
		v.Time = requestTime(p)
	}
	cond := disruption(p)
	if cond != nil {
		v.Evidence = append(v.Evidence, fmt.Sprintf("condition DisruptionTarget: reason=%s message=%q", cond.Reason, cond.Message))
	}
	if p.Status.Reason != "" {
		v.Evidence = append(v.Evidence, fmt.Sprintf("status: phase=%s reason=%s message=%q", p.Status.Phase, p.Status.Reason, firstLine(p.Status.Message)))
	}
	evictRec := firstRecord(in.Audit, func(r AuditRecord) bool { return r.Subresource == "eviction" })
	delRec := firstRecord(in.Audit, func(r AuditRecord) bool {
		return r.Subresource == "" && (r.Verb == "delete" || r.Verb == "deletecollection") && !strings.HasPrefix(r.User, "system:node:")
	})
	preemptEv := findEvent(in.Events, "Preempted")
	taintEv := findEvent(in.Events, "TaintManagerEviction")

	reason := ""
	if cond != nil {
		reason = cond.Reason
	}
	switch {
	case reason == "PreemptionByScheduler" || preemptEv != nil:
		v.Cause = Preempted
		v.Detail = "preempted by the scheduler to make room for a higher-priority pod"
		if preemptEv != nil {
			v.Evidence = append(v.Evidence, "event Preempted: "+preemptEv.Message)
			if m := preemptRe.FindStringSubmatch(preemptEv.Message); m != nil {
				who := m[1]
				if in.ResolvePod != nil {
					if n := in.ResolvePod(who); n != "" {
						who = n
					}
				}
				if who != "a" {
					v.Detail = fmt.Sprintf("preempted by the scheduler for higher-priority pod %s on node %s", who, m[2])
				}
			}
		}
		if prio := p.Spec.PriorityClassName; prio != "" {
			v.Detail += " (own priority class " + prio + ")"
		}
	case reason == "EvictionByEvictionAPI" || evictRec != nil:
		v.Cause = APIEviction
		v.Detail = "evicted through the Eviction API"
		if in.Node != nil && in.Node.Spec.Unschedulable {
			v.Detail += " while node " + in.Node.Name + " was cordoned (node drain)"
		}
		if evictRec != nil {
			v.Actor = evictRec.User
			v.Evidence = append(v.Evidence, auditLine(*evictRec))
			if ua := agent(evictRec.UserAgent); ua != "" {
				v.Detail += " by " + ua
			}
		} else {
			v.Inferred = true
		}
	case reason == "DeletionByTaintManager" || taintEv != nil || (delRec != nil && delRec.User == saPrefix+"taint-eviction-controller"):
		v.Cause = TaintEviction
		v.Detail = "deleted by the taint eviction controller"
		if taints := noExecute(in.Node, p); taints != "" {
			v.Detail += ": node " + in.Node.Name + " has NoExecute taint " + taints + " that the pod does not tolerate"
		}
		if taintEv != nil {
			v.Evidence = append(v.Evidence, "event TaintManagerEviction: "+taintEv.Message)
		}
	case reason == "DeletionByPodGC" || (delRec != nil && delRec.User == saPrefix+"pod-garbage-collector"):
		v.Cause = PodGC
		v.Detail = "deleted by the pod garbage collector"
		if cond != nil && cond.Message != "" {
			v.Detail += ": " + firstLine(cond.Message)
		} else if p.Spec.NodeName != "" && in.Node == nil {
			v.Detail += ": node " + p.Spec.NodeName + " no longer exists"
		}
	case p.Status.Reason == "Evicted" || reason == "TerminationByKubelet":
		msg := firstLine(p.Status.Message)
		if msg == "" && cond != nil {
			msg = firstLine(cond.Message)
		}
		if strings.Contains(strings.ToLower(msg), "shutdown") || p.Status.Reason == "Terminated" || p.Status.Reason == "Shutdown" {
			v.Cause = NodeShutdown
			v.Detail = "terminated by the kubelet because node " + p.Spec.NodeName + " is shutting down"
		} else {
			v.Cause = KubeletEviction
			v.Detail = "evicted by the kubelet on node " + p.Spec.NodeName
			if msg != "" {
				v.Detail += ": " + msg
			}
		}
	case p.Status.Reason == "DeadlineExceeded":
		v.Cause = DeadlineExceeded
		v.Detail = "activeDeadlineSeconds reached"
		if p.Spec.ActiveDeadlineSeconds != nil {
			v.Detail = fmt.Sprintf("activeDeadlineSeconds (%d) reached", *p.Spec.ActiveDeadlineSeconds)
		}
	case delRec != nil:
		v.Evidence = append(v.Evidence, auditLine(*delRec))
		deleter := delRec.User
		switch strings.TrimPrefix(deleter, saPrefix) {
		case "replicaset-controller", "statefulset-controller", "daemon-set-controller":
			ownerVerdict(&v, in.Owner, deleter, false)
		case "job-controller":
			v.Cause = Deleted
			v.Detail = "deleted by the Job controller"
			if in.Owner != nil {
				v.Detail += " (" + in.Owner.Kind + " " + in.Owner.Name + ")"
			}
			v.Actor = deleter
		case "generic-garbage-collector":
			v.Cause = OwnerDeleted
			v.Detail = "deleted by the garbage collector because its owner was deleted"
			if o := ownerRef(p); o != "" {
				v.Detail = "deleted by the garbage collector because its owner " + o + " was deleted"
			}
			v.Actor = deleter
		default:
			v.Cause = Deleted
			v.Detail = "deleted through the API"
			if ua := agent(delRec.UserAgent); ua != "" {
				v.Detail += " by " + ua
			}
			if delRec.Verb == "deletecollection" {
				v.Detail += " (delete collection)"
			}
			v.Actor = deleter
		}
	case p.DeletionTimestamp != nil:
		if in.Owner != nil && (in.Owner.Kind == "ReplicaSet" || in.Owner.Kind == "StatefulSet" || in.Owner.Kind == "DaemonSet") {
			ownerVerdict(&v, in.Owner, "", true)
		} else {
			v.Cause = Deleted
			v.Detail = "deleted through the API by an unknown client"
			v.Inferred = true
		}
	default:
		v.Cause = Unknown
		v.Detail = fmt.Sprintf("pod phase %s", p.Status.Phase)
		if p.Status.Reason != "" {
			v.Detail += ", reason " + p.Status.Reason
		}
		if p.Status.Message != "" {
			v.Detail += ": " + firstLine(p.Status.Message)
		}
	}
	return v
}

func ownerVerdict(v *Verdict, o *Owner, deleter string, inferred bool) {
	v.Inferred = inferred
	if deleter != "" {
		v.Evidence = append(v.Evidence, "deleted by "+deleter)
	}
	if o == nil {
		v.Cause = Deleted
		v.Detail = "deleted by its controller"
		v.Actor = deleter
		return
	}
	name := o.Kind + " " + o.Name
	if o.Workload != "" {
		name = strings.Replace(o.Workload, "/", " ", 1)
	}
	if o.Outdated {
		v.Cause = Rollout
		v.Detail = "replaced during a rollout of " + name
		if o.Revision != "" {
			v.Detail += " (now at revision " + o.Revision + ")"
		}
	} else {
		v.Cause = ScaleDown
		v.Detail = "removed by a scale-down of " + name
		if o.Replicas >= 0 {
			v.Detail += fmt.Sprintf(" to %d replicas", o.Replicas)
		}
	}
	if o.Change != nil {
		v.Actor = o.Change.User
		v.Evidence = append(v.Evidence, auditLine(*o.Change))
		if ua := agent(o.Change.UserAgent); ua != "" && !strings.HasPrefix(o.Change.User, saPrefix) {
			v.Detail += " (change made with " + ua + ")"
		}
		if o.Change.User == saPrefix+"horizontal-pod-autoscaler" {
			v.Detail += " by the HorizontalPodAutoscaler"
		}
	}
}

func base(kind string, p *corev1.Pod) Verdict {
	return Verdict{Kind: kind, Namespace: p.Namespace, Pod: p.Name, UID: string(p.UID), Node: p.Spec.NodeName, Evidence: []string{}}
}

// requestTime is when the delete or eviction request was made: the deletion
// timestamp is set to request time plus the grace period.
func requestTime(p *corev1.Pod) time.Time {
	t := p.DeletionTimestamp.Time
	if p.DeletionGracePeriodSeconds != nil {
		t = t.Add(-time.Duration(*p.DeletionGracePeriodSeconds) * time.Second)
	}
	return t
}

// RequestTime is exported for the controller, which matches audit records
// against it.
func RequestTime(p *corev1.Pod) time.Time { return requestTime(p) }

func disruption(p *corev1.Pod) *corev1.PodCondition {
	for i := range p.Status.Conditions {
		c := &p.Status.Conditions[i]
		if c.Type == corev1.DisruptionTarget && c.Status == corev1.ConditionTrue {
			return c
		}
	}
	return nil
}

func firstRecord(rs []AuditRecord, ok func(AuditRecord) bool) *AuditRecord {
	sorted := append([]AuditRecord(nil), rs...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Time.Before(sorted[j].Time) })
	for i := range sorted {
		if sorted[i].Code >= 200 && sorted[i].Code < 300 && ok(sorted[i]) {
			return &sorted[i]
		}
	}
	return nil
}

func findEvent(es []Event, reason string) *Event {
	for i := len(es) - 1; i >= 0; i-- {
		if es[i].Reason == reason {
			return &es[i]
		}
	}
	return nil
}

func noExecute(n *corev1.Node, p *corev1.Pod) string {
	if n == nil {
		return ""
	}
	var out []string
	for _, t := range n.Spec.Taints {
		if t.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		tolerated := false
		for _, tol := range p.Spec.Tolerations {
			if tol.ToleratesTaint(&t) && tol.TolerationSeconds == nil {
				tolerated = true
			}
		}
		if !tolerated {
			s := t.Key
			if t.Value != "" {
				s += "=" + t.Value
			}
			out = append(out, s+":NoExecute")
		}
	}
	return strings.Join(out, ", ")
}

func ownerRef(p *corev1.Pod) string {
	for _, o := range p.OwnerReferences {
		return o.Kind + "/" + o.Name
	}
	return ""
}

// agent shortens a user agent to the client name, e.g. "kubectl/v1.35.0 (linux/amd64) kubernetes/abc" -> "kubectl".
func agent(ua string) string {
	if ua == "" {
		return ""
	}
	name := strings.Fields(ua)[0]
	if i := strings.Index(name, "/"); i > 0 {
		name = name[:i]
	}
	return name
}

func auditLine(r AuditRecord) string {
	res := r.Resource
	if r.Subresource != "" {
		res += "/" + r.Subresource
	}
	return fmt.Sprintf("audit: %s %s %s/%s by %s (%s) at %s, code %d", r.Verb, res, r.Namespace, r.Name, r.User, agent(r.UserAgent), r.Time.UTC().Format(time.RFC3339), r.Code)
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}
