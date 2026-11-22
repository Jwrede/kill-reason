// Package output publishes verdicts: as Kubernetes Events on the pod, as a JSON
// log line, as Prometheus counters and through a small HTTP API.
package output

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/Jwrede/kill-reason/internal/classify"
)

// Component is the event source name used by kill-reason.
const Component = "kill-reason"

// Recorder fans a verdict out to all outputs.
type Recorder struct {
	Client   kubernetes.Interface // nil disables Kubernetes Events
	Instance string

	mu      sync.Mutex
	recent  []classify.Verdict
	limit   int
	counts  map[[3]string]int64
	started time.Time
}

// NewRecorder keeps the last limit verdicts for the API.
func NewRecorder(client kubernetes.Interface, limit int) *Recorder {
	host, _ := os.Hostname()
	return &Recorder{Client: client, Instance: host, limit: limit, counts: map[[3]string]int64{}, started: time.Now()}
}

// Report implements controller.Sink.
func (r *Recorder) Report(ctx context.Context, v classify.Verdict) {
	r.mu.Lock()
	r.recent = append(r.recent, v)
	if len(r.recent) > r.limit {
		r.recent = r.recent[len(r.recent)-r.limit:]
	}
	r.counts[[3]string{v.Kind, v.Namespace, string(v.Cause)}]++
	r.mu.Unlock()

	b, _ := json.Marshal(v)
	fmt.Println(string(b))
	if r.Client != nil {
		if err := r.event(ctx, v); err != nil {
			slog.Warn("could not create event", "pod", v.Namespace+"/"+v.Pod, "err", err)
		}
	}
}

func (r *Recorder) event(ctx context.Context, v classify.Verdict) error {
	typ := corev1.EventTypeNormal
	if v.Cause.Warning() {
		typ = corev1.EventTypeWarning
	}
	now := metav1.NewTime(time.Now())
	msg := v.Message()
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{GenerateName: v.Pod + ".", Namespace: v.Namespace,
			Labels: map[string]string{"app.kubernetes.io/managed-by": Component}},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", APIVersion: "v1", Namespace: v.Namespace, Name: v.Pod, UID: types.UID(v.UID)},
		Reason:         string(v.Cause),
		Message:        msg,
		Type:           typ,
		Source:         corev1.EventSource{Component: Component, Host: v.Node},
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
	}
	if v.Container != "" {
		ev.InvolvedObject.FieldPath = "spec.containers{" + v.Container + "}"
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := r.Client.CoreV1().Events(v.Namespace).Create(ctx, ev, metav1.CreateOptions{})
	return err
}

// Recent returns stored verdicts, optionally filtered.
func (r *Recorder) Recent(ns, pod string) []classify.Verdict {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []classify.Verdict{}
	for _, v := range r.recent {
		if (ns == "" || v.Namespace == ns) && (pod == "" || v.Pod == pod) {
			out = append(out, v)
		}
	}
	return out
}

// ServeAPI handles GET /api/v1/terminations?namespace=&pod=.
func (r *Recorder) ServeAPI(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(r.Recent(req.URL.Query().Get("namespace"), req.URL.Query().Get("pod")))
}

// ServeMetrics writes the counters in the Prometheus text format.
func (r *Recorder) ServeMetrics(extra func() map[string]float64) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		r.mu.Lock()
		keys := make([][3]string, 0, len(r.counts))
		for k := range r.counts {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return strings.Join(keys[i][:], "\x00") < strings.Join(keys[j][:], "\x00") })
		var b strings.Builder
		b.WriteString("# HELP kill_reason_terminations_total Classified container and pod terminations.\n# TYPE kill_reason_terminations_total counter\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "kill_reason_terminations_total{kind=%q,namespace=%q,cause=%q} %d\n", k[0], k[1], k[2], r.counts[k])
		}
		r.mu.Unlock()
		if extra != nil {
			names := []string{}
			vals := extra()
			for n := range vals {
				names = append(names, n)
			}
			sort.Strings(names)
			for _, n := range names {
				fmt.Fprintf(&b, "# TYPE %s gauge\n%s %g\n", n, n, vals[n])
			}
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(b.String()))
	}
}
