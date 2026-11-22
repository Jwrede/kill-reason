// Package audit receives kube-apiserver audit events through the webhook
// backend and keeps the few records kill-reason needs: who deleted or evicted
// a pod, and who last changed the spec or scale of a workload.
package audit

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"

	"github.com/Jwrede/kill-reason/internal/classify"
)

// Store keeps audit records for a limited time.
type Store struct {
	mu       sync.Mutex
	pods     []classify.AuditRecord // pod delete, deletecollection, eviction
	changes  []classify.AuditRecord // workload update, patch, scale
	keep     time.Duration
	token    string
	received int64
	now      func() time.Time
}

// NewStore returns a store that keeps records for the given duration. If token
// is not empty, requests must carry "Authorization: Bearer <token>".
func NewStore(keep time.Duration, token string) *Store {
	return &Store{keep: keep, token: token, now: time.Now}
}

var workloads = map[string]bool{"deployments": true, "replicasets": true, "statefulsets": true, "daemonsets": true}

// Add records one audit event if it is relevant.
func (s *Store) Add(e auditv1.Event) {
	if e.Stage != auditv1.StageResponseComplete || e.ObjectRef == nil {
		return
	}
	o := e.ObjectRef
	r := classify.AuditRecord{
		Verb:        e.Verb,
		Resource:    o.Resource,
		Subresource: o.Subresource,
		Namespace:   o.Namespace,
		Name:        o.Name,
		User:        e.User.Username,
		UserAgent:   e.UserAgent,
		Time:        e.RequestReceivedTimestamp.Time,
	}
	if e.ResponseStatus != nil {
		r.Code = e.ResponseStatus.Code
	}
	if r.Code == 0 {
		r.Code = 200
	}
	if e.ImpersonatedUser != nil && e.ImpersonatedUser.Username != "" {
		r.User = e.ImpersonatedUser.Username + " (impersonated by " + e.User.Username + ")"
	}
	var into *[]classify.AuditRecord
	switch {
	case o.APIGroup == "" && o.Resource == "pods" && o.Subresource == "" && (e.Verb == "delete" || e.Verb == "deletecollection"):
		into = &s.pods
	case o.APIGroup == "" && o.Resource == "pods" && o.Subresource == "eviction" && e.Verb == "create":
		into = &s.pods
	case o.APIGroup == "apps" && workloads[o.Resource] && (o.Subresource == "" || o.Subresource == "scale") && (e.Verb == "update" || e.Verb == "patch"):
		if strings.HasPrefix(r.User, "system:serviceaccount:kube-system:") && r.User != "system:serviceaccount:kube-system:horizontal-pod-autoscaler" {
			return // controllers updating their own objects
		}
		into = &s.changes
	default:
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.received++
	*into = append(*into, r)
	s.gc()
}

func (s *Store) gc() {
	cut := s.now().Add(-s.keep)
	for _, l := range []*[]classify.AuditRecord{&s.pods, &s.changes} {
		i := 0
		for i < len(*l) && (*l)[i].Time.Before(cut) {
			i++
		}
		if i > 0 {
			*l = append((*l)[:0], (*l)[i:]...)
		}
	}
}

// ForPod returns delete and eviction records for the pod between from and to.
// A deletecollection in the namespace matches every pod in it.
func (s *Store) ForPod(ns, name string, from, to time.Time) []classify.AuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []classify.AuditRecord
	for _, r := range s.pods {
		if r.Namespace != ns || r.Time.Before(from) || r.Time.After(to) {
			continue
		}
		if r.Name == name || (r.Verb == "deletecollection" && r.Name == "") {
			out = append(out, r)
		}
	}
	return out
}

// LastChange returns the newest successful spec or scale change of the
// workload (resource is e.g. "deployments") made between from and to.
func (s *Store) LastChange(resource, ns, name string, from, to time.Time) *classify.AuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.changes) - 1; i >= 0; i-- {
		r := s.changes[i]
		if r.Resource == resource && r.Namespace == ns && r.Name == name && r.Code >= 200 && r.Code < 300 && !r.Time.Before(from) && !r.Time.After(to) {
			return &r
		}
	}
	return nil
}

// Received is the number of relevant records seen since start.
func (s *Store) Received() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received
}

// ServeHTTP accepts an audit.k8s.io/v1 EventList from the webhook backend.
func (s *Store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if s.token != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var list auditv1.EventList
	if err := json.Unmarshal(body, &list); err != nil {
		slog.Warn("audit: bad payload", "err", err)
		http.Error(w, "bad EventList: "+err.Error(), http.StatusBadRequest)
		return
	}
	for _, e := range list.Items {
		s.Add(e)
	}
	w.WriteHeader(http.StatusOK)
}
