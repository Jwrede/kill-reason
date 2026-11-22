package audit

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A webhook batch as the kube-apiserver sends it (trimmed real payload shape).
const batch = `{"kind":"EventList","apiVersion":"audit.k8s.io/v1","metadata":{},"items":[
{"level":"Metadata","auditID":"1","stage":"ResponseComplete","requestURI":"/api/v1/namespaces/shop/pods/web-1/eviction","verb":"create",
 "user":{"username":"bob"},"userAgent":"kubectl/v1.35.0 (linux/amd64)","objectRef":{"resource":"pods","namespace":"shop","name":"web-1","apiVersion":"v1","subresource":"eviction"},
 "responseStatus":{"metadata":{},"code":201},"requestReceivedTimestamp":"2026-11-22T10:00:00.000000Z","stageTimestamp":"2026-11-22T10:00:00.010000Z"},
{"level":"Metadata","auditID":"2","stage":"ResponseStarted","verb":"delete","user":{"username":"carol"},"objectRef":{"resource":"pods","namespace":"shop","name":"web-2","apiVersion":"v1"},
 "requestReceivedTimestamp":"2026-11-22T10:00:01.000000Z","stageTimestamp":"2026-11-22T10:00:01.000000Z"},
{"level":"Metadata","auditID":"3","stage":"ResponseComplete","verb":"delete","user":{"username":"kubernetes-admin"},"impersonatedUser":{"username":"alice"},
 "objectRef":{"resource":"pods","namespace":"shop","name":"web-2","apiVersion":"v1"},"responseStatus":{"metadata":{},"code":200},
 "requestReceivedTimestamp":"2026-11-22T10:00:02.000000Z","stageTimestamp":"2026-11-22T10:00:02.000000Z"},
{"level":"Metadata","auditID":"4","stage":"ResponseComplete","verb":"patch","user":{"username":"alice"},
 "objectRef":{"resource":"deployments","namespace":"shop","name":"web","apiGroup":"apps","apiVersion":"v1"},"responseStatus":{"metadata":{},"code":200},
 "requestReceivedTimestamp":"2026-11-22T09:59:50.000000Z","stageTimestamp":"2026-11-22T09:59:50.000000Z"},
{"level":"Metadata","auditID":"5","stage":"ResponseComplete","verb":"update","user":{"username":"system:serviceaccount:kube-system:deployment-controller"},
 "objectRef":{"resource":"deployments","namespace":"shop","name":"web","apiGroup":"apps","apiVersion":"v1"},"responseStatus":{"metadata":{},"code":200},
 "requestReceivedTimestamp":"2026-11-22T09:59:55.000000Z","stageTimestamp":"2026-11-22T09:59:55.000000Z"},
{"level":"Metadata","auditID":"6","stage":"ResponseComplete","verb":"get","user":{"username":"alice"},"objectRef":{"resource":"pods","namespace":"shop","name":"web-1","apiVersion":"v1"},
 "responseStatus":{"metadata":{},"code":200},"requestReceivedTimestamp":"2026-11-22T10:00:00.000000Z","stageTimestamp":"2026-11-22T10:00:00.000000Z"}
]}`

func TestWebhook(t *testing.T) {
	s := NewStore(time.Hour, "secret")
	s.now = func() time.Time { return time.Date(2026, 11, 22, 10, 1, 0, 0, time.UTC) }

	for _, tc := range []struct {
		path, auth string
		code       int
	}{{"/audit", "", 401}, {"/audit", "Bearer wrong", 401}, {"/audit/wrong", "", 401}, {"/audit/", "", 401}, {"/audit/secret", "", 200}, {"/audit", "Bearer secret", 200}} {
		req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(batch))
		if tc.auth != "" {
			req.Header.Set("Authorization", tc.auth)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		if w.Code != tc.code {
			t.Fatalf("%s auth %q: code %d, want %d", tc.path, tc.auth, w.Code, tc.code)
		}
	}
	if got := s.Received(); got != 6 {
		t.Fatalf("received %d relevant records, want 6 (eviction, impersonated delete, alice patch, twice)", got)
	}
	from, to := time.Date(2026, 11, 22, 9, 59, 0, 0, time.UTC), time.Date(2026, 11, 22, 10, 1, 0, 0, time.UTC)
	ev := s.ForPod("shop", "web-1", from, to)
	if len(ev) != 2 || ev[0].User != "bob" || ev[0].Subresource != "eviction" || ev[0].Code != 201 {
		t.Fatalf("web-1 records: %+v", ev)
	}
	del := s.ForPod("shop", "web-2", from, to)
	if len(del) != 2 || del[0].User != "alice (impersonated by kubernetes-admin)" {
		t.Fatalf("web-2 records: %+v", del)
	}
	ch := s.LastChange("deployments", "shop", "web", from, to)
	if ch == nil || ch.User != "alice" {
		t.Fatalf("last change: %+v (controller updates must be ignored)", ch)
	}
	if s.ForPod("other", "web-1", from, to) != nil {
		t.Fatal("namespace filter")
	}
}

func TestBadPayloadAndGC(t *testing.T) {
	s := NewStore(time.Minute, "")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/audit", strings.NewReader("{")))
	if w.Code != 400 {
		t.Fatalf("code %d", w.Code)
	}
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/audit", nil))
	if w.Code != 405 {
		t.Fatalf("code %d", w.Code)
	}
	// Records older than the retention are dropped on the next insert.
	s.now = func() time.Time { return time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC) }
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/audit", strings.NewReader(batch)))
	if n := len(s.pods) + len(s.changes); n != 0 {
		t.Fatalf("%d records kept after retention", n)
	}
}
