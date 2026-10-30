package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ccie7599/edge-rate-limit/internal/counter"
)

type nopRec struct{ n int }

func (r *nopRec) Record(string, time.Duration, int64, uint32) { r.n++ }

func newTestServer(rec Recorder) *http.ServeMux {
	s := New(counter.New(), rec, "tok", time.Hour, func() map[string]any { return map[string]any{"ok": true} }, prometheus.NewRegistry())
	mux := http.NewServeMux()
	s.Routes(mux)
	return mux
}

func post(mux *http.ServeMux, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/v1/rl/hit", strings.NewReader(body))
	req.Header.Set("X-RL-Token", token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func TestHitEnforcesAcrossPolicies(t *testing.T) {
	rec := &nopRec{}
	mux := newTestServer(rec)
	body := `{"hits":[{"policy":"ip","key":"a","limit":100,"window":60},{"policy":"user","key":"b","limit":2,"window":600}]}`
	var resp hitResp
	for i := 0; i < 3; i++ {
		w := post(mux, "tok", body)
		if w.Code != 200 {
			t.Fatalf("status %d: %s", w.Code, w.Body)
		}
		resp = hitResp{}
		json.Unmarshal(w.Body.Bytes(), &resp)
	}
	if !resp.Blocked || resp.Results[0].Blocked || !resp.Results[1].Blocked || resp.Results[1].Count != 3 {
		t.Fatalf("third call should block on user policy only: %+v", resp)
	}
	if resp.RetryAfter <= 0 {
		t.Fatalf("retry_after missing: %+v", resp)
	}
	if rec.n != 6 {
		t.Fatalf("recorded %d hits for replication, want 6", rec.n)
	}
}

func TestHitRejectsBadInput(t *testing.T) {
	mux := newTestServer(&nopRec{})
	if w := post(mux, "wrong", `{"hits":[]}`); w.Code != 401 {
		t.Fatalf("bad token: %d", w.Code)
	}
	for _, body := range []string{
		`{"hits":[]}`,
		`{"hits":[{"policy":"p","key":"k","limit":1,"window":5}]}`,    // window < 10s
		`{"hits":[{"policy":"p","key":"k","limit":1,"window":7200}]}`, // window > max
		`{"hits":[{"policy":"p","key":"k","limit":0,"window":60}]}`,
		`not json`,
	} {
		if w := post(mux, "tok", body); w.Code != 400 {
			t.Fatalf("%s: got %d", body, w.Code)
		}
	}
}

func TestPeekReadsWithoutCounting(t *testing.T) {
	mux := newTestServer(&nopRec{})
	post(mux, "tok", `{"hits":[{"policy":"p","key":"k","limit":10,"window":60}]}`)
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/v1/rl/peek?policy=p&key=k", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"count":1}` {
			t.Fatalf("peek %d: %d %s", i, w.Code, w.Body)
		}
	}
}
