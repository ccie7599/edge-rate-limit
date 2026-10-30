// Package api serves the rate-limit hit endpoint the EdgeWorker calls.
//
//	POST /v1/rl/hit   {"hits":[{"policy":"login","key":"<hex>","limit":100,"window":600,"cost":1}]}
//	             →    {"blocked":false,"retry_after":0,"region":"lax","rtt_ms":28.4,"conn_seq":1,"results":[{"policy":"login","count":7,"limit":100,"blocked":false,"retry_after":0}]}
//	GET  /v1/rl/peek?policy=login&key=<hex>   → {"count":7}  (this node's view; no hit, no auth)
//	GET  /v1/health
//
// rtt_ms is the kernel's round-trip estimate to the calling edge server and
// conn_seq the request's position on its connection (1 = a new connection, so
// the caller also paid TCP and TLS setup). window and retry_after are seconds. Every hit is counted, blocked or not
// (ADR-005). The response's top-level blocked/retry_after summarize all hits so
// the worker needs no per-policy logic to enforce.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ccie7599/edge-rate-limit/internal/conninfo"
	"github.com/ccie7599/edge-rate-limit/internal/counter"
)

const (
	maxHits      = 16
	maxIDLen     = 256
	minWindowSec = 10
	maxBodyBytes = 16 << 10
)

// Recorder receives local hits for replication.
type Recorder interface {
	Record(id string, window time.Duration, slot int64, n uint32)
}

type Health func() map[string]any

type Server struct {
	store     *counter.Store
	rec       Recorder
	token     []byte
	maxWindow time.Duration
	health    Health

	// Region is echoed in hit responses so the edge can report which region answered.
	Region string

	hits     *prometheus.CounterVec
	duration prometheus.Histogram
}

func New(store *counter.Store, rec Recorder, token string, maxWindow time.Duration, health Health, reg prometheus.Registerer) *Server {
	s := &Server{store: store, rec: rec, token: []byte(token), maxWindow: maxWindow, health: health,
		hits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rl_hits_total", Help: "Hits evaluated, by policy and outcome.",
		}, []string{"policy", "outcome"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "rl_hit_request_seconds", Help: "Server-side latency of POST /v1/rl/hit.",
			Buckets: []float64{.00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025},
		}),
	}
	reg.MustRegister(s.hits, s.duration)
	return s
}

func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/rl/hit", s.handleHit)
	mux.HandleFunc("GET /v1/rl/peek", s.handlePeek)
	mux.HandleFunc("GET /v1/health", s.handleHealth)
}

func (s *Server) authorized(r *http.Request) bool {
	return len(s.token) == 0 || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-RL-Token")), s.token) == 1
}

// handlePeek reports this node's windowed count for one key without counting a
// hit. Polling it on every region shows convergence.
// Keys are already hashed at the edge, so peek is unauthenticated.
func (s *Server) handlePeek(w http.ResponseWriter, r *http.Request) {
	policy, key := r.URL.Query().Get("policy"), r.URL.Query().Get("key")
	if policy == "" || key == "" {
		http.Error(w, "policy and key are required", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(map[string]any{"count": s.store.Count(policy+"\x00"+key, time.Now())})
}

type hitReq struct {
	Hits []struct {
		Policy string `json:"policy"`
		Key    string `json:"key"`
		Limit  uint32 `json:"limit"`
		Window uint32 `json:"window"`
		Cost   uint32 `json:"cost"`
	} `json:"hits"`
}

type hitResult struct {
	Policy     string `json:"policy"`
	Count      uint32 `json:"count"`
	Limit      uint32 `json:"limit"`
	Blocked    bool   `json:"blocked"`
	RetryAfter int    `json:"retry_after"`
}

type hitResp struct {
	Blocked    bool        `json:"blocked"`
	RetryAfter int         `json:"retry_after"`
	Region     string      `json:"region,omitempty"`
	RTTms      float64     `json:"rtt_ms,omitempty"`
	ConnSeq    int64       `json:"conn_seq,omitempty"`
	Results    []hitResult `json:"results"`
}

func (s *Server) handleHit(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() { s.duration.Observe(time.Since(start).Seconds()) }()

	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req hitReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if len(req.Hits) == 0 || len(req.Hits) > maxHits {
		http.Error(w, "hits must have 1-16 entries", http.StatusBadRequest)
		return
	}
	for _, h := range req.Hits {
		win := time.Duration(h.Window) * time.Second
		if h.Policy == "" || h.Key == "" || len(h.Policy)+len(h.Key) > maxIDLen ||
			h.Limit == 0 || h.Window < minWindowSec || win > s.maxWindow {
			http.Error(w, "invalid hit: policy, key, limit and window (10s..max) are required", http.StatusBadRequest)
			return
		}
	}

	now := time.Now()
	resp := hitResp{Region: s.Region, Results: make([]hitResult, len(req.Hits))}
	rtt, seq, ok := conninfo.Next(r)
	resp.ConnSeq = seq
	if ok {
		resp.RTTms = math.Round(float64(rtt.Microseconds())/100) / 10
	}
	for i, h := range req.Hits {
		cost := h.Cost
		if cost == 0 {
			cost = 1
		}
		win := time.Duration(h.Window) * time.Second
		id := h.Policy + "\x00" + h.Key
		res := s.store.Hit(id, win, cost, h.Limit, now)
		s.rec.Record(id, win, res.Slot, cost)

		ra := int(math.Ceil(res.RetryAfter.Seconds()))
		resp.Results[i] = hitResult{Policy: h.Policy, Count: res.Count, Limit: h.Limit, Blocked: res.Blocked, RetryAfter: ra}
		outcome := "allowed"
		if res.Blocked {
			outcome = "blocked"
			resp.Blocked = true
			resp.RetryAfter = max(resp.RetryAfter, ra)
		}
		s.hits.WithLabelValues(h.Policy, outcome).Inc()
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.health())
}
