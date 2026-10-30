// rl-adapter: regional rate-limit counter service for the edge rate limiter.
//
// Env:
//
//	NODE_ID            unique node name, also the NATS server name   (default hostname)
//	REGION             placement tag; names this region's delta stream RL_D_<REGION>
//	HTTP_ADDR          hit API + /metrics                            (default :8080)
//	TLS_CERT_FILE      serve HTTPS with this cert chain (and TLS_KEY_FILE)
//	RL_TOKEN           shared secret the EdgeWorker sends as X-RL-Token ("" disables auth)
//	MAX_WINDOW_SEC     longest accepted policy window = stream MaxAge (default 3600)
//	FLUSH_MS           delta publish cadence                         (default 100)
//	STREAM_REPLICAS    replicas of RL_D_<REGION>, within the region  (default 1)
//	JETSTREAM_DIR      store dir                                     (default /var/lib/rl/js)
//	NATS_CLIENT_PORT   embedded NATS client port                     (default 4222)
//	NATS_CLUSTER_PORT  cluster port; 0 = standalone                  (default 0)
//	NATS_ROUTES        comma-separated nats-route:// URLs
//	CLUSTER_ADVERTISE  public host:port for the route (hostNetwork nodes)
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ccie7599/edge-rate-limit/internal/api"
	"github.com/ccie7599/edge-rate-limit/internal/conninfo"
	"github.com/ccie7599/edge-rate-limit/internal/counter"
	"github.com/ccie7599/edge-rate-limit/internal/natsembed"
	"github.com/ccie7599/edge-rate-limit/internal/replicate"
)

func main() {
	host, _ := os.Hostname()
	node := envOr("NODE_ID", host)
	maxWindow := time.Duration(envInt("MAX_WINDOW_SEC", 3600)) * time.Second

	ns, err := natsembed.Start(natsembed.Config{
		Name:        node,
		StoreDir:    envOr("JETSTREAM_DIR", "/var/lib/rl/js"),
		ClientPort:  envInt("NATS_CLIENT_PORT", 4222),
		ClusterPort: envInt("NATS_CLUSTER_PORT", 0),
		Routes:      os.Getenv("NATS_ROUTES"),
		Advertise:   os.Getenv("CLUSTER_ADVERTISE"),
		Region:      os.Getenv("REGION"),
		Debug:       os.Getenv("NATS_DEBUG") == "1",
	})
	if err != nil {
		log.Fatalf("nats: %v", err)
	}
	nc, err := nats.Connect(ns.ClientURL(), nats.InProcessServer(ns))
	if err != nil {
		log.Fatalf("nats connect: %v", err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	m := newMetrics(reg)

	store := counter.New()
	rep, err := replicate.New(nc, store, replicate.Config{
		Node:      node,
		Region:    envOr("REGION", node),
		Flush:     time.Duration(envInt("FLUSH_MS", 100)) * time.Millisecond,
		MaxWindow: maxWindow,
		Replicas:  envInt("STREAM_REPLICAS", 1),
	}, m.hooks())
	if err != nil {
		log.Fatalf("replicator: %v", err)
	}
	// Serve immediately and keep trying to join replication: until the stream
	// is placed (e.g. a peer region is down at boot) this node counts locally,
	// which is the same degraded mode as a partitioned region.
	var replicating atomic.Bool
	go func() {
		for i := 0; ; i++ {
			if err := rep.Start(); err == nil {
				replicating.Store(true)
				log.Printf("replicator started")
				return
			} else if i%10 == 0 {
				log.Printf("replicator start: %v (retrying)", err)
			}
			time.Sleep(3 * time.Second)
		}
	}()

	go func() {
		for range time.Tick(30 * time.Second) {
			m.keys.Set(float64(store.GC(time.Now())))
		}
	}()

	srv := api.New(store, rep, os.Getenv("RL_TOKEN"), maxWindow, func() map[string]any {
		return map[string]any{"ok": true, "node": node, "region": os.Getenv("REGION"), "replicating": replicating.Load(), "replay_caught_up": rep.CaughtUp()}
	}, reg)
	srv.Region = os.Getenv("REGION")
	mux := http.NewServeMux()
	srv.Routes(mux)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	hs := &http.Server{Addr: envOr("HTTP_ADDR", ":8080"), Handler: mux, ConnContext: conninfo.ConnContext,
		ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 120 * time.Second}
	go func() {
		log.Printf("rl-adapter node=%s listening %s", node, hs.Addr)
		var err error
		if cert := os.Getenv("TLS_CERT_FILE"); cert != "" {
			err = hs.ListenAndServeTLS(cert, os.Getenv("TLS_KEY_FILE"))
		} else {
			err = hs.ListenAndServe()
		}
		if err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hs.Shutdown(ctx)
	rep.Stop() // flush last deltas before NATS goes away
	nc.Close()
	ns.Shutdown()
}

type metrics struct {
	published *prometheus.CounterVec
	merged    prometheus.Counter
	lag       prometheus.Histogram
	errors    *prometheus.CounterVec
	keys      prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rl_delta_published_total", Help: "Delta messages and entries published."}, []string{"unit"}),
		merged: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "rl_delta_merged_entries_total", Help: "Remote delta entries merged."}),
		lag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "rl_delta_merge_lag_seconds", Help: "Stream store time to local merge.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 5}}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rl_replication_errors_total", Help: "Replication errors."}, []string{"op"}),
		keys: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "rl_live_keys", Help: "Keys with hits inside their window."}),
	}
	reg.MustRegister(m.published, m.merged, m.lag, m.errors, m.keys)
	return m
}

func (m *metrics) hooks() replicate.Hooks {
	return replicate.Hooks{
		Published: func(msgs, entries int) {
			m.published.WithLabelValues("messages").Add(float64(msgs))
			m.published.WithLabelValues("entries").Add(float64(entries))
		},
		Merged: func(entries int, lag time.Duration) {
			m.merged.Add(float64(entries))
			m.lag.Observe(lag.Seconds())
		},
		Error: func(op string, err error) {
			m.errors.WithLabelValues(op).Inc()
			log.Printf("replicate %s: %v", op, err)
		},
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}
