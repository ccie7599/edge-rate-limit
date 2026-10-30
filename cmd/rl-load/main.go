// rl-load drives POST /v1/rl/hit and reports throughput and client-side latency.
//
//	rl-load -url http://127.0.0.1:8080 -c 64 -d 20s -keys 1000000 -policies 2
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	urls := flag.String("url", "http://127.0.0.1:8080", "adapter base URL(s), comma-separated; workers round-robin")
	conc := flag.Int("c", 64, "concurrent workers")
	dur := flag.Duration("d", 20*time.Second, "duration")
	keys := flag.Int("keys", 1_000_000, "distinct caller keys")
	policies := flag.Int("policies", 2, "hits per request (policies matched)")
	limit := flag.Int("limit", 1000, "limit per key")
	token := flag.String("token", os.Getenv("RL_TOKEN"), "X-RL-Token")
	flag.Parse()

	targets := strings.Split(*urls, ",")
	tr := &http.Transport{MaxIdleConnsPerHost: *conc, MaxConnsPerHost: *conc}
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	var ok, blocked, errs atomic.Int64
	lats := make([][]time.Duration, *conc)
	stop := time.Now().Add(*dur)
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			url := targets[w%len(targets)] + "/v1/rl/hit"
			var body bytes.Buffer
			for time.Now().Before(stop) {
				k := rand.IntN(*keys)
				body.Reset()
				body.WriteString(`{"hits":[`)
				for p := 0; p < *policies; p++ {
					if p > 0 {
						body.WriteByte(',')
					}
					fmt.Fprintf(&body, `{"policy":"p%d","key":"%016x","limit":%d,"window":600}`, p, k, *limit)
				}
				body.WriteString(`]}`)
				req, _ := http.NewRequest("POST", url, bytes.NewReader(body.Bytes()))
				req.Header.Set("X-RL-Token", *token)
				t := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					errs.Add(1)
					continue
				}
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				lats[w] = append(lats[w], time.Since(t))
				switch {
				case resp.StatusCode != 200:
					errs.Add(1)
				case bytes.HasPrefix(b, []byte(`{"blocked":true`)):
					blocked.Add(1)
				default:
					ok.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	var all []time.Duration
	for _, l := range lats {
		all = append(all, l...)
	}
	slices.Sort(all)
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[int(float64(len(all)-1)*p)]
	}
	total := ok.Load() + blocked.Load() + errs.Load()
	fmt.Printf("requests=%d rps=%.0f allowed=%d blocked=%d errors=%d\n",
		total, float64(total)/dur.Seconds(), ok.Load(), blocked.Load(), errs.Load())
	fmt.Printf("latency p50=%v p90=%v p99=%v p99.9=%v max=%v\n",
		pct(.5), pct(.9), pct(.99), pct(.999), pct(1))
}
