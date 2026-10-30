package replicate

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/ccie7599/edge-rate-limit/internal/counter"
	"github.com/ccie7599/edge-rate-limit/internal/natsembed"
)

func TestCodecRoundTrip(t *testing.T) {
	b := newBuf()
	in := []deltaKey{{"login:abc", 600000, 2983333333}, {"cart:\x00x", 60000, -1}}
	for i, k := range in {
		b.put(k, uint32(i+5))
	}
	var out []deltaKey
	n, err := decode(b.bytes(), func(k deltaKey, v uint32) { out = append(out, k) })
	if err != nil || n != 2 || out[0] != in[0] || out[1] != in[1] {
		t.Fatalf("round trip: n=%d err=%v out=%v", n, err, out)
	}
	if _, err := decode([]byte{codecVersion, 0xff}, func(deltaKey, uint32) {}); err == nil {
		t.Fatal("expected corrupt error")
	}
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type node struct {
	name  string
	ns    *natsserver.Server
	nc    *nats.Conn
	store *counter.Store
	rep   *Replicator
}

func startCluster(t *testing.T, names []string, dir string) []*node {
	ports := make([]int, len(names))
	for i := range ports {
		ports[i] = freePort(t)
	}
	var routes []string
	for _, p := range ports {
		routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", p))
	}
	nodes := make([]*node, len(names))
	for i, name := range names {
		ns, err := natsembed.Start(natsembed.Config{
			Name: name, StoreDir: dir + "/" + name, ClientPort: -1,
			ClusterPort: ports[i], Routes: strings.Join(routes, ","), Region: name,
		})
		if err != nil {
			t.Fatal(err)
		}
		nc, err := nats.Connect(ns.ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &node{name: name, ns: ns, nc: nc, store: counter.New()}
	}
	return nodes
}

func (n *node) startReplicator(t *testing.T) {
	rep, err := New(n.nc, n.store, Config{Node: n.name, Region: n.name, Flush: 50 * time.Millisecond,
		MaxWindow: 10 * time.Minute, Discover: 200 * time.Millisecond}, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	// The JetStream meta leader may still be electing right after boot.
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err = rep.Start(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s start: %v", n.name, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	n.rep = rep
}

func (n *node) hit(id string, k uint32) {
	now := time.Now()
	for i := uint32(0); i < k; i++ {
		r := n.store.Hit(id, 10*time.Minute, 1, 1<<30, now)
		n.rep.Record(id, 10*time.Minute, r.Slot, 1)
	}
}

func waitCount(t *testing.T, n *node, id string, want uint32, within time.Duration) time.Duration {
	start := time.Now()
	for time.Since(start) < within {
		if n.store.Count(id, time.Now()) == want {
			return time.Since(start)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s: count %d, want %d after %v", n.name, n.store.Count(id, time.Now()), want, within)
	return 0
}

func TestClusterConvergesAndReplays(t *testing.T) {
	dir := t.TempDir()
	nodes := startCluster(t, []string{"ord", "iad", "sea"}, dir)
	for _, n := range nodes {
		n.startReplicator(t)
	}
	defer func() {
		for _, n := range nodes {
			if n.rep != nil {
				n.rep.Stop()
			}
			n.nc.Close()
			n.ns.Shutdown()
		}
	}()

	// Hits in all three regions on the same key; every region must see 60.
	nodes[0].hit("login:user1", 10)
	nodes[1].hit("login:user1", 20)
	nodes[2].hit("login:user1", 30)
	for _, n := range nodes {
		took := waitCount(t, n, "login:user1", 60, 5*time.Second)
		t.Logf("%s converged in %v", n.name, took)
	}

	// Simulate an adapter restart: fresh store and replicator on the same NATS
	// node. Replay must restore the key, including the node's own earlier hits.
	sea := nodes[2]
	sea.rep.Stop()
	sea.store = counter.New()
	sea.startReplicator(t)
	waitCount(t, sea, "login:user1", 60, 5*time.Second)
	for deadline := time.Now().Add(2 * time.Second); !sea.rep.CaughtUp(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("replay not marked caught up")
		}
	}

	// And new hits after the restart still converge without double counting.
	sea.hit("login:user1", 5)
	for _, n := range nodes {
		waitCount(t, n, "login:user1", 65, 5*time.Second)
	}
	time.Sleep(300 * time.Millisecond)
	if c := sea.store.Count("login:user1", time.Now()); c != 65 {
		t.Fatalf("restarted node double counted: %d", c)
	}
}

func TestLateRegionIsDiscovered(t *testing.T) {
	dir := t.TempDir()
	nodes := startCluster(t, []string{"ord", "fra", "syd"}, dir)
	defer func() {
		for _, n := range nodes {
			if n.rep != nil {
				n.rep.Stop()
			}
			n.nc.Close()
			n.ns.Shutdown()
		}
	}()
	// Two regions replicate first; syd's replicator starts later.
	nodes[0].startReplicator(t)
	nodes[1].startReplicator(t)
	nodes[0].hit("search:u", 7)
	waitCount(t, nodes[1], "search:u", 7, 5*time.Second)

	syd := nodes[2]
	syd.startReplicator(t)
	waitCount(t, syd, "search:u", 7, 5*time.Second) // replays the existing regions
	syd.hit("search:u", 3)
	for _, n := range nodes {
		waitCount(t, n, "search:u", 10, 5*time.Second) // existing regions discover RL_D_SYD
	}
}
