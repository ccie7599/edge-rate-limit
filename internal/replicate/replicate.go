// Package replicate shares counter deltas between regional adapters (ADR-001,
// ADR-006).
//
// Local hits are recorded into a pending map. Every flush interval the map is
// swapped out and published as one message per ~maxMsgBytes on subject
// rl.d.<region>.<node>, which lands in that region's own JetStream stream
// RL_D_<REGION>, placed on the region's servers. Publishing therefore never
// leaves the region. Every adapter runs one ordered consumer per region stream
// and merges other nodes' deltas into its counter store, so a delta travels
// directly from the region that recorded it to each reader. Region streams are
// discovered by name, so a region that joins later is picked up without
// restarting the others.
//
// Each stream's MaxAge equals the longest policy window, so a restarted adapter
// starts its consumers at now-MaxAge and rebuilds every live counter, including
// its own pre-restart hits, from replay. After startup, a node's own messages
// are skipped because those hits are already in its store.
package replicate

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/ccie7599/edge-rate-limit/internal/counter"
)

const (
	streamPrefix  = "RL_D_"
	subjectPrefix = "rl.d."
	maxMsgBytes   = 512 * 1024
	codecVersion  = 1
	pendingShards = 64
)

type deltaKey struct {
	id       string
	windowMs int64
	slot     int64
}

type pendingShard struct {
	mu sync.Mutex
	m  map[deltaKey]uint32
}

// Config for a Replicator.
type Config struct {
	Node      string
	Region    string        // placement tag of this node; default Node
	Flush     time.Duration // publish cadence (default 100ms)
	MaxWindow time.Duration // longest policy window = stream MaxAge
	Replicas  int           // replicas of this region's stream, within the region
	Discover  time.Duration // how often to look for new region streams (default 5s)
}

// StreamName is the delta stream for a region.
func StreamName(region string) string { return streamPrefix + strings.ToUpper(region) }

// Hooks lets the caller observe replication without this package owning metrics.
type Hooks struct {
	Published func(msgs, entries int)
	Merged    func(entries int, lag time.Duration)
	Error     func(op string, err error)
}

type Replicator struct {
	cfg     Config
	hooks   Hooks
	js      nats.JetStreamContext
	store   *counter.Store
	pending [pendingShards]pendingShard
	start   time.Time

	mu       sync.Mutex
	subs     map[string]*nats.Subscription // by stream name
	caughtUp atomic.Bool

	stop chan struct{}
	done chan struct{}
}

func New(nc *nats.Conn, store *counter.Store, cfg Config, hooks Hooks) (*Replicator, error) {
	if cfg.Flush <= 0 {
		cfg.Flush = 100 * time.Millisecond
	}
	if cfg.Replicas <= 0 {
		cfg.Replicas = 1
	}
	if cfg.Region == "" {
		cfg.Region = cfg.Node
	}
	if cfg.Discover <= 0 {
		cfg.Discover = 5 * time.Second
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(4096))
	if err != nil {
		return nil, err
	}
	r := &Replicator{cfg: cfg, hooks: hooks, js: js, store: store, subs: map[string]*nats.Subscription{},
		stop: make(chan struct{}), done: make(chan struct{})}
	for i := range r.pending {
		r.pending[i].m = make(map[deltaKey]uint32)
	}
	return r, nil
}

// Start ensures this region's stream exists, begins replay/merge of every
// region stream, and starts the flush and discovery loops.
func (r *Replicator) Start() error {
	region := strings.ToLower(r.cfg.Region)
	sc := &nats.StreamConfig{
		Name:      StreamName(region),
		Subjects:  []string{subjectPrefix + region + ".>"},
		Storage:   nats.FileStorage,
		Replicas:  r.cfg.Replicas,
		MaxAge:    r.cfg.MaxWindow,
		Discard:   nats.DiscardOld,
		Retention: nats.LimitsPolicy,
		Placement: &nats.Placement{Tags: []string{"region:" + r.cfg.Region}},
	}
	if _, err := r.js.AddStream(sc); err != nil {
		if !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			return fmt.Errorf("add stream: %w", err)
		}
		if _, err = r.js.UpdateStream(sc); err != nil {
			return fmt.Errorf("update stream: %w", err)
		}
	}

	r.start = time.Now()
	replay := &sync.WaitGroup{} // streams present now, until each reaches its head
	if err := r.discover(replay); err != nil {
		r.unsubscribeAll()
		return err
	}
	go func() {
		replay.Wait()
		r.caughtUp.Store(true)
		log.Printf("replicate: replay caught up on %d region streams", r.streamCount())
	}()
	go r.flushLoop()
	return nil
}

// discover subscribes to region streams not yet followed. Every stream is
// replayed from now-MaxWindow; a region seen for the first time after startup
// has no hits in this store yet, so that is correct for late joiners too.
// replay is non-nil on the initial pass, which also fails on any error.
func (r *Replicator) discover(replay *sync.WaitGroup) error {
	initial := replay != nil
	for name := range r.js.StreamNames() {
		if !strings.HasPrefix(name, streamPrefix) {
			continue
		}
		r.mu.Lock()
		_, have := r.subs[name]
		r.mu.Unlock()
		if have {
			continue
		}
		info, err := r.js.StreamInfo(name)
		if err != nil {
			if initial {
				return fmt.Errorf("stream info %s: %w", name, err)
			}
			continue // e.g. its region is down; retry next round
		}
		var done func()
		if initial && info.State.Msgs > 0 {
			replay.Add(1)
			var once sync.Once
			done = func() { once.Do(replay.Done) }
			// If the stream's last messages age out before delivery, no message
			// reports NumPending 0; don't hold CaughtUp forever.
			time.AfterFunc(30*time.Second, done)
		}
		sub, err := r.js.Subscribe(info.Config.Subjects[0], func(m *nats.Msg) { r.onMsg(m, done) },
			nats.OrderedConsumer(), nats.BindStream(name), nats.StartTime(r.start.Add(-r.cfg.MaxWindow)))
		if err != nil {
			if done != nil {
				done()
			}
			if initial {
				return fmt.Errorf("subscribe %s: %w", name, err)
			}
			continue
		}
		r.mu.Lock()
		r.subs[name] = sub
		r.mu.Unlock()
		if !initial {
			log.Printf("replicate: following new region stream %s", name)
		}
	}
	return nil
}

func (r *Replicator) streamCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.subs)
}

func (r *Replicator) unsubscribeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, sub := range r.subs {
		sub.Unsubscribe()
		delete(r.subs, name)
	}
}

// CaughtUp reports whether the startup replay has reached the head of the stream.
func (r *Replicator) CaughtUp() bool { return r.caughtUp.Load() }

// Record queues a local hit for replication.
func (r *Replicator) Record(id string, window time.Duration, slot int64, n uint32) {
	k := deltaKey{id: id, windowMs: window.Milliseconds(), slot: slot}
	h := fnv.New32a()
	h.Write([]byte(id))
	p := &r.pending[h.Sum32()%pendingShards]
	p.mu.Lock()
	p.m[k] += n
	p.mu.Unlock()
}

// Stop flushes outstanding deltas and stops replication.
func (r *Replicator) Stop() {
	close(r.stop)
	<-r.done
	r.unsubscribeAll()
}

func (r *Replicator) flushLoop() {
	defer close(r.done)
	t := time.NewTicker(r.cfg.Flush)
	defer t.Stop()
	d := time.NewTicker(r.cfg.Discover)
	defer d.Stop()
	for {
		select {
		case <-t.C:
			r.flush()
		case <-d.C:
			go r.discover(nil)
		case <-r.stop:
			r.flush()
			select {
			case <-r.js.PublishAsyncComplete():
			case <-time.After(2 * time.Second):
			}
			return
		}
	}
}

func (r *Replicator) flush() {
	subj := subjectPrefix + strings.ToLower(r.cfg.Region) + "." + r.cfg.Node
	buf := newBuf()
	msgs, entries := 0, 0
	send := func() {
		if buf.n == 0 {
			return
		}
		if _, err := r.js.PublishAsync(subj, buf.bytes()); err != nil && r.hooks.Error != nil {
			r.hooks.Error("publish", err)
		}
		msgs++
		entries += buf.n
		buf = newBuf()
	}
	for i := range r.pending {
		p := &r.pending[i]
		p.mu.Lock()
		if len(p.m) == 0 {
			p.mu.Unlock()
			continue
		}
		m := p.m
		p.m = make(map[deltaKey]uint32, len(m))
		p.mu.Unlock()
		for k, n := range m {
			buf.put(k, n)
			if buf.size() >= maxMsgBytes {
				send()
			}
		}
	}
	send()
	if msgs > 0 && r.hooks.Published != nil {
		r.hooks.Published(msgs, entries)
	}
}

func (r *Replicator) onMsg(m *nats.Msg, replayed func()) {
	meta, err := m.Metadata()
	if err != nil {
		return
	}
	if replayed != nil && meta.NumPending == 0 {
		replayed()
	}
	node := m.Subject[strings.LastIndexByte(m.Subject, '.')+1:]
	if node == r.cfg.Node && !meta.Timestamp.Before(r.start) {
		return // our own post-start hits are already in the store
	}
	now := time.Now()
	n, err := decode(m.Data, func(k deltaKey, n uint32) {
		r.store.Add(k.id, time.Duration(k.windowMs)*time.Millisecond, k.slot, n, now)
	})
	if err != nil {
		if r.hooks.Error != nil {
			r.hooks.Error("decode", err)
		}
		return
	}
	if r.hooks.Merged != nil {
		r.hooks.Merged(n, now.Sub(meta.Timestamp))
	}
}

// Wire format: version byte, then repeated entries of
// uvarint(len id) id uvarint(windowMs) varint(slot) uvarint(n).

type buf struct {
	b []byte
	n int
}

func newBuf() *buf { return &buf{b: []byte{codecVersion}} }

func (b *buf) put(k deltaKey, n uint32) {
	b.b = binary.AppendUvarint(b.b, uint64(len(k.id)))
	b.b = append(b.b, k.id...)
	b.b = binary.AppendUvarint(b.b, uint64(k.windowMs))
	b.b = binary.AppendVarint(b.b, k.slot)
	b.b = binary.AppendUvarint(b.b, uint64(n))
	b.n++
}

func (b *buf) size() int     { return len(b.b) }
func (b *buf) bytes() []byte { return b.b }

var errCorrupt = errors.New("corrupt delta message")

func decode(data []byte, fn func(deltaKey, uint32)) (int, error) {
	if len(data) == 0 || data[0] != codecVersion {
		return 0, errCorrupt
	}
	p := data[1:]
	count := 0
	for len(p) > 0 {
		l, w := binary.Uvarint(p)
		if w <= 0 || uint64(len(p)-w) < l {
			return count, errCorrupt
		}
		p = p[w:]
		id := string(p[:l])
		p = p[l:]
		win, w := binary.Uvarint(p)
		if w <= 0 {
			return count, errCorrupt
		}
		p = p[w:]
		slot, w := binary.Varint(p)
		if w <= 0 {
			return count, errCorrupt
		}
		p = p[w:]
		n, w := binary.Uvarint(p)
		if w <= 0 {
			return count, errCorrupt
		}
		p = p[w:]
		fn(deltaKey{id: id, windowMs: int64(win), slot: slot}, uint32(n))
		count++
	}
	return count, nil
}
