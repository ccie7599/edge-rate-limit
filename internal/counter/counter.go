// Package counter holds per-key sliding-window hit counters in memory.
//
// Each key's window is split into Slots fixed-width slots (width = window/Slots).
// A slot is identified by its absolute index (unixMillis / width), so local hits
// and remote deltas from other regions land in the same slot as long as their
// clocks agree to within a slot width. The windowed count is the sum of the
// last Slots slots, which over-counts by at most one slot's worth of hits at the
// trailing edge — the usual sliding-window approximation.
package counter

import (
	"hash/fnv"
	"sync"
	"time"
)

// Slots per window. 10 gives 1-minute granularity on a 10-minute window.
const Slots = 10

const numShards = 256

type entry struct {
	widthMs int64
	counts  [Slots]uint32
	idx     [Slots]int64 // absolute slot index held in each ring position
	last    int64        // newest slot index written, for GC
}

type shard struct {
	mu sync.Mutex
	m  map[string]*entry
}

// Store is safe for concurrent use.
type Store struct {
	shards [numShards]shard
}

func New() *Store {
	s := &Store{}
	for i := range s.shards {
		s.shards[i].m = make(map[string]*entry)
	}
	return s
}

// Result of a hit against a limit.
type Result struct {
	Count      uint32        // windowed count including this hit
	Blocked    bool          // Count > limit
	RetryAfter time.Duration // zero unless Blocked
	Slot       int64         // slot index the hit landed in (for replication)
}

// WidthMs is the slot width for a window.
func WidthMs(window time.Duration) int64 {
	w := window.Milliseconds() / Slots
	if w < 1 {
		w = 1
	}
	return w
}

func (s *Store) shardFor(id string) *shard {
	h := fnv.New32a()
	h.Write([]byte(id))
	return &s.shards[h.Sum32()%numShards]
}

// Hit adds n to id's current slot and evaluates the window against limit.
func (s *Store) Hit(id string, window time.Duration, n, limit uint32, now time.Time) Result {
	width := WidthMs(window)
	cur := now.UnixMilli() / width
	sh := s.shardFor(id)
	sh.mu.Lock()
	e := sh.get(id, width)
	e.add(cur, n)
	count := e.sum(cur)
	r := Result{Count: count, Slot: cur}
	if count > limit {
		r.Blocked = true
		r.RetryAfter = e.retryAfter(cur, limit, now.UnixMilli())
	}
	sh.mu.Unlock()
	return r
}

// Add merges a delta (typically from another region) into a specific slot.
// Deltas for slots already outside the window are dropped.
func (s *Store) Add(id string, window time.Duration, slot int64, n uint32, now time.Time) {
	width := WidthMs(window)
	if slot <= now.UnixMilli()/width-Slots {
		return
	}
	sh := s.shardFor(id)
	sh.mu.Lock()
	sh.get(id, width).add(slot, n)
	sh.mu.Unlock()
}

// Count returns id's windowed count without adding a hit.
func (s *Store) Count(id string, now time.Time) uint32 {
	sh := s.shardFor(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e, ok := sh.m[id]
	if !ok {
		return 0
	}
	return e.sum(now.UnixMilli() / e.widthMs)
}

// GC drops keys with no hits inside their window and returns the number of
// live keys remaining.
func (s *Store) GC(now time.Time) int {
	ms := now.UnixMilli()
	live := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for id, e := range sh.m {
			if e.last <= ms/e.widthMs-Slots {
				delete(sh.m, id)
			}
		}
		live += len(sh.m)
		sh.mu.Unlock()
	}
	return live
}

func (sh *shard) get(id string, width int64) *entry {
	e, ok := sh.m[id]
	if !ok || e.widthMs != width {
		// A policy's window changed: start the key over at the new width.
		e = &entry{widthMs: width}
		sh.m[id] = e
	}
	return e
}

func (e *entry) add(slot int64, n uint32) {
	pos := slot % Slots
	if e.idx[pos] != slot {
		if e.idx[pos] > slot {
			return // ring position already reused by a newer slot; delta is stale
		}
		e.idx[pos] = slot
		e.counts[pos] = 0
	}
	e.counts[pos] += n
	if slot > e.last {
		e.last = slot
	}
}

func (e *entry) sum(cur int64) uint32 {
	var total uint32
	for i := 0; i < Slots; i++ {
		if e.idx[i] > cur-Slots && e.idx[i] <= cur {
			total += e.counts[i]
		}
	}
	return total
}

// retryAfter is how long until, with no further hits, the next hit would be
// allowed (count+1 <= limit). Oldest slots expire first.
func (e *entry) retryAfter(cur int64, limit uint32, nowMs int64) time.Duration {
	remaining := e.sum(cur)
	for k := int64(1); k <= Slots; k++ {
		expiring := cur - Slots + k
		pos := expiring % Slots
		if e.idx[pos] == expiring {
			remaining -= e.counts[pos]
		}
		if remaining < limit {
			return time.Duration((cur+k)*e.widthMs-nowMs) * time.Millisecond
		}
	}
	return time.Duration((cur+Slots)*e.widthMs-nowMs) * time.Millisecond
}
