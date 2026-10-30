package counter

import (
	"testing"
	"time"
)

var t0 = time.UnixMilli(1_790_000_000_000)

func TestHitBlocksOverLimit(t *testing.T) {
	s := New()
	w := 10 * time.Minute
	for i := 1; i <= 5; i++ {
		r := s.Hit("p:k", w, 1, 5, t0)
		if r.Blocked || r.Count != uint32(i) {
			t.Fatalf("hit %d: %+v", i, r)
		}
	}
	r := s.Hit("p:k", w, 1, 5, t0)
	if !r.Blocked || r.Count != 6 {
		t.Fatalf("6th hit should block: %+v", r)
	}
	if r.RetryAfter <= 0 || r.RetryAfter > w {
		t.Fatalf("retry-after out of range: %v", r.RetryAfter)
	}
}

func TestWindowSlides(t *testing.T) {
	s := New()
	w := 10 * time.Minute // 1-minute slots
	s.Hit("k", w, 3, 100, t0)
	s.Hit("k", w, 2, 100, t0.Add(5*time.Minute))
	if c := s.Count("k", t0.Add(9*time.Minute)); c != 5 {
		t.Fatalf("within window: got %d want 5", c)
	}
	if c := s.Count("k", t0.Add(11*time.Minute)); c != 2 {
		t.Fatalf("first slot expired: got %d want 2", c)
	}
	if c := s.Count("k", t0.Add(16*time.Minute)); c != 0 {
		t.Fatalf("all expired: got %d want 0", c)
	}
}

func TestRetryAfterReleasesWhenOldSlotsExpire(t *testing.T) {
	s := New()
	w := 10 * time.Minute
	s.Hit("k", w, 10, 10, t0)                        // fills the limit in the first slot
	r := s.Hit("k", w, 1, 10, t0.Add(3*time.Minute)) // 11 > 10
	if !r.Blocked {
		t.Fatal("expected block")
	}
	// The 10 hits in t0's slot expire 10 minutes after that slot started.
	if c := s.Count("k", t0.Add(r.RetryAfter).Add(3*time.Minute)); c >= 10 {
		t.Fatalf("still at %d after retry-after", c)
	}
}

func TestRemoteDeltaMerges(t *testing.T) {
	s := New()
	w := time.Minute
	slot := t0.UnixMilli() / WidthMs(w)
	s.Add("k", w, slot, 7, t0)
	r := s.Hit("k", w, 1, 5, t0)
	if r.Count != 8 || !r.Blocked {
		t.Fatalf("remote delta not merged: %+v", r)
	}
	// A delta for an expired slot is ignored.
	s.Add("k", w, slot-Slots, 100, t0)
	if c := s.Count("k", t0); c != 8 {
		t.Fatalf("stale delta applied: %d", c)
	}
}

func TestGC(t *testing.T) {
	s := New()
	s.Hit("a", time.Minute, 1, 10, t0)
	s.Hit("b", time.Hour, 1, 10, t0)
	if n := s.GC(t0.Add(2 * time.Minute)); n != 1 {
		t.Fatalf("GC kept %d keys, want 1", n)
	}
}

func BenchmarkHitParallel(b *testing.B) {
	s := New()
	keys := make([]string, 1<<16)
	for i := range keys {
		keys[i] = "policy:" + string(rune('a'+i%26)) + time.Duration(i).String()
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		now := time.Now()
		for pb.Next() {
			s.Hit(keys[i&(len(keys)-1)], 10*time.Minute, 1, 1000, now)
			i++
		}
	})
}
