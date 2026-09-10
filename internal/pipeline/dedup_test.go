package pipeline

import (
	"math/rand"
	"sync"
	"testing"
)

func TestFirstFrameAccepted(t *testing.T) {
	d := NewDeduper(10)
	if d.Seen(1, 500) {
		t.Fatal("first frame from a device reported as duplicate")
	}
}

func TestImmediateReplayRejected(t *testing.T) {
	d := NewDeduper(10)
	d.Seen(1, 100)
	if !d.Seen(1, 100) {
		t.Fatal("exact replay accepted")
	}
}

// TestReconnectReplay is the scenario the whole package exists for: the device
// sent 1..10, the link dropped before the acks landed, it reconnected and sent
// 1..10 again followed by 11..15.
func TestReconnectReplay(t *testing.T) {
	d := NewDeduper(10)
	for seq := uint32(1); seq <= 10; seq++ {
		if d.Seen(7, seq) {
			t.Fatalf("seq %d rejected on first pass", seq)
		}
	}
	for seq := uint32(1); seq <= 10; seq++ {
		if !d.Seen(7, seq) {
			t.Fatalf("replayed seq %d was accepted a second time", seq)
		}
	}
	for seq := uint32(11); seq <= 15; seq++ {
		if d.Seen(7, seq) {
			t.Fatalf("fresh seq %d rejected after replay", seq)
		}
	}
}

// TestOutOfOrderWithinWindow covers a device that buffered while offline and
// flushes its backlog in a different order than it recorded it.
func TestOutOfOrderWithinWindow(t *testing.T) {
	d := NewDeduper(10)
	order := []uint32{50, 47, 49, 45, 48, 46}
	for _, seq := range order {
		if d.Seen(3, seq) {
			t.Fatalf("out-of-order seq %d wrongly flagged duplicate", seq)
		}
	}
	for _, seq := range order {
		if !d.Seen(3, seq) {
			t.Fatalf("seq %d not remembered after out-of-order insert", seq)
		}
	}
}

func TestWindowEdges(t *testing.T) {
	d := NewDeduper(10)
	d.Seen(1, 1)
	d.Seen(1, WindowSize) // window now covers [1, WindowSize]

	if !d.Seen(1, 1) {
		t.Error("oldest in-window sequence forgotten")
	}
	// One past the trailing edge can no longer be distinguished.
	d2 := NewDeduper(10)
	d2.Seen(2, 1)
	d2.Seen(2, WindowSize+1)
	if !d2.Seen(2, 1) {
		t.Error("sequence outside the window should be dropped, not accepted")
	}
	_, _, tooOld, _, _ := d2.Stats()
	if tooOld != 1 {
		t.Errorf("expected 1 too-old drop, got %d", tooOld)
	}
}

func TestLargeJumpClearsWindow(t *testing.T) {
	d := NewDeduper(10)
	for seq := uint32(1); seq <= 100; seq++ {
		d.Seen(5, seq)
	}
	// Device rebooted and its clock-derived sequence leapt forward.
	if d.Seen(5, 1_000_000) {
		t.Fatal("large forward jump rejected")
	}
	if d.Seen(5, 999_999) {
		t.Fatal("sequence just below a jumped high should be accepted once")
	}
	if !d.Seen(5, 999_999) {
		t.Fatal("that sequence should now be a duplicate")
	}
}

func TestDevicesAreIndependent(t *testing.T) {
	d := NewDeduper(10)
	d.Seen(1, 77)
	if d.Seen(2, 77) {
		t.Fatal("device 2 shares device 1's window")
	}
}

func TestLRUEvictionBounded(t *testing.T) {
	const cap = 4
	d := NewDeduper(cap)
	for id := uint64(1); id <= 100; id++ {
		d.Seen(id, 1)
	}
	_, _, _, evicted, tracked := d.Stats()
	if tracked > cap {
		t.Fatalf("tracked %d devices with cap %d", tracked, cap)
	}
	if evicted == 0 {
		t.Fatal("expected evictions")
	}
}

func TestResetForgetsDevice(t *testing.T) {
	d := NewDeduper(10)
	d.Seen(9, 500)
	if !d.Seen(9, 500) {
		t.Fatal("setup: expected duplicate")
	}
	d.Reset(9)
	if d.Seen(9, 500) {
		t.Fatal("Reset did not clear the window")
	}
}

func TestConcurrentAccess(t *testing.T) {
	d := NewDeduper(1000)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < 5000; i++ {
				d.Seen(uint64(r.Intn(50)), uint32(r.Intn(2000)))
			}
		}(w)
	}
	wg.Wait()
}

func BenchmarkSeenSequential(b *testing.B) {
	d := NewDeduper(10000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d.Seen(uint64(i%1000), uint32(i))
	}
}
