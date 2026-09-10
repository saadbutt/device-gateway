package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saadbutt/device-gateway/internal/protocol"
)

func pos(seq uint32) protocol.Position {
	return protocol.Position{DeviceID: 1, Seq: seq, At: time.Now()}
}

func TestProcessesEverySubmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var got atomic.Int64
	p := New(ctx, Config{QueueSize: 64, Workers: 4, Policy: Block}, func(context.Context, protocol.Position) {
		got.Add(1)
	})

	const n = 500
	for i := 0; i < n; i++ {
		if err := p.Submit(ctx, pos(uint32(i))); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	cancel()
	p.Wait()

	if got.Load() != n {
		t.Fatalf("processed %d of %d", got.Load(), n)
	}
}

func TestDropNewestWhenFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	var once sync.Once
	p := New(ctx, Config{QueueSize: 2, Workers: 1, Policy: DropNewest}, func(context.Context, protocol.Position) {
		once.Do(func() { <-release })
	})

	// One worker parks on the first item, two more fill the queue, the rest
	// must be refused rather than buffered.
	var refused int
	for i := 0; i < 20; i++ {
		if errors.Is(p.Submit(ctx, pos(uint32(i))), ErrQueueFull) {
			refused++
		}
	}
	close(release)

	if refused == 0 {
		t.Fatal("DropNewest never refused anything: the queue is not actually bounded")
	}
	_, _, dropped := p.Stats()
	if dropped != uint64(refused) {
		t.Errorf("dropped counter %d does not match %d refusals", dropped, refused)
	}
}

func TestDropOldestKeepsAccepting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	var once sync.Once
	p := New(ctx, Config{QueueSize: 2, Workers: 1, Policy: DropOldest}, func(context.Context, protocol.Position) {
		once.Do(func() { <-release })
	})

	for i := 0; i < 20; i++ {
		if err := p.Submit(ctx, pos(uint32(i))); err != nil {
			t.Fatalf("DropOldest should never refuse, got %v", err)
		}
	}
	close(release)

	_, _, dropped := p.Stats()
	if dropped == 0 {
		t.Fatal("DropOldest accepted 20 items into a queue of 2 without evicting anything")
	}
}

func TestBlockAppliesBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gate := make(chan struct{})
	p := New(ctx, Config{QueueSize: 1, Workers: 1, Policy: Block}, func(context.Context, protocol.Position) {
		<-gate
	})

	_ = p.Submit(ctx, pos(1)) // taken by the worker, which now blocks
	_ = p.Submit(ctx, pos(2)) // fills the queue

	done := make(chan struct{})
	go func() { _ = p.Submit(ctx, pos(3)); close(done) }()

	select {
	case <-done:
		t.Fatal("Submit returned while the queue was full: no backpressure")
	case <-time.After(50 * time.Millisecond):
	}

	close(gate)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Submit never unblocked after the worker drained")
	}
}

func TestBlockTimeoutGivesUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gate := make(chan struct{})
	defer close(gate)
	p := New(ctx, Config{
		QueueSize: 1, Workers: 1, Policy: Block, BlockTimeout: 20 * time.Millisecond,
	}, func(context.Context, protocol.Position) { <-gate })

	_ = p.Submit(ctx, pos(1))
	_ = p.Submit(ctx, pos(2))

	start := time.Now()
	err := p.Submit(ctx, pos(3))
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("BlockTimeout ignored: waited %v", elapsed)
	}
}

// TestShutdownDrainsQueue guards a real correctness bug: frames already
// acknowledged to the device must not be discarded because the process is
// stopping.
func TestShutdownDrainsQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var handled atomic.Int64
	p := New(ctx, Config{QueueSize: 100, Workers: 2, Policy: Block}, func(context.Context, protocol.Position) {
		handled.Add(1)
	})

	const n = 50
	for i := 0; i < n; i++ {
		_ = p.Submit(ctx, pos(uint32(i)))
	}
	cancel()
	p.Wait()

	if handled.Load() != n {
		t.Fatalf("shutdown discarded data: handled %d of %d", handled.Load(), n)
	}
}
