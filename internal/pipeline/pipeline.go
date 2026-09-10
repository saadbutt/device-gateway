package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/saadbutt/device-gateway/internal/protocol"
)

// OverflowPolicy decides what happens when the ingest queue is full.
//
// This is the single most consequential knob in a telemetry gateway and it is
// usually left implicit, which is how systems end up with unbounded queues and
// an OOM at 4am. The three honest options:
type OverflowPolicy int

const (
	// DropNewest rejects the arriving frame and leaves the queue untouched.
	// Correct when older data is more valuable, such as trip reconstruction
	// where a gap at the end is easier to explain than a gap in the middle.
	DropNewest OverflowPolicy = iota

	// DropOldest evicts the head to make room. Correct for live tracking, where
	// a stale position is worthless and the current one is the whole product.
	DropOldest

	// Block applies real backpressure: the connection goroutine stops reading,
	// the TCP receive window closes, and the device slows down or buffers on
	// its own flash. Correct when you would rather lose throughput than data.
	Block
)

func (p OverflowPolicy) String() string {
	switch p {
	case DropOldest:
		return "drop_oldest"
	case Block:
		return "block"
	default:
		return "drop_newest"
	}
}

// ErrQueueFull is returned by Submit under DropNewest.
var ErrQueueFull = errors.New("pipeline: queue full")

// Handler consumes an accepted position. It runs on a worker goroutine, not
// the connection goroutine, so a slow database cannot stall the read loop.
type Handler func(ctx context.Context, p protocol.Position)

// Pipeline is a bounded fan-in queue with a fixed worker pool.
type Pipeline struct {
	queue   chan protocol.Position
	policy  OverflowPolicy
	handler Handler

	blockTimeout time.Duration

	submitted atomic.Uint64
	processed atomic.Uint64
	dropped   atomic.Uint64

	wg   sync.WaitGroup
	once sync.Once
}

// Config parameterises a Pipeline.
type Config struct {
	QueueSize int
	Workers   int
	Policy    OverflowPolicy
	// BlockTimeout caps how long Submit waits under the Block policy before
	// giving up. Zero means wait until the context is done.
	BlockTimeout time.Duration
}

// New starts the worker pool. Cancel ctx and call Wait to shut it down.
func New(ctx context.Context, cfg Config, h Handler) *Pipeline {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1024
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	p := &Pipeline{
		queue:        make(chan protocol.Position, cfg.QueueSize),
		policy:       cfg.Policy,
		handler:      h,
		blockTimeout: cfg.BlockTimeout,
	}
	for i := 0; i < cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
	return p
}

func (p *Pipeline) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			// Drain what is already queued rather than discarding it. A
			// shutdown that throws away accepted-and-acknowledged data is a
			// correctness bug, not a tidy exit.
			for {
				select {
				case pos := <-p.queue:
					p.handler(context.WithoutCancel(ctx), pos)
					p.processed.Add(1)
				default:
					return
				}
			}
		case pos := <-p.queue:
			p.handler(ctx, pos)
			p.processed.Add(1)
		}
	}
}

// Submit offers a position to the queue according to the overflow policy.
func (p *Pipeline) Submit(ctx context.Context, pos protocol.Position) error {
	p.submitted.Add(1)

	switch p.policy {
	case Block:
		if p.blockTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, p.blockTimeout)
			defer cancel()
		}
		select {
		case p.queue <- pos:
			return nil
		case <-ctx.Done():
			p.dropped.Add(1)
			return ctx.Err()
		}

	case DropOldest:
		for {
			select {
			case p.queue <- pos:
				return nil
			default:
			}
			select {
			case <-p.queue:
				p.dropped.Add(1)
			default:
				// A worker drained it between the two selects. Retry.
			}
		}

	default: // DropNewest
		select {
		case p.queue <- pos:
			return nil
		default:
			p.dropped.Add(1)
			return ErrQueueFull
		}
	}
}

// Wait blocks until every worker has exited and the queue is drained.
func (p *Pipeline) Wait() { p.once.Do(func() { p.wg.Wait() }) }

// Depth is the number of positions currently queued.
func (p *Pipeline) Depth() int { return len(p.queue) }

// Capacity is the configured queue size.
func (p *Pipeline) Capacity() int { return cap(p.queue) }

// Stats returns submitted, processed and dropped counts.
func (p *Pipeline) Stats() (submitted, processed, dropped uint64) {
	return p.submitted.Load(), p.processed.Load(), p.dropped.Load()
}
