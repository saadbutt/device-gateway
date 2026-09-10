package gateway

import (
	"context"
	"sync"
	"time"
)

// rateLimiter is a token bucket.
//
// Written by hand rather than pulling golang.org/x/time/rate, for the same
// reason the metrics are hand-rolled: this binary ships to edge hardware and
// its dependency list is a feature.
type rateLimiter struct {
	mu      sync.Mutex
	tokens  float64
	burst   float64
	rate    float64
	last    time.Time
	nowFn   func() time.Time
	sleepFn func(context.Context, time.Duration) bool
}

func newRateLimiter(perSecond float64, burst int) *rateLimiter {
	return &rateLimiter{
		tokens:  float64(burst),
		burst:   float64(burst),
		rate:    perSecond,
		last:    time.Now(),
		nowFn:   time.Now,
		sleepFn: sleepCtx,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// allow waits until a token is available, or returns false if ctx ends first.
func (l *rateLimiter) allow(ctx context.Context) bool {
	for {
		wait, ok := l.reserve()
		if ok {
			return true
		}
		if !l.sleepFn(ctx, wait) {
			return false
		}
	}
}

func (l *rateLimiter) reserve() (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.nowFn()
	if elapsed := now.Sub(l.last); elapsed > 0 {
		l.tokens += elapsed.Seconds() * l.rate
		if l.tokens > l.burst {
			l.tokens = l.burst
		}
		l.last = now
	}
	if l.tokens >= 1 {
		l.tokens--
		return 0, true
	}
	need := (1 - l.tokens) / l.rate
	return time.Duration(need * float64(time.Second)), false
}
