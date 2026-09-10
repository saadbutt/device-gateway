package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func render(t *testing.T, r *Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("wrong content type for Prometheus exposition: %q", ct)
	}
	return rec.Body.String()
}

func mustContain(t *testing.T, body string, lines ...string) {
	t.Helper()
	for _, want := range lines {
		if !strings.Contains(body, want) {
			t.Errorf("exposition missing %q\n---\n%s", want, body)
		}
	}
}

func TestCounterExposition(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("frames_total", "Frames seen.", nil)
	c.Add(7)
	c.Inc()

	mustContain(t, render(t, r),
		"# HELP frames_total Frames seen.",
		"# TYPE frames_total counter",
		"frames_total 8",
	)
}

func TestGaugeGoesUpAndDown(t *testing.T) {
	r := NewRegistry()
	g := r.NewGauge("conns_open", "Open connections.", nil)
	g.Inc()
	g.Inc()
	g.Dec()

	mustContain(t, render(t, r), "# TYPE conns_open gauge", "conns_open 1")
	if g.Load() != 1 {
		t.Fatalf("gauge reads %d", g.Load())
	}
}

func TestLabelsRenderedSortedAndQuoted(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("thing_total", "A thing.", map[string]string{"zone": "b", "app": "gw"}).Inc()
	// Prometheus does not require sorted labels, but stable output makes the
	// exposition diffable in tests and in review.
	mustContain(t, render(t, r), `thing_total{app="gw",zone="b"} 1`)
}

// TestHistogramBucketsAreCumulative guards the easiest mistake in hand-rolled
// exposition: emitting per-bucket counts where Prometheus expects a running
// total, which silently corrupts every quantile downstream.
func TestHistogramBucketsAreCumulative(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("latency_seconds", "Handling latency.", []float64{0.001, 0.01, 0.1}, nil)
	h.Observe(0.0005) // bucket 0.001
	h.Observe(0.005)  // bucket 0.01
	h.Observe(0.05)   // bucket 0.1
	h.Observe(5)      // +Inf only

	body := render(t, r)
	mustContain(t, body,
		"# TYPE latency_seconds histogram",
		`latency_seconds_bucket{le="0.001"} 1`,
		`latency_seconds_bucket{le="0.01"} 2`,
		`latency_seconds_bucket{le="0.1"} 3`,
		`latency_seconds_bucket{le="+Inf"} 4`,
		"latency_seconds_count 4",
	)
	if !strings.Contains(body, "latency_seconds_sum 5.0555") {
		t.Errorf("sum missing or wrong:\n%s", body)
	}
}

func TestObservationOnBoundGoesInThatBucket(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogram("h_seconds", "h", []float64{0.01, 1}, nil)
	h.Observe(0.01) // "le" is an inclusive upper bound

	mustContain(t, render(t, r), `h_seconds_bucket{le="0.01"} 1`)
}

func TestConcurrentUpdates(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("c_total", "c", nil)
	h := r.NewHistogram("h_seconds", "h", []float64{0.1, 1}, nil)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				c.Inc()
				h.Observe(0.05)
			}
		}()
	}
	wg.Wait()

	if c.Load() != 8000 {
		t.Fatalf("counter lost updates: %d of 8000", c.Load())
	}
	mustContain(t, render(t, r), "h_seconds_count 8000")
}

func TestHealthOfEmptyRegistry(t *testing.T) {
	if body := render(t, NewRegistry()); body != "" {
		t.Fatalf("empty registry rendered %q", body)
	}
}
