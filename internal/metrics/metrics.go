// Package metrics exposes Prometheus text-format metrics without pulling in a
// client library.
//
// The exposition format is stable and trivial; a gateway that has to run on a
// Raspberry Pi in a truck depot benefits more from a small static binary and
// zero supply chain than from the convenience of prometheus/client_golang.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()         { c.v.Add(1) }
func (c *Counter) Add(n uint64) { c.v.Add(n) }
func (c *Counter) Load() uint64 { return c.v.Load() }
func (c *Counter) Set(n uint64) { c.v.Store(n) }

// Gauge is a value that goes up and down.
type Gauge struct{ v atomic.Int64 }

func (g *Gauge) Inc()        { g.v.Add(1) }
func (g *Gauge) Dec()        { g.v.Add(-1) }
func (g *Gauge) Set(n int64) { g.v.Store(n) }
func (g *Gauge) Load() int64 { return g.v.Load() }

// Histogram accumulates observations into fixed buckets.
//
// Buckets are cumulative in the exposition, as Prometheus requires, but stored
// as per-bucket counts because that is cheaper to increment.
type Histogram struct {
	bounds []float64
	counts []atomic.Uint64
	sum    atomic.Uint64 // float64 bits, updated with CAS
	total  atomic.Uint64
}

// NewHistogram returns a Histogram with the given upper bounds, in seconds.
func NewHistogram(bounds ...float64) *Histogram {
	b := append([]float64(nil), bounds...)
	sort.Float64s(b)
	return &Histogram{bounds: b, counts: make([]atomic.Uint64, len(b)+1)}
}

// Observe records v.
func (h *Histogram) Observe(v float64) {
	// Buckets use inclusive upper bounds (Prometheus "le"), so an observation
	// belongs in the first bucket whose bound is >= v. That is exactly what
	// SearchFloat64s returns, and len(bounds) for the overflow bucket.
	i := sort.SearchFloat64s(h.bounds, v)
	h.counts[i].Add(1)
	h.total.Add(1)
	for {
		old := h.sum.Load()
		next := float64FromBits(old) + v
		if h.sum.CompareAndSwap(old, float64ToBits(next)) {
			return
		}
	}
}

// Registry holds named metrics and renders them.
type Registry struct {
	mu    sync.RWMutex
	order []string
	items map[string]*item
}

type item struct {
	kind   string
	help   string
	metric any
	labels map[string]string
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{items: make(map[string]*item)}
}

func (r *Registry) register(name, kind, help string, m any, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.items[name]; !dup {
		r.order = append(r.order, name)
	}
	r.items[name] = &item{kind: kind, help: help, metric: m, labels: labels}
}

// NewCounter registers and returns a Counter.
func (r *Registry) NewCounter(name, help string, labels map[string]string) *Counter {
	c := &Counter{}
	r.register(name, "counter", help, c, labels)
	return c
}

// NewGauge registers and returns a Gauge.
func (r *Registry) NewGauge(name, help string, labels map[string]string) *Gauge {
	g := &Gauge{}
	r.register(name, "gauge", help, g, labels)
	return g
}

// NewHistogram registers and returns a Histogram.
func (r *Registry) NewHistogram(name, help string, bounds []float64, labels map[string]string) *Histogram {
	h := NewHistogram(bounds...)
	r.register(name, "histogram", help, h, labels)
	return h
}

// Handler serves the registry in Prometheus text exposition format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var b strings.Builder
		r.write(&b)
		_, _ = w.Write([]byte(b.String()))
	})
}

func (r *Registry) write(b *strings.Builder) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, name := range r.order {
		it := r.items[name]
		fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, it.help, name, it.kind)
		lbl := renderLabels(it.labels, "")
		switch m := it.metric.(type) {
		case *Counter:
			fmt.Fprintf(b, "%s%s %d\n", name, lbl, m.Load())
		case *Gauge:
			fmt.Fprintf(b, "%s%s %d\n", name, lbl, m.Load())
		case *Histogram:
			var cum uint64
			for i, bound := range m.bounds {
				cum += m.counts[i].Load()
				fmt.Fprintf(b, "%s_bucket%s %d\n", name,
					renderLabels(it.labels, strconv.FormatFloat(bound, 'g', -1, 64)), cum)
			}
			cum += m.counts[len(m.bounds)].Load()
			fmt.Fprintf(b, "%s_bucket%s %d\n", name, renderLabels(it.labels, "+Inf"), cum)
			fmt.Fprintf(b, "%s_sum%s %g\n", name, lbl, float64FromBits(m.sum.Load()))
			fmt.Fprintf(b, "%s_count%s %d\n", name, lbl, m.total.Load())
		}
	}
}

func renderLabels(labels map[string]string, le string) string {
	if len(labels) == 0 && le == "" {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys)+1)
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, labels[k]))
	}
	if le != "" {
		parts = append(parts, fmt.Sprintf("le=%q", le))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
