// Command simulator emulates a fleet of GPS trackers against the gateway.
//
// It exists because the interesting behaviour of an ingest gateway only shows
// up under conditions that are tedious to reproduce by hand: devices that drop
// mid-session, buffer while offline, reconnect with backoff and replay
// everything the gateway never acknowledged.
//
//	simulator -addr localhost:5027 -devices 200 -drop-rate 0.05
//	simulator -addr localhost:5027 -devices 500 -storm 30s
//
// Watch gateway_frames_duplicate_total on the gateway's /metrics while it runs.
// That counter going up is the deduplication window doing its job.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/saadbutt/device-gateway/internal/protocol"
)

type stats struct {
	sent        atomic.Uint64
	acked       atomic.Uint64
	replayed    atomic.Uint64
	reconnects  atomic.Uint64
	dialFailure atomic.Uint64
}

func main() {
	var (
		addr     = flag.String("addr", "localhost:5027", "gateway address")
		devices  = flag.Int("devices", 50, "number of simulated devices")
		interval = flag.Duration("interval", 2*time.Second, "reporting interval per device")
		dropRate = flag.Float64("drop-rate", 0.02, "probability per report that the link drops")
		storm    = flag.Duration("storm", 0, "if set, drop every device simultaneously this often")
		duration = flag.Duration("duration", 0, "stop after this long; zero runs until interrupted")
		batch    = flag.Int("batch", 1, "records flushed per interval, as buffering trackers do")
		seed     = flag.Int64("seed", time.Now().UnixNano(), "random seed for reproducible runs")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	var st stats
	var wg sync.WaitGroup

	// stormCh fans a single drop signal out to every device.
	stormCh := make([]chan struct{}, *devices)
	for i := range stormCh {
		stormCh[i] = make(chan struct{}, 1)
	}
	if *storm > 0 {
		go func() {
			t := time.NewTicker(*storm)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					log.Warn("simulating carrier outage: dropping every device")
					for _, ch := range stormCh {
						select {
						case ch <- struct{}{}:
						default:
						}
					}
				}
			}
		}()
	}

	log.Info("simulator starting",
		"devices", *devices, "addr", *addr, "interval", *interval,
		"batch", *batch, "drop_rate", *dropRate, "seed", *seed)

	// Base ID is a uint64 constant, not an untyped literal: an untyped
	// 9e14 overflows int on 32-bit targets and this binary cross-compiles
	// to armv7 for single-board hardware.
	const baseDeviceID uint64 = 900000000000000

	for i := 0; i < *devices; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			d := &device{
				id:       baseDeviceID + uint64(idx),
				addr:     *addr,
				interval: *interval,
				dropRate: *dropRate,
				batch:    *batch,
				rng:      rand.New(rand.NewSource(*seed + int64(idx))),
				stats:    &st,
				log:      log.With("device", baseDeviceID+uint64(idx)),
				storm:    stormCh[idx],
			}
			d.run(ctx)
		}(i)
	}

	go reportLoop(ctx, &st, log)
	wg.Wait()

	fmt.Printf("\nsent=%d acked=%d replayed=%d reconnects=%d dial_failures=%d\n",
		st.sent.Load(), st.acked.Load(), st.replayed.Load(),
		st.reconnects.Load(), st.dialFailure.Load())
}

func reportLoop(ctx context.Context, st *stats, log *slog.Logger) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			log.Info("progress",
				"sent", st.sent.Load(), "acked", st.acked.Load(),
				"replayed", st.replayed.Load(), "reconnects", st.reconnects.Load())
		}
	}
}

// device is one simulated tracker.
type device struct {
	id       uint64
	addr     string
	interval time.Duration
	dropRate float64
	batch    int
	rng      *rand.Rand
	stats    *stats
	log      *slog.Logger
	storm    <-chan struct{}

	seq     uint32
	pending []protocol.Position // sent but not yet acknowledged
	lat     float64
	lon     float64
	heading float64
}

func (d *device) run(ctx context.Context) {
	// Scatter across Dubai so the output looks like a fleet rather than a dot.
	d.lat = 25.05 + d.rng.Float64()*0.25
	d.lon = 55.10 + d.rng.Float64()*0.30
	d.heading = d.rng.Float64() * 360

	attempt := 0
	for ctx.Err() == nil {
		conn, err := d.dial(ctx)
		if err != nil {
			d.stats.dialFailure.Add(1)
			attempt++
			if !sleepCtx(ctx, backoff(attempt, d.rng)) {
				return
			}
			continue
		}
		attempt = 0
		d.session(ctx, conn)
		d.stats.reconnects.Add(1)
		if !sleepCtx(ctx, backoff(1, d.rng)) {
			return
		}
	}
}

func (d *device) dial(ctx context.Context) (net.Conn, error) {
	var dialer net.Dialer
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return dialer.DialContext(dialCtx, "tcp", d.addr)
}

// session sends until the link "drops". On entry it replays everything that
// was never acknowledged, which is exactly what real firmware does and exactly
// what the gateway's deduplication window has to absorb.
func (d *device) session(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	acks := make(chan uint32, 256)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		rd := protocol.NewReader(bufio.NewReader(conn))
		for {
			payload, err := rd.ReadFrame()
			if err != nil {
				return
			}
			if _, seq, err := protocol.DecodeAck(payload); err == nil {
				select {
				case acks <- seq:
				default:
				}
			}
		}
	}()

	if len(d.pending) > 0 {
		d.log.Info("replaying unacknowledged buffer", "frames", len(d.pending))
		for _, p := range d.pending {
			if err := d.write(conn, p); err != nil {
				return
			}
			d.stats.replayed.Add(1)
		}
	}

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-readDone:
			return
		case <-d.storm:
			d.log.Debug("dropped by storm")
			return
		case seq := <-acks:
			d.clearPending(seq)
			d.stats.acked.Add(1)
		case <-ticker.C:
			if d.rng.Float64() < d.dropRate {
				d.log.Debug("link dropped", "unacked", len(d.pending))
				return
			}
			n := d.batch
			if n < 1 {
				n = 1
			}
			for i := 0; i < n; i++ {
				p := d.nextPosition()
				d.pending = append(d.pending, p)
				// Firmware buffers are finite. Once full the oldest record is
				// overwritten, which is why the gateway can legitimately see
				// a gap rather than a duplicate.
				if len(d.pending) > 512 {
					d.pending = d.pending[len(d.pending)-512:]
				}
				if err := d.write(conn, p); err != nil {
					return
				}
				d.stats.sent.Add(1)
			}
		}
	}
}

func (d *device) write(conn net.Conn, p protocol.Position) error {
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	_, err := conn.Write(p.Encode(nil))
	return err
}

func (d *device) clearPending(ackedSeq uint32) {
	for i, p := range d.pending {
		if p.Seq == ackedSeq {
			d.pending = append(d.pending[:i], d.pending[i+1:]...)
			return
		}
	}
}

// nextPosition walks the device along a slowly curving path.
func (d *device) nextPosition() protocol.Position {
	d.seq++
	d.heading += (d.rng.Float64() - 0.5) * 20
	speed := 20 + d.rng.Float64()*80
	dist := speed * d.interval.Seconds() / 3600 // km
	rad := d.heading * math.Pi / 180
	d.lat += dist / 111.0 * math.Cos(rad)
	d.lon += dist / (111.0 * math.Cos(d.lat*math.Pi/180)) * math.Sin(rad)

	return protocol.Position{
		DeviceID: d.id,
		Seq:      d.seq,
		At:       time.Now().UTC(),
		Lat:      d.lat,
		Lon:      d.lon,
		SpeedKph: uint16(speed),
		Heading:  uint16(int(d.heading+360) % 360),
		Ignition: true,
	}
}

// backoff is exponential with full jitter, capped at 30 seconds.
//
// The jitter is the important half. Without it, every device knocked offline by
// the same outage retries at the same instant, and the gateway is hit by a
// synchronised wave on every retry rather than just the first.
func backoff(attempt int, rng *rand.Rand) time.Duration {
	const base, max = 500 * time.Millisecond, 30 * time.Second
	d := time.Duration(float64(base) * math.Pow(2, float64(attempt)))
	if d > max || d <= 0 {
		d = max
	}
	return time.Duration(rng.Float64() * float64(d))
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
