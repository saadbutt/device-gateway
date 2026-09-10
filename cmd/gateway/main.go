// Command gateway is a TCP ingest server for GPS and telemetry devices.
//
//	gateway -addr :5027 -metrics-addr :9100 -overflow block
//
// It speaks the framing in internal/protocol, acknowledges every sequence
// number, suppresses replays from reconnecting devices, and exposes Prometheus
// metrics. See README.md for the reasoning behind each knob.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/saadbutt/device-gateway/internal/config"
	"github.com/saadbutt/device-gateway/internal/gateway"
	"github.com/saadbutt/device-gateway/internal/metrics"
	"github.com/saadbutt/device-gateway/internal/pipeline"
	"github.com/saadbutt/device-gateway/internal/protocol"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := config.Parse(args)
	if err != nil {
		return err
	}

	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	// SIGINT and SIGTERM cancel the root context. Everything downstream hangs
	// off it, so shutdown is one cancellation rather than a pile of channels.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := metrics.NewRegistry()
	positions := reg.NewCounter("gateway_positions_persisted_total",
		"Positions handed to the storage handler.", nil)

	// The handler stands in for whatever the deployment actually does with a
	// position: insert into TimescaleDB, publish to Kafka, push to a socket.
	// It is deliberately the only part of this repo that is a placeholder.
	pipe := pipeline.New(ctx, pipeline.Config{
		QueueSize: cfg.QueueSize,
		Workers:   cfg.Workers,
		Policy:    cfg.Policy,
	}, func(_ context.Context, p protocol.Position) {
		positions.Inc()
		log.Debug("position",
			"device", p.DeviceID, "seq", p.Seq,
			"lat", p.Lat, "lon", p.Lon, "kph", p.SpeedKph, "at", p.At)
	})

	dedup := pipeline.NewDeduper(cfg.MaxDevices)
	registerDedupMetrics(reg, dedup)

	srv := gateway.New(gateway.Options{
		Addr:            cfg.Addr,
		MaxConns:        cfg.MaxConns,
		AcceptPerSecond: cfg.AcceptPerSecond,
		AcceptBurst:     cfg.AcceptBurst,
		IdleTimeout:     cfg.IdleTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		DrainTimeout:    cfg.DrainTimeout,
		Logger:          log,
	}, pipe, dedup, reg)

	// Bind before starting anything else so a port clash fails immediately
	// instead of after the metrics server is already up.
	if err := srv.Listen(ctx); err != nil {
		return err
	}

	adminErr := startAdminServer(ctx, cfg.MetricsAddr, reg, log)

	log.Info("gateway started",
		"devices", cfg.Addr, "admin", cfg.MetricsAddr,
		"overflow", cfg.Policy.String(), "workers", cfg.Workers)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()

	select {
	case err := <-serveErr:
		if err != nil {
			return err
		}
	case err := <-adminErr:
		return fmt.Errorf("admin server: %w", err)
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
		if err := <-serveErr; err != nil {
			return err
		}
	}

	// Workers stop when ctx ends, but they drain the queue first, so waiting
	// here is what guarantees acknowledged frames were actually handled.
	pipe.Wait()
	submitted, processed, dropped := pipe.Stats()
	accepted, dupes, tooOld, evicted, tracked := dedup.Stats()
	log.Info("shutdown complete",
		"submitted", submitted, "processed", processed, "dropped", dropped,
		"accepted", accepted, "duplicates", dupes, "too_old", tooOld,
		"evicted", evicted, "tracked_devices", tracked)
	return nil
}

// startAdminServer runs /metrics and /healthz on a separate port so the device
// port never has to serve HTTP.
func startAdminServer(ctx context.Context, addr string, reg *metrics.Registry, log *slog.Logger) <-chan error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", reg.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Warn("admin server shutdown", "err", err)
		}
	}()
	return errc
}

// registerDedupMetrics wires the deduper's counters into the registry. They are
// sampled on scrape rather than incremented in the hot path.
func registerDedupMetrics(reg *metrics.Registry, d *pipeline.Deduper) {
	accepted := reg.NewCounter("dedup_accepted_total", "Frames accepted as new.", nil)
	dupes := reg.NewCounter("dedup_duplicates_total", "Frames suppressed as replays.", nil)
	tooOld := reg.NewCounter("dedup_out_of_window_total", "Frames older than the replay window.", nil)
	evicted := reg.NewCounter("dedup_devices_evicted_total", "Device windows evicted by the LRU cap.", nil)
	tracked := reg.NewGauge("dedup_devices_tracked", "Device windows currently held.", nil)

	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			a, d2, o, e, n := d.Stats()
			accepted.Set(a)
			dupes.Set(d2)
			tooOld.Set(o)
			evicted.Set(e)
			tracked.Set(int64(n))
		}
	}()
}

func newLogger(level string) *slog.Logger {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(level)); err != nil {
		lv = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv}))
}
