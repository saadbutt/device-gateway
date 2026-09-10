package gateway

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saadbutt/device-gateway/internal/metrics"
	"github.com/saadbutt/device-gateway/internal/pipeline"
	"github.com/saadbutt/device-gateway/internal/protocol"
)

type harness struct {
	srv      *Server
	addr     string
	received *atomic.Int64
	dedup    *pipeline.Deduper
	cancel   context.CancelFunc

	serveOnce sync.Once
	serveErr  error
	done      chan error
}

// waitServe blocks until Serve returns and caches the result, so both the test
// body and the cleanup hook can call it.
func (h *harness) waitServe(t *testing.T, d time.Duration) error {
	t.Helper()
	h.serveOnce.Do(func() {
		select {
		case h.serveErr = <-h.done:
		case <-time.After(d):
			t.Errorf("Serve did not return within %v of cancellation", d)
		}
	})
	return h.serveErr
}

func newHarness(t *testing.T, opts Options) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	var received atomic.Int64
	pipe := pipeline.New(ctx, pipeline.Config{QueueSize: 1024, Workers: 4, Policy: pipeline.Block},
		func(context.Context, protocol.Position) { received.Add(1) })
	dedup := pipeline.NewDeduper(1000)

	opts.Addr = "127.0.0.1:0"
	opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(opts, pipe, dedup, metrics.NewRegistry())

	// Bind before serving so the test knows the port without polling.
	if err := srv.Listen(ctx); err != nil {
		cancel()
		t.Fatalf("listen: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()

	h := &harness{srv: srv, addr: srv.Addr().String(), received: &received, dedup: dedup, cancel: cancel, done: done}
	t.Cleanup(func() {
		cancel()
		h.waitServe(t, 5*time.Second)
		pipe.Wait()
	})
	return h
}

// sendAndAwaitAcks writes n positions starting at startSeq and reads back the
// same number of acks.
func sendAndAwaitAcks(t *testing.T, conn net.Conn, deviceID uint64, startSeq uint32, n int) []uint32 {
	t.Helper()
	var buf []byte
	for i := 0; i < n; i++ {
		buf = protocol.Position{
			DeviceID: deviceID, Seq: startSeq + uint32(i), At: time.Now(),
			Lat: 25.2, Lon: 55.27, SpeedKph: 60,
		}.Encode(buf)
	}
	if _, err := conn.Write(buf); err != nil {
		t.Fatalf("write: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	rd := protocol.NewReader(bufio.NewReader(conn))
	acks := make([]uint32, 0, n)
	for i := 0; i < n; i++ {
		payload, err := rd.ReadFrame()
		if err != nil {
			t.Fatalf("reading ack %d/%d: %v", i+1, n, err)
		}
		_, seq, err := protocol.DecodeAck(payload)
		if err != nil {
			t.Fatalf("decode ack: %v", err)
		}
		acks = append(acks, seq)
	}
	return acks
}

func TestEndToEndIngest(t *testing.T) {
	h := newHarness(t, Options{})

	conn, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	acks := sendAndAwaitAcks(t, conn, 1001, 1, 20)
	if len(acks) != 20 {
		t.Fatalf("got %d acks", len(acks))
	}
	for i, seq := range acks {
		if seq != uint32(i+1) {
			t.Fatalf("ack %d out of order: %d", i, seq)
		}
	}

	waitFor(t, func() bool { return h.received.Load() == 20 })
}

// TestReconnectReplayIsDeduplicated is the headline behaviour: a device that
// reconnects and replays its unacknowledged buffer must be acknowledged again
// but must not be counted twice.
func TestReconnectReplayIsDeduplicated(t *testing.T) {
	h := newHarness(t, Options{})

	first, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	sendAndAwaitAcks(t, first, 2002, 1, 10)
	waitFor(t, func() bool { return h.received.Load() == 10 })

	// The link dies before the device processed those acks.
	_ = first.(*net.TCPConn).SetLinger(0)
	first.Close()

	second, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	// It replays 1..10 and then sends 11..15.
	acks := sendAndAwaitAcks(t, second, 2002, 1, 15)
	if len(acks) != 15 {
		t.Fatalf("replayed frames must still be acknowledged, got %d acks", len(acks))
	}

	waitFor(t, func() bool { return h.received.Load() == 15 })
	if got := h.received.Load(); got != 15 {
		t.Fatalf("double counted: pipeline saw %d positions, want 15", got)
	}

	_, dupes, _, _, _ := h.dedup.Stats()
	if dupes != 10 {
		t.Fatalf("expected 10 duplicates suppressed, got %d", dupes)
	}
}

func TestIdleTimeoutClosesHalfOpenConnection(t *testing.T) {
	h := newHarness(t, Options{IdleTimeout: 150 * time.Millisecond})

	conn, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	sendAndAwaitAcks(t, conn, 3003, 1, 1)

	// Go quiet, as a device does when the carrier drops its NAT mapping.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected the server to close an idle connection")
	}
}

func TestMaxConnsRejectsBeyondCapacity(t *testing.T) {
	h := newHarness(t, Options{MaxConns: 2, IdleTimeout: 5 * time.Second})

	var held []net.Conn
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		c, err := net.Dial("tcp", h.addr)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
		sendAndAwaitAcks(t, c, uint64(4000+i), 1, 1)
	}

	extra, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	_ = extra.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadAll(extra); err != nil && !isTimeout(err) {
		t.Logf("read error as expected: %v", err)
	}
	// The third connection must be closed promptly rather than parked.
	buf := make([]byte, 1)
	_ = extra.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := extra.Read(buf); err == nil {
		t.Fatal("connection beyond MaxConns was not closed")
	}
}

// TestReconnectStorm approximates a carrier outage recovering: many devices
// connect at once and every frame must still be accounted for.
func TestReconnectStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("storm test skipped in short mode")
	}
	const devices, framesEach = 60, 10
	h := newHarness(t, Options{AcceptPerSecond: 2000, AcceptBurst: 50, MaxConns: 200})

	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, devices)

	for i := 0; i < devices; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			<-start
			conn, err := net.Dial("tcp", h.addr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()

			var buf []byte
			for s := 1; s <= framesEach; s++ {
				buf = protocol.Position{DeviceID: id, Seq: uint32(s), At: time.Now()}.Encode(buf)
			}
			if _, err := conn.Write(buf); err != nil {
				errs <- err
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			rd := protocol.NewReader(bufio.NewReader(conn))
			for s := 0; s < framesEach; s++ {
				if _, err := rd.ReadFrame(); err != nil {
					errs <- err
					return
				}
			}
		}(uint64(5000 + i))
	}

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("device failed during storm: %v", err)
	}

	want := int64(devices * framesEach)
	waitFor(t, func() bool { return h.received.Load() == want })
	if got := h.received.Load(); got != want {
		t.Fatalf("storm lost data: got %d of %d", got, want)
	}
}

func TestGracefulShutdownReturns(t *testing.T) {
	h := newHarness(t, Options{DrainTimeout: 2 * time.Second})
	conn, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	sendAndAwaitAcks(t, conn, 6006, 1, 1)

	h.cancel()
	if err := h.waitServe(t, 5*time.Second); err != nil {
		t.Fatalf("graceful shutdown reported %v", err)
	}
}

func TestGarbageDoesNotKillTheServer(t *testing.T) {
	h := newHarness(t, Options{})

	bad, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = bad.Write([]byte{0xAA, 0x55, 0x00, 0x05, 1, 2, 3, 4, 5, 0xDE, 0xAD}) // bad CRC
	bad.Close()

	good, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer good.Close()
	if acks := sendAndAwaitAcks(t, good, 7007, 1, 3); len(acks) != 3 {
		t.Fatal("server stopped serving after a malformed frame")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 3s")
}

func isTimeout(err error) bool {
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}
