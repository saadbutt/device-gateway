// Package gateway is the TCP front door: it accepts device connections,
// survives them all reconnecting at once, and hands decoded positions to the
// pipeline.
package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/saadbutt/device-gateway/internal/metrics"
	"github.com/saadbutt/device-gateway/internal/pipeline"
	"github.com/saadbutt/device-gateway/internal/protocol"
)

// Options configures a Server.
type Options struct {
	// Addr is the TCP listen address.
	Addr string

	// MaxConns caps concurrent device connections. Beyond it, new connections
	// are accepted and immediately closed rather than queued, so the kernel
	// backlog does not turn into an invisible unbounded buffer.
	MaxConns int

	// AcceptBurst and AcceptPerSecond throttle how fast new connections are
	// admitted.
	//
	// This is the reconnect-storm valve. When a mobile carrier drops a region,
	// every device in it reconnects within the same few seconds. Without a
	// limit the gateway spends its CPU on TLS handshakes and goroutine
	// creation while the devices that are already connected time out, which
	// makes them reconnect too. Admitting them at a steady rate is slower for
	// any single device and dramatically faster for the fleet.
	AcceptBurst     int
	AcceptPerSecond float64

	// IdleTimeout closes a connection that has sent nothing for this long.
	//
	// Half-open sockets are the default failure mode behind carrier NAT: the
	// device is gone, the carrier silently dropped the mapping, and the
	// gateway's socket looks perfectly healthy until it tries to write. An
	// application-level idle deadline is the only reliable detector.
	IdleTimeout time.Duration

	// WriteTimeout bounds a single ack write.
	WriteTimeout time.Duration

	// DrainTimeout is how long Shutdown waits for connections to finish.
	DrainTimeout time.Duration

	Logger *slog.Logger
}

func (o *Options) setDefaults() {
	if o.MaxConns <= 0 {
		o.MaxConns = 10000
	}
	if o.AcceptBurst <= 0 {
		o.AcceptBurst = 200
	}
	if o.AcceptPerSecond <= 0 {
		o.AcceptPerSecond = 500
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = 90 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 5 * time.Second
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = 15 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Server accepts device connections.
type Server struct {
	opts Options

	mu      sync.Mutex
	ln      net.Listener
	pipe    *pipeline.Pipeline
	dedup   *pipeline.Deduper
	limiter *rateLimiter
	sem     chan struct{}

	m struct {
		connsOpen     *metrics.Gauge
		connsTotal    *metrics.Counter
		connsRejected *metrics.Counter
		framesTotal   *metrics.Counter
		framesBad     *metrics.Counter
		framesDup     *metrics.Counter
		framesDropped *metrics.Counter
		idleTimeouts  *metrics.Counter
		queueDepth    *metrics.Gauge
		frameLatency  *metrics.Histogram
	}

	wg     sync.WaitGroup
	closed chan struct{}
	once   sync.Once
}

// New builds a Server. Call Serve to start it.
func New(opts Options, pipe *pipeline.Pipeline, dedup *pipeline.Deduper, reg *metrics.Registry) *Server {
	opts.setDefaults()
	s := &Server{
		opts:    opts,
		pipe:    pipe,
		dedup:   dedup,
		limiter: newRateLimiter(opts.AcceptPerSecond, opts.AcceptBurst),
		sem:     make(chan struct{}, opts.MaxConns),
		closed:  make(chan struct{}),
	}
	s.m.connsOpen = reg.NewGauge("gateway_connections_open", "Device connections currently established.", nil)
	s.m.connsTotal = reg.NewCounter("gateway_connections_total", "Device connections accepted since start.", nil)
	s.m.connsRejected = reg.NewCounter("gateway_connections_rejected_total", "Connections closed immediately due to limits.", nil)
	s.m.framesTotal = reg.NewCounter("gateway_frames_total", "Frames decoded successfully.", nil)
	s.m.framesBad = reg.NewCounter("gateway_frames_invalid_total", "Frames rejected by framing or CRC.", nil)
	s.m.framesDup = reg.NewCounter("gateway_frames_duplicate_total", "Frames dropped as replays.", nil)
	s.m.framesDropped = reg.NewCounter("gateway_frames_dropped_total", "Frames dropped by queue overflow.", nil)
	s.m.idleTimeouts = reg.NewCounter("gateway_idle_timeouts_total", "Connections closed after idle timeout.", nil)
	s.m.queueDepth = reg.NewGauge("gateway_queue_depth", "Positions waiting in the ingest queue.", nil)
	s.m.frameLatency = reg.NewHistogram("gateway_frame_handling_seconds",
		"Time from frame decode to pipeline submission.",
		[]float64{0.0001, 0.001, 0.01, 0.1, 1}, nil)
	return s
}

// Addr reports the bound address, which is how callers discover the port when
// Options.Addr used ":0". It returns nil before Listen has succeeded.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Listen binds the socket without accepting anything yet.
//
// Binding is separate from serving so that a caller can learn the port, or
// fail fast on a port clash, before any goroutines are running. Serve calls it
// automatically if you skip it.
func (s *Server) Listen(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return nil
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.opts.Addr)
	if err != nil {
		return fmt.Errorf("gateway: listen %s: %w", s.opts.Addr, err)
	}
	s.ln = ln
	s.opts.Logger.Info("gateway listening", "addr", ln.Addr().String())
	return nil
}

// Serve accepts connections until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	if err := s.Listen(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.once.Do(func() { close(s.closed) })
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return s.drain()
			default:
			}
			// A per-connection accept error (EMFILE, ECONNABORTED) must not
			// kill the listener. Back off briefly and keep going.
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			s.opts.Logger.Warn("accept failed", "err", err)
			select {
			case <-time.After(50 * time.Millisecond):
				continue
			case <-s.closed:
				return s.drain()
			}
		}

		if !s.admit(ctx, conn) {
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-s.sem }()
			s.handle(ctx, conn)
		}()
	}
}

// admit applies the connection cap and the accept rate limit.
func (s *Server) admit(ctx context.Context, conn net.Conn) bool {
	select {
	case s.sem <- struct{}{}:
	default:
		s.m.connsRejected.Inc()
		s.opts.Logger.Warn("connection rejected: at capacity", "remote", conn.RemoteAddr().String())
		_ = conn.Close()
		return false
	}

	if !s.limiter.allow(ctx) {
		<-s.sem
		s.m.connsRejected.Inc()
		_ = conn.Close()
		return false
	}

	s.m.connsTotal.Inc()
	s.m.connsOpen.Inc()
	return true
}

func (s *Server) drain() error {
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		s.opts.Logger.Info("all connections closed")
		return nil
	case <-time.After(s.opts.DrainTimeout):
		return errors.New("gateway: drain timeout, connections still open")
	}
}

// handle runs one connection's read loop.
func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer s.m.connsOpen.Dec()
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	log := s.opts.Logger.With("remote", remote)

	if tc, ok := conn.(*net.TCPConn); ok {
		// Keepalive is a backstop, not the primary detector. It is measured in
		// minutes on most kernels, which is far too slow for a fleet dashboard,
		// hence the application-level idle timeout below.
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
		_ = tc.SetNoDelay(true)
	}

	// Closing the socket is what unblocks a goroutine parked in Read. A
	// context alone cannot interrupt it.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	br := bufio.NewReaderSize(conn, 8<<10)
	rd := protocol.NewReader(br)
	bw := bufio.NewWriterSize(conn, 1<<10)

	var ackBuf []byte
	for {
		if err := conn.SetReadDeadline(time.Now().Add(s.opts.IdleTimeout)); err != nil {
			return
		}

		payload, err := rd.ReadFrame()
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				log.Debug("device disconnected")
			case errors.Is(err, protocol.ErrBadCRC), errors.Is(err, protocol.ErrTooLarge),
				errors.Is(err, protocol.ErrShortPayload), errors.Is(err, protocol.ErrResyncLimit):
				s.m.framesBad.Inc()
				log.Warn("dropping connection after frame error", "err", err)
			default:
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					s.m.idleTimeouts.Inc()
					log.Info("idle timeout, closing")
				} else {
					log.Debug("read ended", "err", err)
				}
			}
			return
		}

		start := time.Now()
		pos, err := protocol.DecodePosition(payload)
		if err != nil {
			s.m.framesBad.Inc()
			log.Warn("undecodable payload", "err", err)
			continue
		}
		s.m.framesTotal.Inc()

		// Acknowledge before deciding whether to process. A duplicate still
		// needs its ack, because the device is resending precisely because it
		// never received the first one.
		dup := s.dedup.Seen(pos.DeviceID, pos.Seq)
		if dup {
			s.m.framesDup.Inc()
		} else if err := s.pipe.Submit(ctx, pos); err != nil {
			s.m.framesDropped.Inc()
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Warn("frame dropped", "device", pos.DeviceID, "seq", pos.Seq, "err", err)
		}

		ackBuf = protocol.EncodeAck(ackBuf[:0], pos.DeviceID, pos.Seq)
		if err := conn.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout)); err != nil {
			return
		}
		if _, err := bw.Write(ackBuf); err != nil {
			log.Debug("ack write failed", "err", err)
			return
		}
		if err := bw.Flush(); err != nil {
			log.Debug("ack flush failed", "err", err)
			return
		}

		s.m.frameLatency.Observe(time.Since(start).Seconds())
		s.m.queueDepth.Set(int64(s.pipe.Depth()))
	}
}
