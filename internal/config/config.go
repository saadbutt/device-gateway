// Package config turns flags and environment variables into gateway options.
package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/saadbutt/device-gateway/internal/pipeline"
)

// Config is the full runtime configuration of the gateway.
type Config struct {
	Addr        string
	MetricsAddr string

	MaxConns        int
	AcceptPerSecond float64
	AcceptBurst     int
	IdleTimeout     time.Duration
	WriteTimeout    time.Duration
	DrainTimeout    time.Duration

	QueueSize int
	Workers   int
	Policy    pipeline.OverflowPolicy

	MaxDevices int
	LogLevel   string
}

// Default returns the configuration used when nothing is overridden.
func Default() Config {
	return Config{
		Addr:            ":5027", // the port the common tracker protocols squat on
		MetricsAddr:     ":9100",
		MaxConns:        10000,
		AcceptPerSecond: 500,
		AcceptBurst:     200,
		IdleTimeout:     90 * time.Second,
		WriteTimeout:    5 * time.Second,
		DrainTimeout:    15 * time.Second,
		QueueSize:       4096,
		Workers:         8,
		Policy:          pipeline.Block,
		MaxDevices:      50000,
		LogLevel:        "info",
	}
}

// Parse reads flags, falling back to GATEWAY_* environment variables and then
// to Default. Flags win over the environment.
func Parse(args []string) (Config, error) {
	c := Default()
	applyEnv(&c)

	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.StringVar(&c.Addr, "addr", c.Addr, "TCP listen address for devices")
	fs.StringVar(&c.MetricsAddr, "metrics-addr", c.MetricsAddr, "HTTP address for /metrics and /healthz")
	fs.IntVar(&c.MaxConns, "max-conns", c.MaxConns, "maximum concurrent device connections")
	fs.Float64Var(&c.AcceptPerSecond, "accept-rate", c.AcceptPerSecond, "sustained accepts per second")
	fs.IntVar(&c.AcceptBurst, "accept-burst", c.AcceptBurst, "accept burst allowance")
	fs.DurationVar(&c.IdleTimeout, "idle-timeout", c.IdleTimeout, "close a connection idle for this long")
	fs.DurationVar(&c.WriteTimeout, "write-timeout", c.WriteTimeout, "deadline for a single ack write")
	fs.DurationVar(&c.DrainTimeout, "drain-timeout", c.DrainTimeout, "how long shutdown waits for connections")
	fs.IntVar(&c.QueueSize, "queue-size", c.QueueSize, "ingest queue depth")
	fs.IntVar(&c.Workers, "workers", c.Workers, "pipeline worker goroutines")
	fs.IntVar(&c.MaxDevices, "max-devices", c.MaxDevices, "devices tracked for deduplication")
	fs.StringVar(&c.LogLevel, "log-level", c.LogLevel, "debug, info, warn or error")
	policy := fs.String("overflow", c.Policy.String(), "queue overflow policy: block, drop_oldest or drop_newest")

	if err := fs.Parse(args); err != nil {
		return c, err
	}
	p, err := ParsePolicy(*policy)
	if err != nil {
		return c, err
	}
	c.Policy = p
	return c, c.Validate()
}

// ParsePolicy maps a policy name to its constant.
func ParsePolicy(s string) (pipeline.OverflowPolicy, error) {
	switch s {
	case "block":
		return pipeline.Block, nil
	case "drop_oldest":
		return pipeline.DropOldest, nil
	case "drop_newest":
		return pipeline.DropNewest, nil
	default:
		return pipeline.Block, fmt.Errorf("config: unknown overflow policy %q", s)
	}
}

// Validate rejects combinations that would misbehave at runtime.
func (c Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("config: addr is required")
	}
	if c.MaxConns <= 0 {
		return fmt.Errorf("config: max-conns must be positive, got %d", c.MaxConns)
	}
	if c.Workers <= 0 {
		return fmt.Errorf("config: workers must be positive, got %d", c.Workers)
	}
	if c.QueueSize <= 0 {
		return fmt.Errorf("config: queue-size must be positive, got %d", c.QueueSize)
	}
	if c.AcceptPerSecond <= 0 {
		return fmt.Errorf("config: accept-rate must be positive, got %v", c.AcceptPerSecond)
	}
	if c.IdleTimeout <= c.WriteTimeout {
		return fmt.Errorf("config: idle-timeout (%v) must exceed write-timeout (%v)", c.IdleTimeout, c.WriteTimeout)
	}
	return nil
}

func applyEnv(c *Config) {
	if v := os.Getenv("GATEWAY_ADDR"); v != "" {
		c.Addr = v
	}
	if v := os.Getenv("GATEWAY_METRICS_ADDR"); v != "" {
		c.MetricsAddr = v
	}
	if v := os.Getenv("GATEWAY_LOG_LEVEL"); v != "" {
		c.LogLevel = v
	}
	if v := os.Getenv("GATEWAY_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.MaxConns = n
		}
	}
	if v := os.Getenv("GATEWAY_IDLE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.IdleTimeout = d
		}
	}
	if v := os.Getenv("GATEWAY_OVERFLOW"); v != "" {
		if p, err := ParsePolicy(v); err == nil {
			c.Policy = p
		}
	}
}
