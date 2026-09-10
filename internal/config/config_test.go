package config

import (
	"testing"
	"time"

	"github.com/saadbutt/device-gateway/internal/pipeline"
)

func TestDefaultsAreValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("shipped defaults do not validate: %v", err)
	}
}

func TestFlagsOverrideDefaults(t *testing.T) {
	c, err := Parse([]string{"-addr", ":6000", "-workers", "16", "-overflow", "drop_oldest"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":6000" || c.Workers != 16 || c.Policy != pipeline.DropOldest {
		t.Fatalf("flags not applied: %+v", c)
	}
}

func TestEnvApplied(t *testing.T) {
	t.Setenv("GATEWAY_ADDR", ":7777")
	t.Setenv("GATEWAY_IDLE_TIMEOUT", "45s")
	c, err := Parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":7777" || c.IdleTimeout != 45*time.Second {
		t.Fatalf("env not applied: %+v", c)
	}
}

func TestFlagBeatsEnv(t *testing.T) {
	t.Setenv("GATEWAY_ADDR", ":7777")
	c, err := Parse([]string{"-addr", ":8888"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":8888" {
		t.Fatalf("env overrode an explicit flag: %s", c.Addr)
	}
}

func TestUnknownPolicyRejected(t *testing.T) {
	if _, err := Parse([]string{"-overflow", "explode"}); err == nil {
		t.Fatal("unknown overflow policy accepted")
	}
}

func TestIdleMustExceedWriteTimeout(t *testing.T) {
	c := Default()
	c.IdleTimeout = time.Second
	c.WriteTimeout = time.Second
	if err := c.Validate(); err == nil {
		t.Fatal("accepted an idle timeout that cannot outlast a write")
	}
}
