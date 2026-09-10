package protocol

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"
	"testing/iotest"
	"time"
)

func samplePosition() Position {
	return Position{
		DeviceID: 0x0102030405060708,
		Seq:      42,
		At:       time.UnixMilli(1_757_000_000_000).UTC(),
		Lat:      25.2048493,
		Lon:      55.2707828,
		SpeedKph: 87,
		Heading:  271,
		Ignition: true,
	}
}

func TestPositionRoundTrip(t *testing.T) {
	want := samplePosition()
	frame := want.Encode(nil)

	rd := NewReader(bytes.NewReader(frame))
	payload, err := rd.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	got, err := DecodePosition(payload)
	if err != nil {
		t.Fatalf("DecodePosition: %v", err)
	}

	if got.DeviceID != want.DeviceID || got.Seq != want.Seq {
		t.Errorf("identity mismatch: got %d/%d want %d/%d", got.DeviceID, got.Seq, want.DeviceID, want.Seq)
	}
	if !got.At.Equal(want.At) {
		t.Errorf("time: got %v want %v", got.At, want.At)
	}
	// Fixed-point at 1e-7 degrees resolves to roughly 1.1 cm.
	if math.Abs(got.Lat-want.Lat) > 1e-7 || math.Abs(got.Lon-want.Lon) > 1e-7 {
		t.Errorf("coords: got %.7f,%.7f want %.7f,%.7f", got.Lat, got.Lon, want.Lat, want.Lon)
	}
	if got.SpeedKph != want.SpeedKph || got.Heading != want.Heading || got.Ignition != want.Ignition {
		t.Errorf("telemetry mismatch: %+v", got)
	}
}

func TestNegativeCoordinates(t *testing.T) {
	p := samplePosition()
	p.Lat, p.Lon = -33.8688197, -151.2092955
	rd := NewReader(bytes.NewReader(p.Encode(nil)))
	payload, err := rd.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePosition(payload)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(got.Lat-p.Lat) > 1e-7 || math.Abs(got.Lon-p.Lon) > 1e-7 {
		t.Fatalf("southern/western hemisphere sign lost: got %.7f,%.7f", got.Lat, got.Lon)
	}
}

// TestOneByteAtATime is the test that matters for TCP: a frame does not arrive
// in one Read, and a reader that assumes it does works on localhost and fails
// over a mobile link.
func TestOneByteAtATime(t *testing.T) {
	frame := samplePosition().Encode(nil)
	rd := NewReader(iotest.OneByteReader(bytes.NewReader(frame)))
	payload, err := rd.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame over one-byte reader: %v", err)
	}
	if _, err := DecodePosition(payload); err != nil {
		t.Fatalf("DecodePosition: %v", err)
	}
}

func TestMultipleFramesInOneStream(t *testing.T) {
	var buf []byte
	for i := 0; i < 5; i++ {
		p := samplePosition()
		p.Seq = uint32(i)
		buf = p.Encode(buf)
	}
	rd := NewReader(bytes.NewReader(buf))
	for i := 0; i < 5; i++ {
		payload, err := rd.ReadFrame()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		got, err := DecodePosition(payload)
		if err != nil {
			t.Fatalf("frame %d decode: %v", i, err)
		}
		if got.Seq != uint32(i) {
			t.Fatalf("frame %d: got seq %d", i, got.Seq)
		}
	}
	if _, err := rd.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF after last frame, got %v", err)
	}
}

// TestResyncAfterGarbage covers a device that lost power mid-write and then
// came back mid-frame. Dropping the connection here would be an over-reaction.
func TestResyncAfterGarbage(t *testing.T) {
	garbage := []byte{0x00, 0xAA, 0x13, 0x37, 0xAA, 0x00, 0xFF}
	frame := samplePosition().Encode(nil)
	rd := NewReader(bytes.NewReader(append(garbage, frame...)))

	payload, err := rd.ReadFrame()
	if err != nil {
		t.Fatalf("failed to resync: %v", err)
	}
	got, err := DecodePosition(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got.Seq != 42 {
		t.Fatalf("resynced onto the wrong frame: seq %d", got.Seq)
	}
}

func TestResyncLimit(t *testing.T) {
	rd := NewReader(bytes.NewReader(bytes.Repeat([]byte{0x00}, 1024)))
	rd.ResyncLimit = 64
	if _, err := rd.ReadFrame(); !errors.Is(err, ErrResyncLimit) {
		t.Fatalf("want ErrResyncLimit, got %v", err)
	}
}

func TestCorruptedFrameDetected(t *testing.T) {
	frame := samplePosition().Encode(nil)
	frame[10] ^= 0xFF // flip a payload bit, leave the CRC alone

	rd := NewReader(bytes.NewReader(frame))
	if _, err := rd.ReadFrame(); !errors.Is(err, ErrBadCRC) {
		t.Fatalf("corruption slipped through: %v", err)
	}
}

func TestOversizedLengthRejected(t *testing.T) {
	// A hostile or broken device claiming a 60 KB payload must not cause a
	// 60 KB allocation.
	frame := []byte{Magic0, Magic1, 0xFF, 0xFF}
	rd := NewReader(bytes.NewReader(frame))
	if _, err := rd.ReadFrame(); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
}

func TestTruncatedFrame(t *testing.T) {
	frame := samplePosition().Encode(nil)
	rd := NewReader(bytes.NewReader(frame[:len(frame)-3]))
	if _, err := rd.ReadFrame(); err == nil {
		t.Fatal("truncated frame accepted")
	}
}

func TestAckRoundTrip(t *testing.T) {
	frame := EncodeAck(nil, 99, 12345)
	rd := NewReader(bytes.NewReader(frame))
	payload, err := rd.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	id, seq, err := DecodeAck(payload)
	if err != nil {
		t.Fatal(err)
	}
	if id != 99 || seq != 12345 {
		t.Fatalf("got %d/%d", id, seq)
	}
}

func TestDecodeWrongType(t *testing.T) {
	ack := EncodeAck(nil, 1, 1)
	rd := NewReader(bytes.NewReader(ack))
	payload, _ := rd.ReadFrame()
	if _, err := DecodePosition(payload); err == nil {
		t.Fatal("position decoder accepted an ack frame")
	}
}

// CRC-16/CCITT-FALSE has a published check value for "123456789".
func TestCRC16CheckValue(t *testing.T) {
	if got := CRC16([]byte("123456789")); got != 0x29B1 {
		t.Fatalf("CRC16 check value: got 0x%04X want 0x29B1", got)
	}
}

func BenchmarkEncodeDecode(b *testing.B) {
	p := samplePosition()
	buf := make([]byte, 0, 64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = p.Encode(buf[:0])
		rd := NewReader(bytes.NewReader(buf))
		payload, _ := rd.ReadFrame()
		_, _ = DecodePosition(payload)
	}
}
