// Package protocol implements the wire format spoken between field devices
// and the gateway.
//
// The framing is deliberately close to what cheap GPS/telemetry trackers
// actually emit: a magic prefix, a length, a fixed-size binary payload and a
// CRC. It is not JSON over newlines, because that is not what shows up on
// port 5027 in the real world.
//
//	+--------+--------+------------+-----------------+--------+--------+
//	|  0xAA  |  0x55  | len uint16 |  payload (len)  |    crc uint16   |
//	+--------+--------+------------+-----------------+--------+--------+
//
// All multi-byte integers are big endian. The CRC is CRC-16/CCITT-FALSE over
// the payload bytes only.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

// Magic bytes that open every frame.
const (
	Magic0 = 0xAA
	Magic1 = 0x55
)

// MaxPayload bounds a single frame so a malformed or hostile length field
// cannot make the gateway allocate an arbitrary buffer.
const MaxPayload = 4096

// PositionSize is the encoded width of a Position payload.
const PositionSize = 34

// Frame type discriminator, carried in the first payload byte.
const (
	TypePosition uint8 = 0x01
	TypeAck      uint8 = 0x02
)

var (
	// ErrBadMagic means the stream is not aligned on a frame boundary.
	ErrBadMagic = errors.New("protocol: bad magic")
	// ErrBadCRC means the frame arrived corrupted.
	ErrBadCRC = errors.New("protocol: crc mismatch")
	// ErrTooLarge means the length field exceeded MaxPayload.
	ErrTooLarge = errors.New("protocol: payload too large")
	// ErrShortPayload means the payload was smaller than its type requires.
	ErrShortPayload = errors.New("protocol: short payload")
	// ErrResyncLimit means the reader scanned too far without finding magic.
	ErrResyncLimit = errors.New("protocol: resync limit exceeded")
)

// Position is a single report from a device.
//
// Latitude and longitude are carried as fixed-point 1e-7 degrees in int32,
// which is the usual trade in this space: it keeps the frame small, survives
// integer-only firmware, and still resolves to about 1.1 cm.
type Position struct {
	DeviceID uint64
	Seq      uint32
	At       time.Time
	Lat      float64
	Lon      float64
	SpeedKph uint16
	Heading  uint16
	Ignition bool
}

const latLonScale = 1e7

// Status flags packed into the final payload byte.
const (
	flagIgnition uint8 = 1 << 0
)

// Encode serialises p into a complete frame appended to dst.
func (p Position) Encode(dst []byte) []byte {
	var payload [PositionSize]byte
	payload[0] = TypePosition
	binary.BigEndian.PutUint64(payload[1:9], p.DeviceID)
	binary.BigEndian.PutUint32(payload[9:13], p.Seq)
	binary.BigEndian.PutUint64(payload[13:21], uint64(p.At.UnixMilli()))
	binary.BigEndian.PutUint32(payload[21:25], uint32(int32(math.Round(p.Lat*latLonScale))))
	binary.BigEndian.PutUint32(payload[25:29], uint32(int32(math.Round(p.Lon*latLonScale))))
	binary.BigEndian.PutUint16(payload[29:31], p.SpeedKph)
	binary.BigEndian.PutUint16(payload[31:33], p.Heading)
	if p.Ignition {
		payload[33] |= flagIgnition
	}
	return appendFrame(dst, payload[:])
}

// EncodeAck builds the acknowledgement the gateway sends back once a sequence
// number is durably accepted. Devices use it to release buffered frames, which
// is the whole reason replay and deduplication matter downstream.
func EncodeAck(dst []byte, deviceID uint64, seq uint32) []byte {
	var payload [13]byte
	payload[0] = TypeAck
	binary.BigEndian.PutUint64(payload[1:9], deviceID)
	binary.BigEndian.PutUint32(payload[9:13], seq)
	return appendFrame(dst, payload[:])
}

func appendFrame(dst, payload []byte) []byte {
	dst = append(dst, Magic0, Magic1)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(payload)))
	dst = append(dst, payload...)
	return binary.BigEndian.AppendUint16(dst, CRC16(payload))
}

// DecodePosition parses a payload previously produced by Position.Encode.
func DecodePosition(payload []byte) (Position, error) {
	if len(payload) < PositionSize {
		return Position{}, ErrShortPayload
	}
	if payload[0] != TypePosition {
		return Position{}, fmt.Errorf("protocol: want position frame, got type 0x%02x", payload[0])
	}
	return Position{
		DeviceID: binary.BigEndian.Uint64(payload[1:9]),
		Seq:      binary.BigEndian.Uint32(payload[9:13]),
		At:       time.UnixMilli(int64(binary.BigEndian.Uint64(payload[13:21]))).UTC(),
		Lat:      float64(int32(binary.BigEndian.Uint32(payload[21:25]))) / latLonScale,
		Lon:      float64(int32(binary.BigEndian.Uint32(payload[25:29]))) / latLonScale,
		SpeedKph: binary.BigEndian.Uint16(payload[29:31]),
		Heading:  binary.BigEndian.Uint16(payload[31:33]),
		Ignition: payload[33]&flagIgnition != 0,
	}, nil
}

// DecodeAck parses an acknowledgement payload.
func DecodeAck(payload []byte) (deviceID uint64, seq uint32, err error) {
	if len(payload) < 13 {
		return 0, 0, ErrShortPayload
	}
	if payload[0] != TypeAck {
		return 0, 0, fmt.Errorf("protocol: want ack frame, got type 0x%02x", payload[0])
	}
	return binary.BigEndian.Uint64(payload[1:9]), binary.BigEndian.Uint32(payload[9:13]), nil
}

// Reader pulls frames off a byte stream.
//
// It is not safe for concurrent use. One Reader belongs to one connection,
// read by one goroutine, which is the only arrangement that makes read
// deadlines mean anything.
type Reader struct {
	r   io.Reader
	hdr [4]byte
	buf []byte

	// ResyncLimit caps how many bytes may be discarded while hunting for the
	// next magic prefix. Zero means the package default.
	ResyncLimit int
}

// NewReader wraps r. Callers should hand it a *bufio.Reader over a net.Conn
// so that a single frame does not turn into four syscalls.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r, buf: make([]byte, 0, 256)}
}

const defaultResyncLimit = 64 << 10

// ReadFrame returns the next payload. The returned slice is only valid until
// the next call, which avoids an allocation per frame on a hot ingest path.
//
// On a CRC or magic error the stream is left positioned after the offending
// bytes so the caller may choose to continue rather than drop the connection.
func (rd *Reader) ReadFrame() ([]byte, error) {
	if err := rd.sync(); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(rd.r, rd.hdr[2:4]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(rd.hdr[2:4]))
	if n > MaxPayload {
		return nil, ErrTooLarge
	}
	if cap(rd.buf) < n+2 {
		rd.buf = make([]byte, n+2)
	}
	rd.buf = rd.buf[:n+2]
	if _, err := io.ReadFull(rd.r, rd.buf); err != nil {
		return nil, err
	}
	payload := rd.buf[:n]
	want := binary.BigEndian.Uint16(rd.buf[n : n+2])
	if CRC16(payload) != want {
		return nil, ErrBadCRC
	}
	return payload, nil
}

// sync advances the stream until the magic prefix has been consumed.
//
// Devices in the field truncate frames when they lose power mid-write, and a
// gateway that gives up on the first misaligned byte will spend its life
// reconnecting. Scanning forward is cheaper than a reconnect, up to a limit.
func (rd *Reader) sync() error {
	limit := rd.ResyncLimit
	if limit <= 0 {
		limit = defaultResyncLimit
	}
	var one [1]byte
	discarded := 0
	// Read until hdr[0:2] holds the magic pair.
	if _, err := io.ReadFull(rd.r, rd.hdr[0:2]); err != nil {
		return err
	}
	for {
		if rd.hdr[0] == Magic0 && rd.hdr[1] == Magic1 {
			return nil
		}
		if discarded++; discarded > limit {
			return ErrResyncLimit
		}
		if _, err := io.ReadFull(rd.r, one[:]); err != nil {
			return err
		}
		rd.hdr[0], rd.hdr[1] = rd.hdr[1], one[0]
	}
}

// CRC16 computes CRC-16/CCITT-FALSE: polynomial 0x1021, init 0xFFFF, no
// reflection, no final xor.
func CRC16(b []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, c := range b {
		crc ^= uint16(c) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
