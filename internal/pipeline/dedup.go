// Package pipeline holds the stages a decoded position passes through after
// it leaves the connection goroutine.
package pipeline

import (
	"container/list"
	"sync"
)

// WindowSize is how far back, in sequence numbers, a device may replay before
// the gateway stops being able to tell a duplicate from a genuinely old frame.
//
// 1024 is chosen against real behaviour rather than taste: a tracker that
// loses its uplink buffers for minutes, not hours, and firmware buffers are
// usually a few hundred records deep. Widening this is cheap in memory
// (one bit per sequence) and expensive in nothing, so err generous.
const WindowSize = 1024

const windowWords = WindowSize / 64

// Deduper answers "have I already accepted this frame?" for a fleet of
// devices, in constant time and bounded memory.
//
// Why this exists: the gateway acknowledges each sequence number, and a device
// only releases a buffered frame once it sees that ack. If the link drops
// between the gateway writing an ack and the device receiving it, the device
// reconnects and sends the frame again. It is behaving correctly. Every
// telemetry pipeline that lacks this component quietly double-counts mileage,
// fuel and trip starts, and nobody notices until a customer disputes a report.
//
// The algorithm is the sliding window used for IPsec anti-replay: track the
// highest sequence seen, plus a bitmap of the WindowSize sequences below it.
//
// Deduper is safe for concurrent use.
type Deduper struct {
	mu         sync.Mutex
	maxDevices int
	windows    map[uint64]*list.Element
	lru        *list.List // front = most recently used

	dupes    uint64
	tooOld   uint64
	evicted  uint64
	accepted uint64
}

type entry struct {
	deviceID uint64
	high     uint32
	seen     bool
	bits     [windowWords]uint64
}

// NewDeduper returns a Deduper that tracks at most maxDevices distinct
// devices, evicting the least recently seen when it runs out of room.
//
// The cap matters: device IDs are attacker-controlled on an open TCP port, and
// an unbounded map keyed by them is a memory exhaustion bug wearing a hat.
func NewDeduper(maxDevices int) *Deduper {
	if maxDevices <= 0 {
		maxDevices = 10000
	}
	return &Deduper{
		maxDevices: maxDevices,
		windows:    make(map[uint64]*list.Element, maxDevices),
		lru:        list.New(),
	}
}

// Seen reports whether (deviceID, seq) has already been accepted.
//
// It returns false for a frame that should be processed, and true for one that
// should be acknowledged but dropped. Note the asymmetry: a duplicate still
// gets an ack, because the device is asking again precisely because it never
// heard the first one.
func (d *Deduper) Seen(deviceID uint64, seq uint32) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	e := d.lookup(deviceID)

	if !e.seen {
		e.seen = true
		e.high = seq
		e.mark(0)
		d.accepted++
		return false
	}

	switch {
	case seq > e.high:
		e.advance(seq)
		d.accepted++
		return false

	case e.high-seq >= WindowSize:
		// Older than the window. It may be a genuine duplicate from a long
		// outage or a device that reset its counter. Either way it cannot be
		// distinguished, and accepting it risks double-counting, so drop.
		d.tooOld++
		return true

	default:
		offset := e.high - seq
		if e.test(offset) {
			d.dupes++
			return true
		}
		e.mark(offset)
		d.accepted++
		return false
	}
}

// Reset forgets a device's window. Call it when a device legitimately restarts
// its sequence counter, which firmware does after a factory reset.
func (d *Deduper) Reset(deviceID uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.windows[deviceID]; ok {
		d.lru.Remove(el)
		delete(d.windows, deviceID)
	}
}

// Stats snapshots the counters. Field order matches the metric names exported
// by the gateway.
func (d *Deduper) Stats() (accepted, dupes, tooOld, evicted uint64, tracked int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.accepted, d.dupes, d.tooOld, d.evicted, len(d.windows)
}

func (d *Deduper) lookup(deviceID uint64) *entry {
	if el, ok := d.windows[deviceID]; ok {
		d.lru.MoveToFront(el)
		return el.Value.(*entry)
	}
	for len(d.windows) >= d.maxDevices {
		oldest := d.lru.Back()
		if oldest == nil {
			break
		}
		d.lru.Remove(oldest)
		delete(d.windows, oldest.Value.(*entry).deviceID)
		d.evicted++
	}
	e := &entry{deviceID: deviceID}
	d.windows[deviceID] = d.lru.PushFront(e)
	return e
}

// advance slides the window up to newHigh and marks it.
func (e *entry) advance(newHigh uint32) {
	shift := newHigh - e.high
	if shift >= WindowSize {
		e.bits = [windowWords]uint64{}
	} else {
		e.shiftLeft(uint(shift))
	}
	e.high = newHigh
	e.mark(0)
}

// shiftLeft moves every recorded sequence n positions further into the past.
func (e *entry) shiftLeft(n uint) {
	words, bits := n/64, n%64
	if words > 0 {
		for i := windowWords - 1; i >= 0; i-- {
			if i >= int(words) {
				e.bits[i] = e.bits[i-int(words)]
			} else {
				e.bits[i] = 0
			}
		}
	}
	if bits > 0 {
		var carry uint64
		for i := 0; i < windowWords; i++ {
			next := e.bits[i] >> (64 - bits)
			e.bits[i] = e.bits[i]<<bits | carry
			carry = next
		}
	}
}

func (e *entry) mark(offset uint32) { e.bits[offset/64] |= 1 << (offset % 64) }
func (e *entry) test(offset uint32) bool {
	return e.bits[offset/64]&(1<<(offset%64)) != 0
}
