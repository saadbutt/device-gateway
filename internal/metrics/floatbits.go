package metrics

import "math"

func float64ToBits(f float64) uint64 { return math.Float64bits(f) }
func float64FromBits(b uint64) float64 {
	if b == 0 {
		return 0
	}
	return math.Float64frombits(b)
}
