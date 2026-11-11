package promql

import (
	"fmt"
	"math"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
)

// hysteresis_count_over_time(low, high, series[window]) counts the number of complete
// low -> high -> low cycles observed by series inside the given lookbehind window.
//
// State machine (the state is never inherited from samples before the window start):
//
//   - hysteresisStateUnarmed: no value <= low has been observed yet. A stale marker always
//     moves the machine back to this state.
//   - hysteresisStateArmed: a value <= low was observed; the machine waits for a value >= high.
//   - hysteresisStateHigh: a value >= high was observed while armed; the machine waits for a
//     value <= low, which completes a single cycle and re-arms the machine.
//
// Values strictly between low and high do not change the state. Ordinary NaN values are
// ignored (they act as identity), while Prometheus stale markers reset the machine to
// hysteresisStateUnarmed, so cycles can never span a stale segment.
const (
	hysteresisStateUnarmed = 0
	hysteresisStateArmed   = 1
	hysteresisStateHigh    = 2

	hysteresisStatesCount = 3
)

// addUint64Checked returns the saturated sum of a and b and reports whether the addition
// overflowed. The overflow condition is detected explicitly instead of relying on the
// wrapped-around result.
func addUint64Checked(a, b uint64) (uint64, bool) {
	if a > math.MaxUint64-b {
		return math.MaxUint64, true
	}
	return a + b, false
}

// subInt64Checked returns a-b and reports whether the subtraction overflowed.
//
// It is used for time arithmetic, where subtracting two arbitrary int64 millisecond
// timestamps may overflow.
func subInt64Checked(a, b int64) (int64, bool) {
	res := a - b
	// The subtraction overflows iff a and b have different signs and the result
	// loses the sign of a.
	if (a^b) < 0 && (res^a) < 0 {
		if b < 0 {
			return math.MaxInt64, true
		}
		return math.MinInt64, true
	}
	return res, false
}

// hysteresisSegment is an associative summary of a time-ordered sequence of samples.
//
// For every possible finite initial state of the hysteresis state machine it stores the
// state reached after processing the whole segment and the number of complete cycles
// counted along the way. This makes it possible to concatenate (merge) summaries of
// adjacent segments without re-scanning the underlying samples.
//
// The merge operation is associative but not commutative: swapping left and right
// segments changes the result, because the state machine is directed in time.
type hysteresisSegment struct {
	// nextState[s] is the state after processing the segment when entering it in state s.
	nextState [hysteresisStatesCount]uint8

	// cycles[s] is the number of complete low -> high -> low cycles when entering
	// the segment in state s. The value saturates at math.MaxUint64 on overflow.
	cycles [hysteresisStatesCount]uint64

	// overflow[s] is set to true when cycles[s] saturated because of an overflow.
	overflow [hysteresisStatesCount]bool

	// hasValidSample is true if the segment contains at least one finite sample
	// (neither an ordinary NaN nor a Prometheus stale marker).
	hasValidSample bool

	// hasStaleMarker is true if the segment contains at least one Prometheus stale marker.
	hasStaleMarker bool
}

// identityHysteresisSegment returns an empty segment: every initial state maps to itself
// and no cycles are counted.
func identityHysteresisSegment() hysteresisSegment {
	var seg hysteresisSegment
	for s := 0; s < hysteresisStatesCount; s++ {
		seg.nextState[s] = uint8(s)
	}
	return seg
}

// mergeHysteresisSegments returns the summary of processing left followed by right.
//
// The operation is intentionally non-commutative: the right segment is entered in the
// state produced by the left segment, while cycle counts are summed using checked
// arithmetic.
func mergeHysteresisSegments(left, right *hysteresisSegment) hysteresisSegment {
	var res hysteresisSegment
	for s := 0; s < hysteresisStatesCount; s++ {
		middleState := left.nextState[s]
		res.nextState[s] = right.nextState[middleState]
		sum, overflowed := addUint64Checked(left.cycles[s], right.cycles[middleState])
		res.cycles[s] = sum
		res.overflow[s] = left.overflow[s] || right.overflow[middleState] || overflowed
	}
	res.hasValidSample = left.hasValidSample || right.hasValidSample
	res.hasStaleMarker = left.hasStaleMarker || right.hasStaleMarker
	return res
}

// newHysteresisSampleSegment builds the summary of a single sample value v evaluated
// against fixed thresholds low < high (both finite).
func newHysteresisSampleSegment(v, low, high float64) hysteresisSegment {
	seg := identityHysteresisSegment()

	if decimal.IsStaleNaN(v) {
		// A stale marker resets the state machine regardless of its current state,
		// so cycles cannot be connected across stale segments.
		for s := 0; s < hysteresisStatesCount; s++ {
			seg.nextState[s] = hysteresisStateUnarmed
		}
		seg.hasStaleMarker = true
		return seg
	}

	if math.IsNaN(v) {
		// Ordinary NaN values follow the explicit ignore protocol: they do not
		// change the state and do not count as valid samples.
		return seg
	}

	seg.hasValidSample = true
	switch {
	case v <= low:
		// Unarmed and armed machines become (or stay) armed. A machine waiting for
		// the low threshold completes a cycle and becomes armed again.
		seg.nextState[hysteresisStateUnarmed] = hysteresisStateArmed
		seg.nextState[hysteresisStateArmed] = hysteresisStateArmed
		seg.nextState[hysteresisStateHigh] = hysteresisStateArmed
		seg.cycles[hysteresisStateHigh] = 1
	case v >= high:
		// A high value is ignored while unarmed. An armed machine enters the high
		// state and stays there while more high values arrive.
		seg.nextState[hysteresisStateUnarmed] = hysteresisStateUnarmed
		seg.nextState[hysteresisStateArmed] = hysteresisStateHigh
		seg.nextState[hysteresisStateHigh] = hysteresisStateHigh
	default:
		// The value is strictly inside the (low, high) band: no state change.
		seg.nextState[hysteresisStateUnarmed] = hysteresisStateUnarmed
		seg.nextState[hysteresisStateArmed] = hysteresisStateArmed
		seg.nextState[hysteresisStateHigh] = hysteresisStateHigh
	}
	return seg
}

// hysteresisStackEntry pairs a sample timestamp with its single-sample segment and the
// cumulative summary of its stack up to (and including) itself.
type hysteresisStackEntry struct {
	timestamp int64

	// segment is the single-sample summary of this entry's sample.
	segment hysteresisSegment

	// prefix is the cumulative monoid summary of the whole stack in its arrival
	// (oldest -> newest) order up to and including this entry.
	prefix hysteresisSegment
}

// hysteresisWindowAggregator maintains the monoid summary of the samples belonging to
// the current sliding window using the two-stack queue technique.
//
// New samples are pushed onto inStack; expired samples are popped from the front of
// outStack. When outStack becomes empty, all inStack entries are moved onto outStack in
// reverse order. Every stack entry carries the cumulative segment summary of the stack
// below (and including) itself, so the total window summary is obtained with a constant
// number of monoid merges. Each sample is pushed and popped a constant number of times,
// which gives amortized O(1) work per window slide instead of re-scanning the window.
type hysteresisWindowAggregator struct {
	low  float64
	high float64

	// inStack stores recently pushed samples, oldest at index 0.
	inStack []hysteresisStackEntry

	// outStack stores samples ready to expire, oldest at the last index.
	outStack []hysteresisStackEntry

	// lastPushedTimestamp tracks the timestamp of the most recently pushed sample.
	lastPushedTimestamp int64
	hasLastTimestamp    bool
}

// reset drops all queued samples and binds the aggregator to the given thresholds.
func (a *hysteresisWindowAggregator) reset(low, high float64) {
	a.low = low
	a.high = high
	a.inStack = a.inStack[:0]
	a.outStack = a.outStack[:0]
	a.lastPushedTimestamp = 0
	a.hasLastTimestamp = false
}

// push appends a sample to the back of the queue.
func (a *hysteresisWindowAggregator) push(timestamp int64, value float64) {
	seg := newHysteresisSampleSegment(value, a.low, a.high)
	entry := hysteresisStackEntry{
		timestamp: timestamp,
		segment:   seg,
	}
	// The in-stack prefix summarizes the whole in-stack in arrival order, so the new
	// entry is merged to the right of the previous cumulative summary.
	if n := len(a.inStack); n > 0 {
		entry.prefix = mergeHysteresisSegments(&a.inStack[n-1].prefix, &seg)
	} else {
		entry.prefix = seg
	}
	a.inStack = append(a.inStack, entry)
	a.lastPushedTimestamp = timestamp
	a.hasLastTimestamp = true
}

// transferToOutStack moves every entry from inStack onto outStack, reversing their order.
func (a *hysteresisWindowAggregator) transferToOutStack() {
	for len(a.inStack) > 0 {
		n := len(a.inStack) - 1
		entry := a.inStack[n]
		a.inStack = a.inStack[:n]

		// Entries are moved from newest to oldest. The moved entry is older than
		// everything currently on outStack, so its single-sample summary is merged
		// to the left of the current outStack cumulative summary.
		if m := len(a.outStack); m > 0 {
			entry.prefix = mergeHysteresisSegments(&entry.segment, &a.outStack[m-1].prefix)
		} else {
			entry.prefix = entry.segment
		}
		a.outStack = append(a.outStack, entry)
	}
}

// oldestTimestamp returns the timestamp of the oldest queued sample.
func (a *hysteresisWindowAggregator) oldestTimestamp() (int64, bool) {
	if len(a.outStack) > 0 {
		return a.outStack[len(a.outStack)-1].timestamp, true
	}
	if len(a.inStack) > 0 {
		return a.inStack[0].timestamp, true
	}
	return 0, false
}

// popOldest removes the oldest sample from the queue.
func (a *hysteresisWindowAggregator) popOldest() {
	if len(a.outStack) == 0 {
		a.transferToOutStack()
	}
	if len(a.outStack) > 0 {
		a.outStack = a.outStack[:len(a.outStack)-1]
	}
}

// total returns the monoid summary of all queued samples in arrival (oldest -> newest)
// order. An empty queue yields the identity segment.
func (a *hysteresisWindowAggregator) total() hysteresisSegment {
	res := identityHysteresisSegment()
	if n := len(a.outStack); n > 0 {
		res = mergeHysteresisSegments(&res, &a.outStack[n-1].prefix)
	}
	if n := len(a.inStack); n > 0 {
		res = mergeHysteresisSegments(&res, &a.inStack[n-1].prefix)
	}
	return res
}

// advance synchronizes the queue with the window (tStart, tEnd], whose samples are passed
// in time order. Samples older than or equal to tStart expire, while samples not queued
// yet are pushed.
func (a *hysteresisWindowAggregator) advance(tStart int64, values []float64, timestamps []int64) {
	for {
		oldestTS, ok := a.oldestTimestamp()
		if !ok || oldestTS > tStart {
			break
		}
		a.popOldest()
	}
	// Consecutive windows overlap and therefore share a time-ordered prefix. Samples with
	// a timestamp not newer than the already-queued frontier were enqueued by a previous
	// window (same-timestamp duplicates always belong to exactly the same windows, so they
	// are either all queued or all absent). Everything after that prefix is new and is
	// pushed unconditionally, including duplicate timestamps inside the new suffix.
	start := 0
	if a.hasLastTimestamp {
		for start < len(timestamps) && timestamps[start] <= a.lastPushedTimestamp {
			start++
		}
	}
	for i := start; i < len(timestamps); i++ {
		a.push(timestamps[i], values[i])
	}
}

// newRollupHysteresisCountOverTime implements hysteresis_count_over_time(low, high, q[window]).
//
// low and high are scalar arguments evaluated at every output point; they remain fixed
// for the samples of the corresponding window. Both must be finite and low < high.
func newRollupHysteresisCountOverTime(args []any) (rollupFunc, error) {
	if err := expectRollupArgsNum(args, 3); err != nil {
		return nil, err
	}
	lows, err := getScalar(args[0], 0)
	if err != nil {
		return nil, err
	}
	highs, err := getScalar(args[1], 1)
	if err != nil {
		return nil, err
	}
	if len(lows) != len(highs) {
		return nil, fmt.Errorf("low and high args must have the same number of points; got %d vs %d", len(lows), len(highs))
	}
	for i := range lows {
		low := lows[i]
		high := highs[i]
		if math.IsNaN(low) || math.IsInf(low, 0) || math.IsNaN(high) || math.IsInf(high, 0) {
			return nil, fmt.Errorf("hysteresis_count_over_time() requires finite low and high thresholds; got low=%v, high=%v", low, high)
		}
		if !(low < high) {
			return nil, fmt.Errorf("hysteresis_count_over_time() requires low < high; got low=%v, high=%v", low, high)
		}
	}

	rf := func(rfa *rollupFuncArg) float64 {
		low := lows[rfa.idx]
		high := highs[rfa.idx]
		agg := rfa.hysteresisAgg
		if agg == nil {
			agg = &hysteresisWindowAggregator{}
			agg.reset(low, high)
			rfa.hysteresisAgg = agg
		}

		tStart, overflowed := subInt64Checked(rfa.currTimestamp, rfa.window)
		if overflowed {
			// Defensive path for practically unreachable timestamp values: rebuild
			// the queue from the current window instead of trusting the subtraction.
			agg.reset(low, high)
			for i, ts := range rfa.timestamps {
				agg.push(ts, rfa.values[i])
			}
		} else if agg.low != low || agg.high != high {
			// Thresholds are fixed within a window but may change between output
			// points (per-point scalar args). Previously built summaries are bound
			// to the old thresholds, so the queue must be rebuilt.
			agg.reset(low, high)
			for i, ts := range rfa.timestamps {
				agg.push(ts, rfa.values[i])
			}
		} else {
			agg.advance(tStart, rfa.values, rfa.timestamps)
		}

		segment := agg.total()
		if !segment.hasValidSample {
			// No finite samples in the window: distinguish "no data" from "zero cycles".
			return nan
		}
		return float64(segment.cycles[hysteresisStateUnarmed])
	}
	return rf, nil
}
