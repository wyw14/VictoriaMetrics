package promql

import (
	"math"
	"math/rand"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
)

// hysteresisReference is a straightforward three-state implementation of the
// hysteresis_count_over_time() protocol used as an independent oracle in tests.
//
// It processes every sample from scratch, so it deliberately avoids any of the
// monoid / sliding-window optimizations used by the production code.
func hysteresisReference(low, high float64, values []float64, initialState uint8) (state uint8, cycles uint64, hasValid bool) {
	state = initialState
	for _, v := range values {
		if decimal.IsStaleNaN(v) {
			// Stale markers reset the machine to unarmed and break any cycle.
			state = hysteresisStateUnarmed
			continue
		}
		if math.IsNaN(v) {
			// Ordinary NaNs are explicitly ignored.
			continue
		}
		hasValid = true
		switch {
		case v <= low:
			if state == hysteresisStateHigh {
				cycles++
			}
			state = hysteresisStateArmed
		case v >= high:
			if state == hysteresisStateArmed {
				state = hysteresisStateHigh
			}
		default:
			// In-band values do not change the state.
		}
	}
	return state, cycles, hasValid
}

func TestHysteresisSampleSegment(t *testing.T) {
	const low, high = 0.0, 10.0

	f := func(v float64, initialState uint8, wantState uint8, wantCycles uint64) {
		t.Helper()
		seg := newHysteresisSampleSegment(v, low, high)
		if seg.nextState[initialState] != wantState {
			t.Fatalf("v=%v, state=%d: got next state %d; want %d", v, initialState, seg.nextState[initialState], wantState)
		}
		if seg.cycles[initialState] != wantCycles {
			t.Fatalf("v=%v, state=%d: got cycles %d; want %d", v, initialState, seg.cycles[initialState], wantCycles)
		}
	}

	// Boundary equality: v == low arms (and closes a cycle from high); v == high enters high.
	f(0.0, hysteresisStateUnarmed, hysteresisStateArmed, 0)
	f(0.0, hysteresisStateArmed, hysteresisStateArmed, 0)
	f(0.0, hysteresisStateHigh, hysteresisStateArmed, 1)
	f(10.0, hysteresisStateUnarmed, hysteresisStateUnarmed, 0)
	f(10.0, hysteresisStateArmed, hysteresisStateHigh, 0)
	f(10.0, hysteresisStateHigh, hysteresisStateHigh, 0)

	// Strictly outside the thresholds.
	f(-5.0, hysteresisStateUnarmed, hysteresisStateArmed, 0)
	f(-5.0, hysteresisStateHigh, hysteresisStateArmed, 1)
	f(15.0, hysteresisStateArmed, hysteresisStateHigh, 0)
	f(15.0, hysteresisStateUnarmed, hysteresisStateUnarmed, 0)

	// In-band values keep every state unchanged.
	for _, state := range []uint8{hysteresisStateUnarmed, hysteresisStateArmed, hysteresisStateHigh} {
		f(5.0, state, state, 0)
	}

	// Ordinary NaN is an identity sample and is not a valid sample.
	nanSeg := newHysteresisSampleSegment(nan, low, high)
	for state := 0; state < hysteresisStatesCount; state++ {
		if nanSeg.nextState[state] != uint8(state) || nanSeg.cycles[state] != 0 {
			t.Fatalf("NaN must be identity for state %d; got %+v", state, nanSeg)
		}
	}
	if nanSeg.hasValidSample || nanSeg.hasStaleMarker {
		t.Fatalf("ordinary NaN must be neither valid nor stale; got %+v", nanSeg)
	}

	// Stale marker forces every initial state back to unarmed and cannot count a cycle.
	staleSeg := newHysteresisSampleSegment(decimal.StaleNaN, low, high)
	for state := 0; state < hysteresisStatesCount; state++ {
		if staleSeg.nextState[state] != hysteresisStateUnarmed || staleSeg.cycles[state] != 0 {
			t.Fatalf("stale marker must reset state %d to unarmed without cycles; got %+v", state, staleSeg)
		}
	}
	if !staleSeg.hasStaleMarker || staleSeg.hasValidSample {
		t.Fatalf("unexpected stale segment flags: %+v", staleSeg)
	}

	// Infinities follow ordinary comparison rules.
	f(math.Inf(-1), hysteresisStateUnarmed, hysteresisStateArmed, 0)
	f(math.Inf(1), hysteresisStateArmed, hysteresisStateHigh, 0)
}

func TestHysteresisSegmentMatchesReference(t *testing.T) {
	const low, high = 3.0, 7.0
	values := []float64{
		1, 5, 8, 6, 2, // one complete cycle, ends armed
		nan, 8, 4, 9, 2, // NaN ignored; second complete cycle
		decimal.StaleNaN, 8, 4, // stale resets; the high value while unarmed is ignored
		2, 9, 3, // third complete cycle after the stale segment
	}

	// Fold single-sample segments in time order.
	folded := identityHysteresisSegment()
	for _, v := range values {
		single := newHysteresisSampleSegment(v, low, high)
		folded = mergeHysteresisSegments(&folded, &single)
	}

	for initialState := 0; initialState < hysteresisStatesCount; initialState++ {
		wantState, wantCycles, wantValid := hysteresisReference(low, high, values, uint8(initialState))
		if folded.nextState[initialState] != wantState {
			t.Fatalf("state %d: got final state %d; want %d", initialState, folded.nextState[initialState], wantState)
		}
		if folded.cycles[initialState] != wantCycles {
			t.Fatalf("state %d: got cycles %d; want %d", initialState, folded.cycles[initialState], wantCycles)
		}
		if folded.hasValidSample != wantValid {
			t.Fatalf("state %d: got hasValidSample=%v; want %v", initialState, folded.hasValidSample, wantValid)
		}
	}
	if !folded.hasStaleMarker {
		t.Fatalf("the folded segment must remember the stale marker")
	}
}

func TestHysteresisMergeIsNonCommutative(t *testing.T) {
	const low, high = 0.0, 10.0
	// A moves an armed machine into the high state.
	segA := newHysteresisSampleSegment(10.0, low, high)
	// B moves a high machine back to armed and counts one cycle.
	segB := newHysteresisSampleSegment(0.0, low, high)

	ab := mergeHysteresisSegments(&segA, &segB)
	ba := mergeHysteresisSegments(&segB, &segA)

	// Armed -> high -> armed: one cycle.
	if ab.nextState[hysteresisStateArmed] != hysteresisStateArmed || ab.cycles[hysteresisStateArmed] != 1 {
		t.Fatalf("A then B must complete a cycle; got state=%d cycles=%d",
			ab.nextState[hysteresisStateArmed], ab.cycles[hysteresisStateArmed])
	}
	// Armed stays armed (B), then enters high (A): no cycle.
	if ba.nextState[hysteresisStateArmed] != hysteresisStateHigh || ba.cycles[hysteresisStateArmed] != 0 {
		t.Fatalf("B then A must not complete a cycle; got state=%d cycles=%d",
			ba.nextState[hysteresisStateArmed], ba.cycles[hysteresisStateArmed])
	}
}

func TestHysteresisMergeAssociativity(t *testing.T) {
	const low, high = -1.5, 4.5
	rng := rand.New(rand.NewSource(7))

	for iter := 0; iter < 100; iter++ {
		// Build three random adjacent segments.
		buildSegment := func() hysteresisSegment {
			seg := identityHysteresisSegment()
			for k := rng.Intn(8); k >= 0; k-- {
				var v float64
				switch rng.Intn(7) {
				case 0:
					v = low - 1
				case 1:
					v = low
				case 2:
					v = (low + high) / 2
				case 3:
					v = high
				case 4:
					v = high + 1
				case 5:
					v = nan
				default:
					v = decimal.StaleNaN
				}
				single := newHysteresisSampleSegment(v, low, high)
				seg = mergeHysteresisSegments(&seg, &single)
			}
			return seg
		}
		a, b, c := buildSegment(), buildSegment(), buildSegment()

		// (A + B) + C
		left := mergeHysteresisSegments(&a, &b)
		leftAssoc := mergeHysteresisSegments(&left, &c)
		// A + (B + C)
		right := mergeHysteresisSegments(&b, &c)
		rightAssoc := mergeHysteresisSegments(&a, &right)

		for state := 0; state < hysteresisStatesCount; state++ {
			if leftAssoc.nextState[state] != rightAssoc.nextState[state] ||
				leftAssoc.cycles[state] != rightAssoc.cycles[state] ||
				leftAssoc.overflow[state] != rightAssoc.overflow[state] {
				t.Fatalf("iter=%d state=%d: merge is not associative: %+v vs %+v",
					iter, state, leftAssoc, rightAssoc)
			}
		}
		if leftAssoc.hasValidSample != rightAssoc.hasValidSample ||
			leftAssoc.hasStaleMarker != rightAssoc.hasStaleMarker {
			t.Fatalf("iter=%d: segment flags differ after reassociation", iter)
		}
	}
}

// TestHysteresisMergeOverflow checks cycle-count saturation when adjacent summaries
// exceed the uint64 range.
func TestHysteresisMergeOverflow(t *testing.T) {
	segWithCycles := func(c uint64, overflow bool) hysteresisSegment {
		seg := identityHysteresisSegment()
		seg.cycles[hysteresisStateUnarmed] = c
		seg.overflow[hysteresisStateUnarmed] = overflow
		seg.hasValidSample = true
		return seg
	}

	// A saturated summary keeps saturating and latches the overflow flag.
	saturated := segWithCycles(math.MaxUint64, true)
	one := segWithCycles(1, false)
	merged := mergeHysteresisSegments(&saturated, &one)
	if merged.cycles[hysteresisStateUnarmed] != math.MaxUint64 || !merged.overflow[hysteresisStateUnarmed] {
		t.Fatalf("saturated summary must stay saturated; got %+v", merged)
	}

	// Two large finite summaries whose sum overflows saturate as well.
	large := segWithCycles(math.MaxUint64/2+1, false)
	merged = mergeHysteresisSegments(&large, &large)
	if merged.cycles[hysteresisStateUnarmed] != math.MaxUint64 || !merged.overflow[hysteresisStateUnarmed] {
		t.Fatalf("overflowing merge must saturate; got %+v", merged)
	}
}

// TestHysteresisWindowAggregatorQueueOrder exercises the two-stack transfer directly and
// ensures the total summary always reflects arrival order.
func TestHysteresisWindowAggregatorQueueOrder(t *testing.T) {
	const low, high = 0.0, 10.0
	var agg hysteresisWindowAggregator
	agg.reset(low, high)

	// arm -> high -> low completes one cycle; all samples must survive stack transfers.
	values := []float64{0, 10, 0, 10, 0}
	timestamps := []int64{1, 2, 3, 4, 5}
	agg.advance(-100, values, timestamps)
	if seg := agg.total(); seg.cycles[hysteresisStateUnarmed] != 2 {
		t.Fatalf("expected 2 cycles before eviction; got %d", seg.cycles[hysteresisStateUnarmed])
	}

	// Expire samples one by one from the oldest side and compare with the reference
	// evaluated over the remaining suffix.
	for i := range timestamps {
		oldest, ok := agg.oldestTimestamp()
		if !ok || oldest != timestamps[i] {
			t.Fatalf("oldest timestamp mismatch at %d: got (%d, %v); want %d", i, oldest, ok, timestamps[i])
		}
		agg.popOldest()
		seg := agg.total()
		_, wantCycles, hasValid := hysteresisReference(low, high, values[i+1:], hysteresisStateUnarmed)
		if hasValid && seg.cycles[hysteresisStateUnarmed] != wantCycles {
			t.Fatalf("after %d evictions: got %d cycles; want %d", i+1, seg.cycles[hysteresisStateUnarmed], wantCycles)
		}
	}
	if _, ok := agg.oldestTimestamp(); ok {
		t.Fatalf("the queue must be empty after evicting every sample")
	}
	if seg := agg.total(); seg.hasValidSample {
		t.Fatalf("empty queue must produce the identity segment; got %+v", seg)
	}
}

func TestHysteresisCheckedArithmetic(t *testing.T) {
	// uint64 checked addition.
	if sum, overflow := addUint64Checked(math.MaxUint64, 1); sum != math.MaxUint64 || !overflow {
		t.Fatalf("MaxUint64+1 must saturate with overflow; got sum=%d overflow=%v", sum, overflow)
	}
	if sum, overflow := addUint64Checked(math.MaxUint64-10, 10); sum != math.MaxUint64 || overflow {
		t.Fatalf("MaxUint64 boundary addition misbehaved; got sum=%d overflow=%v", sum, overflow)
	}
	if sum, overflow := addUint64Checked(1<<62, 1<<62); sum != 1<<63 || overflow {
		t.Fatalf("ordinary addition must not report overflow; got sum=%d overflow=%v", sum, overflow)
	}

	// int64 checked subtraction, including the time-arithmetic extremes.
	cases := []struct {
		a, b, want int64
		overflow   bool
	}{
		{100, 30, 70, false},
		{math.MinInt64, 1, math.MinInt64, true},
		{math.MaxInt64, -1, math.MaxInt64, true},
		{math.MinInt64, 0, math.MinInt64, false},
		{0, math.MinInt64, math.MinInt64, true},
	}
	for _, c := range cases {
		got, overflow := subInt64Checked(c.a, c.b)
		if overflow != c.overflow || (!overflow && got != c.want) {
			t.Fatalf("subInt64Checked(%d, %d): got (%d, %v); want (%d, %v)",
				c.a, c.b, got, overflow, c.want, c.overflow)
		}
	}
}

// TestHysteresisWindowAggregatorScenarios covers the hand-crafted edge cases required by
// the function specification.
func TestHysteresisWindowAggregatorScenarios(t *testing.T) {
	const low, high = 0.0, 10.0
	type sample struct {
		ts int64
		v  float64
	}

	// run feeds the samples through sliding windows (tStart, tEnd] of the given width,
	// advancing one timestamp unit at a time, and compares the aggregator with the
	// three-state reference evaluated from the unarmed state for every window.
	run := func(t *testing.T, samples []sample, windowWidth int64, low, high float64) {
		t.Helper()
		values := make([]float64, len(samples))
		timestamps := make([]int64, len(samples))
		for i, s := range samples {
			values[i] = s.v
			timestamps[i] = s.ts
		}
		var agg hysteresisWindowAggregator
		agg.reset(low, high)

		var tEnd int64
		if len(timestamps) > 0 {
			tEnd = timestamps[len(timestamps)-1]
		}
		for end := timestamps[0]; end <= tEnd; end++ {
			tStart := end - windowWidth
			i, j := 0, 0
			for i < len(timestamps) && timestamps[i] <= tStart {
				i++
			}
			for j < len(timestamps) && timestamps[j] <= end {
				j++
			}
			agg.advance(tStart, values[i:j], timestamps[i:j])
			got := agg.total()

			wantState, wantCycles, wantValid := hysteresisReference(low, high, values[i:j], hysteresisStateUnarmed)
			if got.hasValidSample != wantValid {
				t.Fatalf("end=%d: got hasValid=%v; want %v", end, got.hasValidSample, wantValid)
			}
			if wantValid {
				if got.cycles[hysteresisStateUnarmed] != wantCycles {
					t.Fatalf("end=%d: got cycles=%d; want %d", end, got.cycles[hysteresisStateUnarmed], wantCycles)
				}
				if got.nextState[hysteresisStateUnarmed] != wantState {
					t.Fatalf("end=%d: got final state=%d; want %d", end, got.nextState[hysteresisStateUnarmed], wantState)
				}
			}
		}
	}

	t.Run("boundary_equality_and_full_cycles", func(t *testing.T) {
		samples := []sample{
			{1, 0}, {2, 5}, {3, 10}, {4, 5}, {5, 0}, // cycle 1
			{6, 10}, {7, 0}, // cycle 2
		}
		run(t, samples, 100, low, high)
	})

	t.Run("in_band_jitter_keeps_state", func(t *testing.T) {
		samples := []sample{
			{1, 0}, {2, 5}, {3, 4.9}, {4, 5.1}, {5, 9.9}, {6, 10}, {7, 5}, {8, 0.1}, {9, 0},
		}
		run(t, samples, 100, low, high)
	})

	t.Run("unarmed_high_values_are_ignored", func(t *testing.T) {
		samples := []sample{
			{1, 10}, {2, 10}, {3, 9}, // no arming low yet
			{4, 0}, {5, 10}, {6, 0}, // a single complete cycle
		}
		run(t, samples, 100, low, high)
	})

	t.Run("window_slides_out_first_low", func(t *testing.T) {
		// Narrow windows: the arming low at ts=1 leaves before the high arrives,
		// so the high value stays ignored until a fresh low arms the machine.
		samples := []sample{
			{1, 0}, {5, 10}, {6, 5}, {7, 0}, {8, 10}, {9, 0},
		}
		run(t, samples, 3, low, high)
	})

	t.Run("multiple_stale_segments_never_connect", func(t *testing.T) {
		samples := []sample{
			{1, 0}, {2, 10}, {3, decimal.StaleNaN},
			{4, 10}, {5, 0}, {6, decimal.StaleNaN},
			{7, 0}, {8, 10}, {9, 0}, // only this segment completes one cycle
		}
		run(t, samples, 100, low, high)
	})

	t.Run("ordinary_nan_is_ignored", func(t *testing.T) {
		samples := []sample{
			{1, 0}, {2, nan}, {3, 10}, {4, nan}, {5, 0},
			{6, nan}, {7, 10}, {8, nan}, {9, 0},
		}
		run(t, samples, 100, low, high)
	})

	t.Run("duplicate_timestamps_follow_existing_order_semantics", func(t *testing.T) {
		// Same-timestamp samples are processed in arrival order; no hidden dedup is applied.
		samples := []sample{
			{1, 0}, {1, 10}, {1, 0}, // arm -> high -> cycle within the same timestamp
			{2, 10}, {2, 0}, // another cycle
		}
		run(t, samples, 100, low, high)
	})

	t.Run("empty_and_stale_only_windows_have_no_valid_sample", func(t *testing.T) {
		samples := []sample{
			{1, decimal.StaleNaN}, {2, nan}, {3, decimal.StaleNaN},
			{10, 5},
		}
		run(t, samples, 2, low, high)
	})
}

// TestHysteresisWindowAggregatorRandom performs an exhaustive randomized cross-check of
// the two-stack aggregator against the per-window three-state reference, including
// duplicate timestamps, ordinary NaNs and multiple stale segments.
func TestHysteresisWindowAggregatorRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	const low, high = 2.0, 8.0

	for iter := 0; iter < 200; iter++ {
		n := rng.Intn(120) + 1
		values := make([]float64, n)
		timestamps := make([]int64, n)
		var ts int64
		for i := 0; i < n; i++ {
			// Occasionally repeat timestamps to emulate un-deduplicated samples.
			if i > 0 && rng.Intn(5) != 0 {
				ts += int64(rng.Intn(3))
			}
			timestamps[i] = ts
			switch rng.Intn(10) {
			case 0:
				values[i] = low - float64(rng.Intn(5)) // below low
			case 1:
				values[i] = low // exactly low
			case 2:
				values[i] = low + 1 + rng.Float64()*(high-low-2) // in-band
			case 3:
				values[i] = high // exactly high
			case 4:
				values[i] = high + float64(rng.Intn(5)) // above high
			case 5:
				values[i] = nan // ordinary NaN, ignored
			case 6:
				values[i] = decimal.StaleNaN // stale marker, reset
			case 7:
				values[i] = math.Inf(rng.Intn(2)*2 - 1) // +/- infinity
			default:
				values[i] = float64(rng.Intn(13)) - 2 // mixed finite values
			}
		}

		windowWidth := int64(rng.Intn(15) + 1)
		step := int64(rng.Intn(3) + 1)
		var agg hysteresisWindowAggregator
		agg.reset(low, high)

		for end := timestamps[0]; end <= timestamps[n-1]; end += step {
			tStart := end - windowWidth
			i, j := 0, 0
			for i < n && timestamps[i] <= tStart {
				i++
			}
			for j < n && timestamps[j] <= end {
				j++
			}
			agg.advance(tStart, values[i:j], timestamps[i:j])
			got := agg.total()

			wantState, wantCycles, wantValid := hysteresisReference(low, high, values[i:j], hysteresisStateUnarmed)
			if got.hasValidSample != wantValid {
				t.Fatalf("iter=%d end=%d: hasValid got %v; want %v", iter, end, got.hasValidSample, wantValid)
			}
			if wantValid {
				if got.cycles[hysteresisStateUnarmed] != wantCycles {
					t.Fatalf("iter=%d end=%d: cycles got %d; want %d", iter, end, got.cycles[hysteresisStateUnarmed], wantCycles)
				}
				if got.nextState[hysteresisStateUnarmed] != wantState {
					t.Fatalf("iter=%d end=%d: final state got %d; want %d", iter, end, got.nextState[hysteresisStateUnarmed], wantState)
				}
			}
			// The monoid summary must agree with the reference for ALL finite initial states.
			for initialState := 0; initialState < hysteresisStatesCount; initialState++ {
				refState, refCycles, _ := hysteresisReference(low, high, values[i:j], uint8(initialState))
				if got.nextState[initialState] != refState || got.cycles[initialState] != refCycles {
					t.Fatalf("iter=%d end=%d initialState=%d: got (state=%d, cycles=%d); want (state=%d, cycles=%d)",
						iter, end, initialState, got.nextState[initialState], got.cycles[initialState], refState, refCycles)
				}
			}
		}
	}
}

// TestHysteresisWindowAggregatorThresholdChange ensures summaries built for one pair of
// thresholds are not reused after the per-point scalars change.
func TestHysteresisWindowAggregatorThresholdChange(t *testing.T) {
	values := []float64{0, 10, 0, 10, 0}
	timestamps := []int64{1, 2, 3, 4, 5}

	// First bind to low=0, high=10: two complete cycles.
	var agg hysteresisWindowAggregator
	agg.reset(0, 10)
	agg.advance(-100, values, timestamps)
	seg := agg.total()
	if seg.cycles[hysteresisStateUnarmed] != 2 {
		t.Fatalf("expected 2 cycles with thresholds (0,10); got %d", seg.cycles[hysteresisStateUnarmed])
	}

	// Rebind to low=-100, high=100: no sample reaches either threshold, so zero cycles
	// and the machine stays unarmed.
	agg.reset(-100, 100)
	agg.advance(-100, values, timestamps)
	seg = agg.total()
	if seg.cycles[hysteresisStateUnarmed] != 0 || seg.nextState[hysteresisStateUnarmed] != hysteresisStateUnarmed {
		t.Fatalf("expected no cycles and unarmed state with thresholds (-100,100); got %+v", seg)
	}
}

func TestRollupHysteresisCountOverTimeFactoryErrors(t *testing.T) {
	scalar := func(v float64) any {
		return []*timeseries{{Values: []float64{v}}}
	}

	if _, err := newRollupHysteresisCountOverTime([]any{scalar(0), scalar(1)}); err == nil {
		t.Fatalf("expected error for 2 args")
	}
	if _, err := newRollupHysteresisCountOverTime([]any{"not-a-scalar", scalar(1), nil}); err == nil {
		t.Fatalf("expected error for non-scalar low arg")
	}
	if _, err := newRollupHysteresisCountOverTime([]any{scalar(1), scalar(1), nil}); err == nil {
		t.Fatalf("expected error for low == high")
	}
	if _, err := newRollupHysteresisCountOverTime([]any{scalar(2), scalar(1), nil}); err == nil {
		t.Fatalf("expected error for low > high")
	}
	if _, err := newRollupHysteresisCountOverTime([]any{scalar(nan), scalar(1), nil}); err == nil {
		t.Fatalf("expected error for NaN low")
	}
	if _, err := newRollupHysteresisCountOverTime([]any{scalar(0), scalar(math.Inf(1)), nil}); err == nil {
		t.Fatalf("expected error for infinite high")
	}
}

// TestRollupHysteresisCountOverTime drives the actual rollup function over emulated
// rollupConfig.Do() windows and cross-checks every output point with the reference.
func TestRollupHysteresisCountOverTime(t *testing.T) {
	const low, high = 1.0, 9.0
	values := []float64{
		0, 5, 10, 2, 0, // cycle 1
		nan, 10, 1, // cycle 2 (NaN ignored)
		decimal.StaleNaN, 10, 5, // stale reset; high while unarmed is ignored
		0, 10, 1, // cycle 3 after the stale segment
		5, // trailing in-band sample, state stays armed
	}
	timestamps := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150}
	const window int64 = 45
	const step int64 = 20

	// Pre-compute the number of output points so the scalar args have one value per point,
	// exactly like the engine produces when evaluating scalar expressions.
	pointCount := int((timestamps[len(timestamps)-1]-timestamps[0])/step) + 1
	lows := make([]float64, pointCount)
	highs := make([]float64, pointCount)
	for i := range lows {
		lows[i] = low
		highs[i] = high
	}

	rf, err := newRollupHysteresisCountOverTime([]any{
		[]*timeseries{{Values: lows}},
		[]*timeseries{{Values: highs}},
		nil,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	rfa := &rollupFuncArg{window: window}
	idx := 0
	for tEnd := timestamps[0]; tEnd <= timestamps[len(timestamps)-1]; tEnd += step {
		tStart := tEnd - window
		i, j := 0, 0
		for i < len(timestamps) && timestamps[i] <= tStart {
			i++
		}
		for j < len(timestamps) && timestamps[j] <= tEnd {
			j++
		}
		rfa.values = values[i:j]
		rfa.timestamps = timestamps[i:j]
		rfa.currTimestamp = tEnd
		rfa.idx = idx
		idx++

		got := rf(rfa)
		_, wantCycles, hasValid := hysteresisReference(low, high, values[i:j], hysteresisStateUnarmed)
		if !hasValid {
			if !math.IsNaN(got) {
				t.Fatalf("tEnd=%d: got %v; want NaN for window without valid samples", tEnd, got)
			}
			continue
		}
		if math.IsNaN(got) || uint64(got) != wantCycles {
			t.Fatalf("tEnd=%d: got %v; want %d cycles", tEnd, got, wantCycles)
		}
	}
	if rfa.hysteresisAgg == nil {
		t.Fatalf("the rollup function must keep its sliding aggregator on rfa")
	}
}

// TestRollupHysteresisCountOverTimePerPointScalars verifies that low/high may change
// between evaluation points while staying fixed inside each window.
func TestRollupHysteresisCountOverTimePerPointScalars(t *testing.T) {
	values := []float64{0, 10, 0, 10, 0}
	timestamps := []int64{1, 2, 3, 4, 5}
	lows := []float64{0, -100}
	highs := []float64{10, 100}

	rf, err := newRollupHysteresisCountOverTime([]any{
		[]*timeseries{{Values: lows}},
		[]*timeseries{{Values: highs}},
		nil,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	rfa := &rollupFuncArg{window: 100}
	for idx, low := range lows {
		high := highs[idx]
		rfa.values = values
		rfa.timestamps = timestamps
		rfa.currTimestamp = 100 + int64(idx)
		rfa.idx = idx
		got := rf(rfa)
		_, wantCycles, hasValid := hysteresisReference(low, high, values, hysteresisStateUnarmed)
		if !hasValid || math.IsNaN(got) || uint64(got) != wantCycles {
			t.Fatalf("idx=%d: got %v; want %d cycles (hasValid=%v)", idx, got, wantCycles, hasValid)
		}
	}
}

// TestRollupHysteresisCountOverTimeEmptyWindows checks the NaN vs zero distinction.
func TestRollupHysteresisCountOverTimeEmptyWindows(t *testing.T) {
	rf, err := newRollupHysteresisCountOverTime([]any{
		[]*timeseries{{Values: []float64{0, 0, 0}}},
		[]*timeseries{{Values: []float64{10, 10, 10}}},
		nil,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	rfa := &rollupFuncArg{
		window:        10,
		currTimestamp: 100,
		idx:           0,
	}
	// No samples at all: NaN.
	if got := rf(rfa); !math.IsNaN(got) {
		t.Fatalf("empty window must return NaN; got %v", got)
	}

	// Only stale markers and ordinary NaNs: still no valid sample, so NaN.
	rfa.values = []float64{decimal.StaleNaN, nan, decimal.StaleNaN}
	rfa.timestamps = []int64{95, 96, 97}
	if got := rf(rfa); !math.IsNaN(got) {
		t.Fatalf("stale/NaN-only window must return NaN; got %v", got)
	}

	// A valid in-band sample without any completed cycle returns zero, not NaN.
	rfa.values = []float64{5}
	rfa.timestamps = []int64{98}
	rfa.idx = 1
	if got := rf(rfa); got != 0 {
		t.Fatalf("valid window without cycles must return 0; got %v", got)
	}

	// A high value observed while unarmed also yields zero.
	rfa.values = []float64{42}
	rfa.timestamps = []int64{99}
	rfa.idx = 2
	if got := rf(rfa); got != 0 {
		t.Fatalf("unarmed high value must return 0 cycles; got %v", got)
	}
}
