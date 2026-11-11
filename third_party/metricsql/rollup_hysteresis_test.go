package metricsql

import "testing"

func TestHysteresisCountOverTimeRegistered(t *testing.T) {
	if !IsRollupFunc("hysteresis_count_over_time") {
		t.Fatalf("hysteresis_count_over_time must be registered as a rollup function")
	}
	// Function name matching is case-insensitive.
	if !IsRollupFunc("Hysteresis_Count_Over_Time") {
		t.Fatalf("function names must be matched case-insensitively")
	}
	// Unrelated names must remain unknown.
	if IsRollupFunc("hysteresis_count_over_time_unknown") {
		t.Fatalf("unknown function name must not be registered")
	}

	expr, err := Parse(`hysteresis_count_over_time(1, 2, foo[5m])`)
	if err != nil {
		t.Fatalf("unexpected parse error: %s", err)
	}
	fe, ok := expr.(*FuncExpr)
	if !ok {
		t.Fatalf("unexpected expression type: %T", expr)
	}
	if fe.Name != "hysteresis_count_over_time" || len(fe.Args) != 3 {
		t.Fatalf("unexpected function expression: %s", fe.AppendString(nil))
	}
	// The series[window] argument is the third one (index 2).
	if idx := GetRollupArgIdx(fe); idx != 2 {
		t.Fatalf("unexpected rollup arg index: got %d; want 2", idx)
	}
}

func TestHysteresisCountOverTimeParseRoundTrip(t *testing.T) {
	same := func(s string) {
		t.Helper()
		e, err := Parse(s)
		if err != nil {
			t.Fatalf("unexpected error when parsing %s: %s", s, err)
		}
		if got := string(e.AppendString(nil)); got != s {
			t.Fatalf("unexpected string constructed;\ngot\n%s\nwant\n%s", got, s)
		}
		fe, ok := e.(*FuncExpr)
		if !ok {
			t.Fatalf("unexpected expression type: %T", e)
		}
		if idx := GetRollupArgIdx(fe); idx != 2 {
			t.Fatalf("unexpected rollup arg index for %s: got %d; want 2", s, idx)
		}
	}

	same(`hysteresis_count_over_time(-1, 2.5, foo[5m])`)
	same(`hysteresis_count_over_time(1, 2, bar[5m:10s] offset 1m)`)
	same(`hysteresis_count_over_time(1, 2, baz[5m] @ 12345)`)
	same(`hysteresis_count_over_time(1, 2, qux[5m:10s] offset 1m @ end())`)
}
