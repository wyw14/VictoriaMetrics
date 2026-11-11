package promql

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metricsql"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// fakeReader replaces evalRollupFuncNoCache in tests.
//
// It returns deterministic timeseries for the requested metric selectors,
// counts every storage-backed fetch (the "reader" metric), can block until
// unblocked or canceled, and can inject failures for chosen selectors.
type fakeReader struct {
	calls atomic.Int64

	// entered is closed once when the first fetch starts.
	entered chan struct{}
	once    sync.Once

	// blockCh, when non-nil, makes every fetch block until the channel is
	// closed or the evaluation context is canceled.
	blockCh chan struct{}

	// failMetric, when non-empty, makes fetches of the metric with this
	// name fail.
	failMetric string
}

func (fr *fakeReader) read(qt *querytracer.Tracer, ec *EvalConfig, funcName string, _ rollupFunc,
	_ metricsql.Expr, me *metricsql.MetricExpr, _ *incrementalAggrFuncContext, window, pointsPerSeries int64,
) ([]*timeseries, error) {
	if ec.batch != nil {
		ec.batch.onReader()
	}
	fr.calls.Add(1)
	if fr.entered != nil {
		fr.once.Do(func() { close(fr.entered) })
	}
	if fr.blockCh != nil {
		select {
		case <-fr.blockCh:
		case <-ec.Context.Done():
			return nil, searchutil.AnnotateContextError(ec.Context)
		}
	}
	if err := ec.Context.Err(); err != nil {
		return nil, searchutil.AnnotateContextError(ec.Context)
	}

	timestamps := getTimestamps(ec.Start, ec.End, ec.Step, ec.MaxPointsPerSeries)
	var rvs []*timeseries
	for _, lfs := range me.LabelFilterss {
		var mn storage.MetricName
		for _, lf := range lfs {
			if lf.Label == "__name__" {
				mn.MetricGroup = []byte(lf.Value)
				if lf.Value == fr.failMetric {
					return nil, fmt.Errorf("injected reader failure for metric %q", lf.Value)
				}
			} else {
				mn.Tags = append(mn.Tags, storage.Tag{
					Key:   []byte(lf.Label),
					Value: []byte(lf.Value),
				})
			}
		}
		h := fnv.New32a()
		_, _ = h.Write(mn.MetricGroup)
		_, _ = h.Write([]byte(funcName))
		var tagBuf []byte
		for _, tag := range mn.Tags {
			tagBuf = append(tagBuf, tag.Key...)
			tagBuf = append(tagBuf, tag.Value...)
		}
		_, _ = h.Write(tagBuf)
		base := float64(1 + h.Sum32()%7)

		values := make([]float64, len(timestamps))
		for i := range values {
			values[i] = base + float64(i)*0.5
		}
		// Include a NaN gap so mutating binary operators (or/default/ifnot)
		// exercise their in-place value writes during CoW tests.
		if len(values) > 1 {
			values[1] = nan
		}
		rvs = append(rvs, &timeseries{
			MetricName: mn,
			Values:     values,
			Timestamps: timestamps,
		})
	}
	return rvs, nil
}

func installFakeReader(t *testing.T, fr *fakeReader) {
	t.Helper()
	orig := evalRollupFuncNoCacheFn
	evalRollupFuncNoCacheFn = fr.read
	t.Cleanup(func() {
		evalRollupFuncNoCacheFn = orig
	})
}

func newTestEvalConfig(ctx context.Context) *EvalConfig {
	const start = 100_000
	const end = 600_000
	const step = 100_000
	return &EvalConfig{
		Context:            ctx,
		Start:              start,
		End:                end,
		Step:               step,
		MaxPointsPerSeries: 1000,
		MayCache:           false,
		LookbackDelta:      step,
		QueryStats:         &QueryStats{},
		GetRequestURI:      func() string { return "/batch_test" },
	}
}

func testTracer() *querytracer.Tracer {
	return querytracer.New(false, "batch_test")
}

// evalSequential evaluates queries one by one without any batch session,
// mirroring the original evalExpr behavior and serving as the correctness
// baseline for every batch test.
func evalSequential(t *testing.T, ec *EvalConfig, queries []string) [][]netstorage.Result {
	t.Helper()
	if ec.batch != nil {
		t.Fatalf("baseline eval config must not carry a batch session")
	}
	results := make([][]netstorage.Result, len(queries))
	qt := testTracer()
	for i, query := range queries {
		e, err := parsePromQLWithCache(query)
		if err != nil {
			t.Fatalf("unexpected parse error for %q: %s", query, err)
		}
		rv, err := evalExpr(qt, ec, e)
		if err != nil {
			t.Fatalf("unexpected eval error for %q: %s", query, err)
		}
		results[i], err = finalizeEvalResult(qt, ec, e, rv, false)
		if err != nil {
			t.Fatalf("unexpected finalize error for %q: %s", query, err)
		}
	}
	return results
}

func evalBatch(t *testing.T, ec *EvalConfig, queries []string, opts BatchEvalOptions) []BatchEvalItemResult {
	t.Helper()
	return BatchEval(testTracer(), ec, queries, opts)
}

func assertResultsEqual(t *testing.T, got, want []netstorage.Result, context string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: series count mismatch; got %d; want %d", context, len(got), len(want))
	}
	for i := range got {
		if got[i].MetricName.String() != want[i].MetricName.String() {
			t.Fatalf("%s: metric name mismatch at series %d; got %s; want %s", context, i, got[i].MetricName.String(), want[i].MetricName.String())
		}
		if len(got[i].Values) != len(want[i].Values) {
			t.Fatalf("%s: values length mismatch at series %d; got %d; want %d", context, i, len(got[i].Values), len(want[i].Values))
		}
		for j := range got[i].Values {
			gv, wv := got[i].Values[j], want[i].Values[j]
			if math.IsNaN(gv) || math.IsNaN(wv) {
				if !(math.IsNaN(gv) && math.IsNaN(wv)) {
					t.Fatalf("%s: NaN mismatch at %d/%d; got %v; want %v", context, i, j, gv, wv)
				}
				continue
			}
			if gv != wv {
				t.Fatalf("%s: value mismatch at %d/%d; got %v; want %v", context, i, j, gv, wv)
			}
		}
		if len(got[i].Timestamps) != len(want[i].Timestamps) {
			t.Fatalf("%s: timestamps length mismatch at series %d; got %d; want %d", context, i, len(got[i].Timestamps), len(want[i].Timestamps))
		}
		for j := range got[i].Timestamps {
			if got[i].Timestamps[j] != want[i].Timestamps[j] {
				t.Fatalf("%s: timestamp mismatch at %d/%d; got %d; want %d", context, i, j, got[i].Timestamps[j], want[i].Timestamps[j])
			}
		}
	}
}

// TestBatchEvalSharedRollupDifferentAggregations verifies that a shared
// rollup subgraph is fetched once even when it feeds different aggregate
// functions, while every result matches the sequential evalExpr baseline.
func TestBatchEvalSharedRollupDifferentAggregations(t *testing.T) {
	fr := &fakeReader{}
	installFakeReader(t, fr)

	queries := []string{
		"sum(rate(m[5m]) + 0)",
		"max(rate(m[5m]) + 0)",
		"avg(rate(m[5m]) + 0)",
	}

	ecBase := newTestEvalConfig(context.Background())
	want := evalSequential(t, ecBase, queries)
	seqReaders := fr.calls.Load()
	if seqReaders != int64(len(queries)) {
		t.Fatalf("unexpected sequential reader count; got %d; want %d", seqReaders, len(queries))
	}

	ecBatch := newTestEvalConfig(context.Background())
	results := evalBatch(t, ecBatch, queries, BatchEvalOptions{})
	if len(results) != len(queries) {
		t.Fatalf("unexpected result count; got %d; want %d", len(results), len(queries))
	}
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("unexpected error at item %d: %s", i, res.Err)
		}
		assertResultsEqual(t, res.Result, want[i], fmt.Sprintf("item %d", i))
	}

	batchReaders := fr.calls.Load() - seqReaders
	if batchReaders != 1 {
		t.Fatalf("shared rollup must be fetched once in batch mode; got %d reader calls", batchReaders)
	}
}

// TestBatchEvalMutatingBinaryOpCopyOnWrite verifies that in-place mutations
// performed by binary operators cannot pollute sibling consumers of the
// same shared timeseries arrays.
func TestBatchEvalMutatingBinaryOpCopyOnWrite(t *testing.T) {
	fr := &fakeReader{}
	installFakeReader(t, fr)

	// or/default do not push down common filters, so every structural
	// occurrence of m keeps the same semantic key and is shared. Both
	// operators mutate input Values slices in place (NaN gap filling and
	// NaN writes), which would corrupt sibling consumers without CoW clones.
	queries := []string{
		"m or m",
		"m default m",
	}

	want := evalSequential(t, newTestEvalConfig(context.Background()), queries)
	fr.calls.Store(0)
	ecBatch := newTestEvalConfig(context.Background())
	results := evalBatch(t, ecBatch, queries, BatchEvalOptions{})

	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("unexpected error at item %d: %s", i, res.Err)
		}
		assertResultsEqual(t, res.Result, want[i], fmt.Sprintf("item %d", i))
	}

	// Every query references m twice; all structural occurrences across the
	// batch must resolve to a single rollup fetch.
	if fr.calls.Load() != 1 {
		t.Fatalf("all m selectors must share a single reader call; got %d", fr.calls.Load())
	}
}

// TestBatchEvalVolatileFunctions verifies that unseeded volatile subgraphs
// are never shared across originally independent evaluations, while
// deterministic subgraphs keep being shared.
func TestBatchEvalVolatileFunctions(t *testing.T) {
	fr := &fakeReader{}
	installFakeReader(t, fr)

	// Unseeded rand: the binary node and rand itself are volatile and must
	// not be interned; the deterministic m subgraph remains shared.
	volatileQueries := []string{
		"rand() + m",
		"rand() + m",
	}
	ecBatch := newTestEvalConfig(context.Background())
	results := evalBatch(t, ecBatch, volatileQueries, BatchEvalOptions{})
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("unexpected error at volatile item %d: %s", i, res.Err)
		}
	}
	if fr.calls.Load() != 1 {
		t.Fatalf("deterministic m under volatile parents must be fetched once; got %d", fr.calls.Load())
	}

	// Structural identity must exclude every volatile subtree from sharing.
	randExpr := mustParseExpr(t, "rand() + m")
	randVolatile := make(map[metricsql.Expr]struct{})
	markVolatileExprs(randExpr, randVolatile)
	if _, ok := randVolatile[randExpr]; !ok {
		t.Fatalf("rand() + m root must be marked volatile")
	}
	randFunc := findFuncExpr(t, randExpr, "rand")
	if _, ok := randVolatile[randFunc]; !ok {
		t.Fatalf("unseeded rand() must be marked volatile")
	}
	mUnderRand := findMetricExpr(t, randExpr)
	if _, ok := randVolatile[mUnderRand]; ok {
		t.Fatalf("deterministic m selector under rand must not be marked volatile")
	}

	// now() is wall-clock dependent and must not be shared either.
	nowSession := newBatchSession(context.Background(), batchLimits{
		maxExpressions: 10, maxDagNodes: 1000, maxResultBytes: 1 << 30, maxWorkUnits: 1000,
	})
	nowExpr := mustParseExpr(t, "now()")
	if _, ok := nowSession.sharedStructural[planStructuralKey(nowExpr)]; ok {
		t.Fatalf("now() must not be registered as a shareable structural node")
	}
	volatileMarks := make(map[metricsql.Expr]struct{})
	markVolatileExprs(nowExpr, volatileMarks)
	if _, ok := volatileMarks[nowExpr]; !ok {
		t.Fatalf("now() must be marked volatile")
	}

	// Seeded rand is deterministic: the whole expression is shareable and
	// both items must return identical results.
	seededQueries := []string{
		"rand(42) + m",
		"rand(42) + m",
	}
	seededExpr := mustParseExpr(t, seededQueries[0])
	seededVolatile := make(map[metricsql.Expr]struct{})
	markVolatileExprs(seededExpr, seededVolatile)
	if len(seededVolatile) != 0 {
		t.Fatalf("seeded rand must be deterministic, got volatile nodes: %d", len(seededVolatile))
	}
	want := evalSequential(t, newTestEvalConfig(context.Background()), seededQueries)
	fr.calls.Store(0)
	seededResults := evalBatch(t, newTestEvalConfig(context.Background()), seededQueries, BatchEvalOptions{})
	for i, res := range seededResults {
		if res.Err != nil {
			t.Fatalf("unexpected error at seeded item %d: %s", i, res.Err)
		}
		assertResultsEqual(t, res.Result, want[i], fmt.Sprintf("seeded item %d", i))
	}
	if fr.calls.Load() != 1 {
		t.Fatalf("m under seeded rand must be fetched once; got %d", fr.calls.Load())
	}
}

func findFuncExpr(t *testing.T, e metricsql.Expr, name string) *metricsql.FuncExpr {
	t.Helper()
	var found *metricsql.FuncExpr
	metricsql.VisitAll(e, func(expr metricsql.Expr) {
		if fe, ok := expr.(*metricsql.FuncExpr); ok && strings.EqualFold(fe.Name, name) {
			found = fe
		}
	})
	if found == nil {
		t.Fatalf("function %q not found in %s", name, e.AppendString(nil))
	}
	return found
}

func findMetricExpr(t *testing.T, e metricsql.Expr) *metricsql.MetricExpr {
	t.Helper()
	var found *metricsql.MetricExpr
	metricsql.VisitAll(e, func(expr metricsql.Expr) {
		if me, ok := expr.(*metricsql.MetricExpr); ok {
			found = me
		}
	})
	if found == nil {
		t.Fatalf("metric selector not found in %s", e.AppendString(nil))
	}
	return found
}

// TestBatchEvalPartialCancellation verifies independent waiter cancellation
// and that the underlying shared work is canceled only when the last demand
// disappears.
func TestBatchEvalPartialCancellation(t *testing.T) {
	fr := &fakeReader{
		entered: make(chan struct{}),
		blockCh: make(chan struct{}),
	}
	installFakeReader(t, fr)

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	session := newBatchSession(rootCtx, batchLimits{
		maxExpressions: 10, maxDagNodes: 1000, maxResultBytes: 1 << 30, maxWorkUnits: 1000,
	})
	expr := mustParseExpr(t, "m")
	session.sharedStructural[planStructuralKey(expr)] = struct{}{}

	ec1 := newTestEvalConfig(rootCtx)
	ec1.batch = session
	ctx2, cancel2 := context.WithCancel(rootCtx)
	ec2 := newTestEvalConfig(ctx2)
	ec2.batch = session

	var wg sync.WaitGroup
	var rv1 []*timeseries
	var err1, err2 error
	waiterDone := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		rv1, err1 = evalExpr(testTracer(), ec1, expr)
	}()
	go func() {
		defer wg.Done()
		_, err2 = evalExpr(testTracer(), ec2, expr)
		close(waiterDone)
	}()

	select {
	case <-fr.entered:
	case <-time.After(5 * time.Second):
		t.Fatalf("fake reader never started")
	}

	// The shared node is still blocked with two active consumers.
	cancel2()
	select {
	case <-waiterDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("waiter did not return after independent cancellation")
	}
	if err2 == nil {
		t.Fatalf("canceled waiter must return a context error")
	}
	if !errors.Is(err2, context.Canceled) {
		t.Fatalf("waiter error must wrap context.Canceled; got %s", err2)
	}
	if err1 != nil || rv1 != nil {
		t.Fatalf("the remaining owner must keep waiting while demand exists; err=%v rv=%v", err1, rv1)
	}

	// Releasing the last demand (root request cancellation) must cancel the
	// underlying reader as well.
	rootCancel()
	close(fr.blockCh)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("owner goroutine never finished after request cancellation")
	}
	if err1 == nil {
		t.Fatalf("owner must return an error once the request context is canceled")
	}
}

// TestBatchEvalIndependentErrors verifies per-item error isolation: parse
// errors and reader failures never leak into unrelated expressions.
func TestBatchEvalIndependentErrors(t *testing.T) {
	fr := &fakeReader{failMetric: "bad_metric"}
	installFakeReader(t, fr)

	queries := []string{
		"m",
		"this is (((",
		"bad_metric",
		"m",
	}
	results := evalBatch(t, newTestEvalConfig(context.Background()), queries, BatchEvalOptions{})
	if len(results) != len(queries) {
		t.Fatalf("unexpected result count; got %d; want %d", len(results), len(queries))
	}
	if results[0].Err != nil || results[3].Err != nil {
		t.Fatalf("valid m items must succeed; got errors: %v, %v", results[0].Err, results[3].Err)
	}
	assertResultsEqual(t, results[0].Result, results[3].Result, "shared m items")
	if results[1].Err == nil {
		t.Fatalf("parse error item must return an error")
	}
	if results[2].Err == nil {
		t.Fatalf("reader failure item must return an error")
	}
	// The failure of bad_metric must not be cached for the unrelated m items.
	if errors.Is(results[0].Err, context.Canceled) || results[0].Err != nil {
		t.Fatalf("unrelated item must not inherit another item's error: %v", results[0].Err)
	}
}

// TestBatchEvalBudgets verifies all static and dynamic budget limits.
func TestBatchEvalBudgets(t *testing.T) {
	fr := &fakeReader{}
	installFakeReader(t, fr)

	// Expression count budget.
	results := evalBatch(t, newTestEvalConfig(context.Background()), []string{"m", "m", "m"}, BatchEvalOptions{
		MaxExpressions: 1,
	})
	var budgetErr *BatchBudgetError
	if results[0].Err != nil {
		t.Fatalf("the first expression must fit the budget; got %s", results[0].Err)
	}
	for i := 1; i < 3; i++ {
		if !errors.As(results[i].Err, &budgetErr) {
			t.Fatalf("item %d must carry BatchBudgetError; got %v", i, results[i].Err)
		}
	}

	// DAG node budget: plain m has 1 AST node, so only one item fits.
	results = evalBatch(t, newTestEvalConfig(context.Background()), []string{"m", "m"}, BatchEvalOptions{
		MaxDAGNodes: 1,
	})
	if results[0].Err != nil {
		t.Fatalf("the first expression must fit the DAG budget; got %s", results[0].Err)
	}
	if !errors.As(results[1].Err, &budgetErr) {
		t.Fatalf("second item must fail with DAG budget error; got %v", results[1].Err)
	}

	// Work unit budget: each shared node consumes one owner work unit.
	results = evalBatch(t, newTestEvalConfig(context.Background()), []string{"m", "m", "k", "k"}, BatchEvalOptions{
		MaxWorkUnits: 1,
	})
	var failedItems int
	for i, res := range results {
		if errors.As(res.Err, &budgetErr) {
			failedItems++
		} else if res.Err != nil {
			t.Fatalf("unexpected non-budget error at item %d: %s", i, res.Err)
		}
	}
	if failedItems == 0 {
		t.Fatalf("work unit budget must produce at least one budget error")
	}

	// Result byte budget: drive the session sequentially so that the first
	// consumer computes the node and exhausts the budget, while the second
	// consumer deterministically receives a per-item budget error.
	fr.calls.Store(0)
	session := newBatchSession(context.Background(), batchLimits{
		maxExpressions: 10, maxDagNodes: 1000, maxResultBytes: 1, maxWorkUnits: 1000,
	})
	expr := mustParseExpr(t, "m")
	session.sharedStructural[planStructuralKey(expr)] = struct{}{}
	ecA := newTestEvalConfig(context.Background())
	ecA.batch = session
	if _, err := evalExpr(testTracer(), ecA, expr); err != nil {
		t.Fatalf("the owner that computes the node must keep its result: %s", err)
	}
	ecB := newTestEvalConfig(context.Background())
	ecB.batch = session
	if _, err := evalExpr(testTracer(), ecB, expr); !errors.As(err, &budgetErr) {
		t.Fatalf("second consumer must receive result byte budget error; got %v", err)
	}
}

// TestBatchEvalSemanticIdentity verifies that identity covers the full
// effective EvalConfig, not just the printed expression.
func TestBatchEvalSemanticIdentity(t *testing.T) {
	fr := &fakeReader{}
	installFakeReader(t, fr)

	rootCtx := context.Background()
	session := newBatchSession(rootCtx, batchLimits{
		maxExpressions: 10, maxDagNodes: 1000, maxResultBytes: 1 << 30, maxWorkUnits: 1000,
	})
	expr := mustParseExpr(t, "m")
	session.sharedStructural[planStructuralKey(expr)] = struct{}{}

	// Different lookback delta => different full key => no sharing.
	ec1 := newTestEvalConfig(rootCtx)
	ec1.LookbackDelta = ec1.Step
	ec1.batch = session
	ec2 := newTestEvalConfig(rootCtx)
	ec2.LookbackDelta = ec2.Step * 2
	ec2.batch = session

	if _, err := evalExpr(testTracer(), ec1, expr); err != nil {
		t.Fatalf("first eval failed: %s", err)
	}
	if _, err := evalExpr(testTracer(), ec2, expr); err != nil {
		t.Fatalf("second eval failed: %s", err)
	}
	if fr.calls.Load() != 2 {
		t.Fatalf("different lookback must prevent sharing; got %d readers; want 2", fr.calls.Load())
	}

	// Different time grid => no sharing.
	fr.calls.Store(0)
	ec3 := newTestEvalConfig(rootCtx)
	ec3.Start += ec3.Step
	ec3.batch = session
	if _, err := evalExpr(testTracer(), ec3, expr); err != nil {
		t.Fatalf("third eval failed: %s", err)
	}
	if fr.calls.Load() != 1 {
		t.Fatalf("different time grid must create a new shared node; got %d readers", fr.calls.Load())
	}

	// Identical config and AST => reuse.
	fr.calls.Store(0)
	ec4 := newTestEvalConfig(rootCtx)
	ec4.LookbackDelta = ec1.Step
	ec4.batch = session
	if _, err := evalExpr(testTracer(), ec4, expr); err != nil {
		t.Fatalf("fourth eval failed: %s", err)
	}
	if fr.calls.Load() != 0 {
		t.Fatalf("identical config must reuse the existing shared node; got %d readers", fr.calls.Load())
	}

	// Enforced tag filters are part of the identity.
	fr.calls.Store(0)
	ec5 := newTestEvalConfig(rootCtx)
	ec5.LookbackDelta = ec1.Step
	ec5.EnforcedTagFilterss = [][]storage.TagFilter{{
		{Key: []byte("tenant"), Value: []byte("b")},
	}}
	ec5.batch = session
	if _, err := evalExpr(testTracer(), ec5, expr); err != nil {
		t.Fatalf("fifth eval failed: %s", err)
	}
	if fr.calls.Load() != 1 {
		t.Fatalf("different enforced filters must prevent sharing; got %d readers", fr.calls.Load())
	}
}

// TestBatchEvalOffsetAndAtIdentity verifies that offset and @ modifiers are
// part of the shared identity, so differently modified selectors are not
// incorrectly merged while identical modifiers still share the fetch.
func TestBatchEvalOffsetAndAtIdentity(t *testing.T) {
	fr := &fakeReader{}
	installFakeReader(t, fr)

	// Different offsets must produce independent fetches.
	differentOffset := []string{
		"m offset 1m",
		"m offset 2m",
	}
	want := evalSequential(t, newTestEvalConfig(context.Background()), differentOffset)
	fr.calls.Store(0)
	results := evalBatch(t, newTestEvalConfig(context.Background()), differentOffset, BatchEvalOptions{})
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("unexpected error for offset item %d: %s", i, res.Err)
		}
		assertResultsEqual(t, res.Result, want[i], fmt.Sprintf("offset item %d", i))
	}
	if fr.calls.Load() != 2 {
		t.Fatalf("different offsets must not be shared; got %d readers; want 2", fr.calls.Load())
	}

	// Identical offsets collapse to a single fetch.
	identicalOffset := []string{
		"m offset 1m",
		"m offset 1m",
	}
	want = evalSequential(t, newTestEvalConfig(context.Background()), identicalOffset)
	fr.calls.Store(0)
	results = evalBatch(t, newTestEvalConfig(context.Background()), identicalOffset, BatchEvalOptions{})
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("unexpected error for identical offset item %d: %s", i, res.Err)
		}
		assertResultsEqual(t, res.Result, want[i], fmt.Sprintf("identical offset item %d", i))
	}
	if fr.calls.Load() != 1 {
		t.Fatalf("identical offsets must share a single fetch; got %d readers; want 1", fr.calls.Load())
	}

	// Different @ timestamps must also prevent sharing.
	differentAt := []string{
		"m @ 200000",
		"m @ 300000",
	}
	want = evalSequential(t, newTestEvalConfig(context.Background()), differentAt)
	fr.calls.Store(0)
	results = evalBatch(t, newTestEvalConfig(context.Background()), differentAt, BatchEvalOptions{})
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("unexpected error for @ item %d: %s", i, res.Err)
		}
		assertResultsEqual(t, res.Result, want[i], fmt.Sprintf("@ item %d", i))
	}
	if fr.calls.Load() != 2 {
		t.Fatalf("different @ modifiers must not be shared; got %d readers; want 2", fr.calls.Load())
	}
}

// TestBatchEvalOrderAndEquivalence verifies input order preservation against
// the sequential evalExpr baseline over a mixed workload.
func TestBatchEvalOrderAndEquivalence(t *testing.T) {
	fr := &fakeReader{}
	installFakeReader(t, fr)

	queries := []string{
		"m",
		"sum(rate(k[5m]) + 0)",
		"m / k",
		"avg(m) + max(k)",
		"rate(m[1m])",
	}
	want := evalSequential(t, newTestEvalConfig(context.Background()), queries)
	results := evalBatch(t, newTestEvalConfig(context.Background()), queries, BatchEvalOptions{})
	if len(results) != len(queries) {
		t.Fatalf("unexpected result count; got %d; want %d", len(results), len(queries))
	}
	for i, res := range results {
		if res.Err != nil {
			t.Fatalf("unexpected error at item %d (%q): %s", i, queries[i], res.Err)
		}
		assertResultsEqual(t, res.Result, want[i], queries[i])
	}
}

func mustParseExpr(t *testing.T, q string) metricsql.Expr {
	t.Helper()
	e, err := parsePromQLWithCache(q)
	if err != nil {
		t.Fatalf("cannot parse %q: %s", q, err)
	}
	return e
}
