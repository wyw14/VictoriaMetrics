package promql

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/metricsql"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// BatchBudgetError is returned for batch items, which cannot be evaluated
// because one of the BatchEval budgets is exhausted.
//
// Each failed batch item gets its own BatchBudgetError, while unrelated
// items keep returning their normal results.
type BatchBudgetError struct {
	msg string
}

// Error implements error interface.
func (e *BatchBudgetError) Error() string {
	return e.msg
}

type batchLimits struct {
	maxExpressions int
	maxDagNodes    int
	maxResultBytes int64
	maxWorkUnits   int64
}

// batchSession coordinates deterministic shared subgraph reuse for the
// expressions evaluated within a single BatchEval request.
//
// A node is shared only when both conditions are met:
//
//  1. it appears in at least two of the batch expressions (the structural
//     identity is discovered during planning and stored in sharedStructural);
//  2. its full semantic key, which includes the effective EvalConfig fields
//     (time grid, lookback, enforced tag filters and the other execution
//     options), is identical at the evaluation time.
//
// Volatile subgraphs (unseeded random and wall-clock dependent functions)
// are never added to sharedStructural, so they are evaluated independently
// for every originally independent expression.
type batchSession struct {
	ctx    context.Context
	limits batchLimits

	// sharedStructural contains structural keys of expression nodes,
	// which occur in at least two batch expressions and are safe to share.
	sharedStructural map[string]struct{}

	// nodes contains lazily created execution nodes keyed by the full
	// semantic key (effective config digest + structural key).
	nodes map[string]*batchNode

	// representatives stores a representative AST for every full key.
	// It is used to confirm structural equivalence with reflect.DeepEqual
	// in addition to the encoded key, so equivalence is never decided by
	// a string or a hash alone.
	representatives map[string]metricsql.Expr

	mu sync.Mutex

	dagNodes    atomic.Int64
	workUnits   atomic.Int64
	resultBytes atomic.Int64

	// Readers counts storage-backed rollup fetches executed while the
	// session is active. It is incremented via the evalRollupFuncNoCacheFn
	// test seam and allows verifying that shared subgraphs really perform
	// less underlying work instead of merely reusing final strings.
	readers atomic.Int64

	budgetMu  sync.Mutex
	budgetErr error
}

func newBatchSession(ctx context.Context, limits batchLimits) *batchSession {
	return &batchSession{
		ctx:              ctx,
		limits:           limits,
		sharedStructural: make(map[string]struct{}),
		nodes:            make(map[string]*batchNode),
		representatives:  make(map[string]metricsql.Expr),
	}
}

// onReader is called from the evalRollupFuncNoCacheFn wrapper on every
// storage-backed rollup fetch performed for an EvalConfig attached to
// a batch session.
func (s *batchSession) onReader() {
	s.readers.Add(1)
}

// Readers returns the total number of storage-backed rollup fetches
// performed during the session lifetime.
func (s *batchSession) Readers() int64 {
	return s.readers.Load()
}

// currentBudgetError returns the dynamic budget error, if any has been
// already triggered by result byte or work unit accounting.
func (s *batchSession) currentBudgetError() error {
	s.budgetMu.Lock()
	err := s.budgetErr
	s.budgetMu.Unlock()
	return err
}

// reserveWorkUnit accounts a single owner execution of a shared node.
// It returns an explicit budget error when the work unit limit is reached.
func (s *batchSession) reserveWorkUnit() error {
	s.budgetMu.Lock()
	defer s.budgetMu.Unlock()
	if s.workUnits.Load() >= s.limits.maxWorkUnits {
		return &BatchBudgetError{
			msg: fmt.Sprintf("BatchEval work unit budget exceeded: the limit of %d shared node evaluations is reached; "+
				"increase -search.maxBatchEvalWorkUnits or split the request into smaller batches", s.limits.maxWorkUnits),
		}
	}
	s.workUnits.Add(1)
	return nil
}

func (s *batchSession) addResultBytes(n int64) {
	s.budgetMu.Lock()
	total := s.resultBytes.Add(n)
	if total > s.limits.maxResultBytes && s.budgetErr == nil {
		s.budgetErr = &BatchBudgetError{
			msg: fmt.Sprintf("BatchEval shared result byte budget exceeded: more than %d bytes of shared results are required; "+
				"increase -search.maxBatchEvalResultBytes or split the request into smaller batches", s.limits.maxResultBytes),
		}
	}
	s.budgetMu.Unlock()
}

// batchNode represents a single deterministic shared subgraph evaluation.
//
// Exactly one worker goroutine computes the node (single explicit owner),
// while all the other consumers (waiters) block on done and independently
// give up waiting when their own request context is canceled. The worker
// runs under a context derived from the batch session context, so the
// underlying work is canceled only when the last demand for the node
// disappears (the whole batch request is canceled) or when it finishes.
type batchNode struct {
	done   chan struct{}
	cancel context.CancelFunc

	stateMu sync.Mutex
	result  []*timeseries
	err     error

	// demand is the number of currently active consumers of the node.
	demand int
}

// planStructuralKey builds a config-independent structural key for an AST
// node during the planning phase.
func planStructuralKey(e metricsql.Expr) string {
	dst := appendSemanticKey(nil, e)
	return string(dst)
}

// fullNodeKey builds the complete semantic key of a node, which covers both
// the effective evaluation config and the structural AST identity.
func (s *batchSession) fullNodeKey(ec *EvalConfig, e metricsql.Expr) string {
	dst := appendEvalConfigKey(nil, ec)
	dst = appendSemanticKey(dst, e)
	return string(dst)
}

// evalShared evaluates e with subgraph sharing enabled.
//
// Nodes that do not occur in at least two batch expressions are evaluated
// directly with zero session overhead. Volatile nodes are never shared.
func (s *batchSession) evalShared(qt *querytracer.Tracer, ec *EvalConfig, e metricsql.Expr) ([]*timeseries, error) {
	if err := s.currentBudgetError(); err != nil {
		return nil, err
	}
	if _, ok := s.sharedStructural[planStructuralKey(e)]; !ok {
		// The node has a single consumer or is volatile - evaluate it
		// directly without interning, cloning or any other overhead.
		return evalExprDirect(qt, ec, e)
	}

	key := s.fullNodeKey(ec, e)

	s.mu.Lock()
	node, ok := s.nodes[key]
	if !ok {
		if representative, exists := s.representatives[key]; exists && !reflect.DeepEqual(representative, e) {
			// Defensive handling of a theoretical key collision: keep
			// distinct AST nodes distinct instead of incorrectly sharing
			// their results.
			key = key + fmt.Sprintf("#collision%d", len(s.nodes))
		}
		node = &batchNode{
			done: make(chan struct{}),
		}
		s.nodes[key] = node
		s.representatives[key] = e
		s.dagNodes.Add(1)
	}
	node.stateMu.Lock()
	node.demand++
	node.stateMu.Unlock()
	s.mu.Unlock()

	if !ok {
		// The current goroutine is the single explicit owner: start exactly
		// one worker goroutine for the node.
		if err := s.reserveWorkUnit(); err != nil {
			s.completeNode(node, nil, err)
			close(node.done)
			return s.consumeNode(node, ec)
		}
		ctx, cancel := context.WithCancel(s.ctx)
		node.cancel = cancel
		go s.runWorker(node, qt, ec, e, ctx)
	}
	return s.consumeNode(node, ec)
}

func (s *batchSession) runWorker(node *batchNode, qt *querytracer.Tracer, ec *EvalConfig, e metricsql.Expr, ctx context.Context) {
	defer close(node.done)
	defer node.cancel()

	// The worker uses its own derived EvalConfig, whose context is canceled
	// only together with the whole batch session (the last demand) or on
	// completion. Individual waiter cancellations do not propagate here.
	ecWorker := copyEvalConfig(ec)
	ecWorker.Context = ctx

	rv, err := evalExprDirect(qt, ecWorker, e)
	if err == nil {
		s.addResultBytes(estimateTimeseriesBytes(rv))
	}
	s.completeNode(node, rv, err)
}

func (s *batchSession) completeNode(node *batchNode, rv []*timeseries, err error) {
	node.stateMu.Lock()
	node.result = rv
	node.err = err
	node.stateMu.Unlock()
}

// consumeNode waits for the shared node result as an independent waiter.
//
// Every waiter obtains its own copy-on-write clone of the result, so
// in-place binary/aggregate optimizations of one consumer cannot pollute
// sibling consumers.
func (s *batchSession) consumeNode(node *batchNode, ec *EvalConfig) ([]*timeseries, error) {
	select {
	case <-ec.Context.Done():
		s.releaseNode(node)
		return nil, searchutil.AnnotateContextError(ec.Context)
	case <-node.done:
	}

	if err := ec.Context.Err(); err != nil {
		// Prefer the waiter's own cancellation error even if the node
		// finished concurrently.
		s.releaseNode(node)
		return nil, searchutil.AnnotateContextError(ec.Context)
	}

	node.stateMu.Lock()
	err := node.err
	rv := node.result
	node.stateMu.Unlock()

	s.releaseNode(node)
	if err != nil {
		return nil, err
	}
	return copyTimeseries(rv), nil
}

// releaseNode deregisters a single consumer and cancels the underlying work
// once the last demand disappears.
func (s *batchSession) releaseNode(node *batchNode) {
	node.stateMu.Lock()
	node.demand--
	demand := node.demand
	cancel := node.cancel
	node.stateMu.Unlock()
	if demand == 0 && cancel != nil {
		cancel()
	}
}

// estimateTimeseriesBytes estimates the retained size of a shared result.
func estimateTimeseriesBytes(tss []*timeseries) int64 {
	var n int64
	for _, ts := range tss {
		n += int64(len(ts.Values)) * 8
		n += int64(len(ts.Timestamps)) * 8
		n += int64(marshaledFastMetricNameSize(&ts.MetricName))
	}
	return n
}

// appendEvalConfigKey appends a typed digest of the effective EvalConfig
// fields that affect evaluation results.
//
// Context, remote address, request URI, query stats and rounding digits are
// intentionally excluded: they either do not affect evalExpr results or are
// attached only for observability.
func appendEvalConfigKey(dst []byte, ec *EvalConfig) []byte {
	dst = append(dst, 'C')
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(ec.Start))
	dst = append(dst, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], uint64(ec.End))
	dst = append(dst, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], uint64(ec.Step))
	dst = append(dst, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], uint64(ec.MaxSeries))
	dst = append(dst, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], uint64(ec.MaxPointsPerSeries))
	dst = append(dst, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], uint64(ec.LookbackDelta))
	dst = append(dst, buf[:]...)
	dst = appendBool(dst, ec.MayCache)
	dst = appendBool(dst, ec.OptimizeRepeatedBinaryOpSubexprs)
	dst = appendTagFilterssKey(dst, ec.EnforcedTagFilterss)
	dst = appendTagFilterssKey(dst, ec.CacheTagFilters)
	return dst
}

func appendBool(dst []byte, b bool) []byte {
	if b {
		return append(dst, 1)
	}
	return append(dst, 0)
}

func appendTagFilterssKey(dst []byte, tfss [][]storage.TagFilter) []byte {
	dst = appendUint32(dst, uint32(len(tfss)))
	for _, tfs := range tfss {
		dst = appendUint32(dst, uint32(len(tfs)))
		for _, f := range tfs {
			dst = f.Marshal(dst)
		}
	}
	return dst
}

func appendStringField(dst []byte, s string) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(s)))
	dst = append(dst, buf[:]...)
	return append(dst, s...)
}

// appendSemanticKey appends a type-tagged, length-prefixed structural
// fingerprint of e to dst.
//
// The encoding is deterministic and includes every semantic field of the
// supported node types. It is used only as an index inside a single bounded
// batch request and is additionally confirmed with reflect.DeepEqual on
// interning, so equivalence is never decided by the encoded string alone.
func appendSemanticKey(dst []byte, e metricsql.Expr) []byte {
	switch t := e.(type) {
	case *metricsql.NumberExpr:
		dst = append(dst, 'N')
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(t.N))
		dst = append(dst, buf[:]...)
	case *metricsql.StringExpr:
		dst = append(dst, 'S')
		dst = appendStringField(dst, t.S)
	case *metricsql.DurationExpr:
		dst = append(dst, 'D')
		dst = appendStringField(dst, string(t.AppendString(nil)))
	case *metricsql.MetricExpr:
		dst = append(dst, 'M')
		dst = appendUint32(dst, uint32(len(t.LabelFilterss)))
		for _, lfs := range t.LabelFilterss {
			dst = appendUint32(dst, uint32(len(lfs)))
			for _, lf := range lfs {
				dst = appendBool(dst, lf.IsNegative)
				dst = appendBool(dst, lf.IsRegexp)
				dst = appendStringField(dst, lf.Label)
				dst = appendStringField(dst, lf.Value)
			}
		}
	case *metricsql.RollupExpr:
		dst = append(dst, 'R')
		dst = appendSemanticKey(dst, t.Expr)
		dst = appendDurationPtr(dst, t.Window)
		dst = appendDurationPtr(dst, t.Offset)
		dst = appendDurationPtr(dst, t.Step)
		dst = appendBool(dst, t.InheritStep)
		if t.At == nil {
			dst = append(dst, 0)
		} else {
			dst = append(dst, 1)
			dst = appendSemanticKey(dst, t.At)
		}
	case *metricsql.FuncExpr:
		dst = append(dst, 'F')
		dst = appendStringField(dst, strings.ToLower(t.Name))
		dst = appendBool(dst, t.KeepMetricNames)
		dst = appendUint32(dst, uint32(len(t.Args)))
		for _, arg := range t.Args {
			dst = appendSemanticKey(dst, arg)
		}
	case *metricsql.AggrFuncExpr:
		dst = append(dst, 'A')
		dst = appendStringField(dst, strings.ToLower(t.Name))
		dst = appendUint32(dst, uint32(t.Limit))
		dst = appendModifierKey(dst, &t.Modifier)
		dst = appendUint32(dst, uint32(len(t.Args)))
		for _, arg := range t.Args {
			dst = appendSemanticKey(dst, arg)
		}
	case *metricsql.BinaryOpExpr:
		dst = append(dst, 'B')
		dst = appendStringField(dst, strings.ToLower(t.Op))
		dst = appendBool(dst, t.Bool)
		dst = appendModifierKey(dst, &t.GroupModifier)
		dst = appendModifierKey(dst, &t.JoinModifier)
		if t.JoinModifierPrefix == nil {
			dst = append(dst, 0)
		} else {
			dst = append(dst, 1)
			dst = appendSemanticKey(dst, t.JoinModifierPrefix)
		}
		dst = appendBool(dst, t.KeepMetricNames)
		dst = appendNumberPtr(dst, t.FillLeft)
		dst = appendNumberPtr(dst, t.FillRight)
		dst = appendSemanticKey(dst, t.Left)
		dst = appendSemanticKey(dst, t.Right)
	case *metricsql.ModifierExpr:
		dst = appendModifierKey(dst, t)
	default:
		// Defensive fallback for unexported or future node types: keep the
		// concrete Go type as a tag plus the canonical representation.
		dst = append(dst, '?')
		dst = appendStringField(dst, reflect.TypeOf(e).String())
		dst = appendStringField(dst, string(e.AppendString(nil)))
	}
	return dst
}

func appendModifierKey(dst []byte, me *metricsql.ModifierExpr) []byte {
	dst = append(dst, 'm')
	dst = appendStringField(dst, me.Op)
	dst = appendUint32(dst, uint32(len(me.Args)))
	for _, arg := range me.Args {
		dst = appendStringField(dst, arg)
	}
	return dst
}

func appendDurationPtr(dst []byte, de *metricsql.DurationExpr) []byte {
	if de == nil {
		return append(dst, 0)
	}
	dst = append(dst, 1)
	return appendStringField(dst, string(de.AppendString(nil)))
}

func appendNumberPtr(dst []byte, ne *metricsql.NumberExpr) []byte {
	if ne == nil {
		return append(dst, 0)
	}
	dst = append(dst, 1)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(ne.N))
	return append(dst, buf[:]...)
}

func appendUint32(dst []byte, n uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], n)
	return append(dst, buf[:]...)
}

// isVolatileFunc reports whether fe produces different results across
// originally independent evaluations even with identical EvalConfig.
func isVolatileFunc(fe *metricsql.FuncExpr) bool {
	switch strings.ToLower(fe.Name) {
	case "now":
		// now() depends on the wall clock.
		return true
	case "rand", "rand_normal", "rand_exponential":
		// Unseeded random functions use the wall clock as a seed.
		// Seeded variants are deterministic and remain shareable.
		return len(fe.Args) == 0
	}
	return false
}

// markVolatileExprs populates m with all nodes of e whose subtree contains
// a volatile function. The root e itself is included when it is volatile.
//
// The returned map uses AST node pointers as keys, so it is valid only for
// the single parsed tree e belongs to.
func markVolatileExprs(e metricsql.Expr, m map[metricsql.Expr]struct{}) bool {
	volatile := false
	switch t := e.(type) {
	case *metricsql.FuncExpr:
		if isVolatileFunc(t) {
			volatile = true
		}
		for _, arg := range t.Args {
			if markVolatileExprs(arg, m) {
				volatile = true
			}
		}
	case *metricsql.AggrFuncExpr:
		for _, arg := range t.Args {
			if markVolatileExprs(arg, m) {
				volatile = true
			}
		}
	case *metricsql.BinaryOpExpr:
		if markVolatileExprs(t.Left, m) {
			volatile = true
		}
		if markVolatileExprs(t.Right, m) {
			volatile = true
		}
		if t.JoinModifierPrefix != nil && markVolatileExprs(t.JoinModifierPrefix, m) {
			volatile = true
		}
	case *metricsql.RollupExpr:
		if markVolatileExprs(t.Expr, m) {
			volatile = true
		}
		if t.At != nil && markVolatileExprs(t.At, m) {
			volatile = true
		}
	}
	if volatile {
		m[e] = struct{}{}
	}
	return volatile
}
