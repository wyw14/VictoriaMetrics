package promql

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metricsql"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/querystats"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
)

var (
	maxBatchEvalExpressions = flag.Int("search.maxBatchEvalExpressions", 32, "The maximum number of MetricsQL expressions allowed in a single /api/v1/batch_query request. "+
		"Expressions beyond the limit get per-item budget errors and are not evaluated. See also -search.maxBatchEvalDagNodes")
	maxBatchEvalDagNodes = flag.Int("search.maxBatchEvalDagNodes", 10000, "The maximum total number of AST nodes allowed across all expressions of a single BatchEval request. "+
		"The item that exceeds the limit gets a per-item budget error. See also -search.maxBatchEvalExpressions")
	maxBatchEvalResultBytes = flagutil.NewBytes("search.maxBatchEvalResultBytes", 256*1024*1024, "The maximum total size in bytes of shared subgraph results retained by a single BatchEval request. "+
		"Items evaluated after the limit is exceeded get per-item budget errors")
	maxBatchEvalWorkUnits = flag.Int("search.maxBatchEvalWorkUnits", 100000, "The maximum number of shared node evaluations (owner work units) allowed in a single BatchEval request. "+
		"Nodes beyond the limit fail with per-item budget errors")
)

// BatchEvalOptions contains optional per-request limits for BatchEval.
//
// Zero values are replaced with the corresponding -search.maxBatchEval*
// command-line flag defaults.
type BatchEvalOptions struct {
	MaxExpressions int
	MaxDAGNodes    int
	MaxResultBytes int64
	MaxWorkUnits   int64
}

// BatchEvalItemResult is the evaluation result of a single batch expression.
//
// Exactly one of Result and Err is populated. Results always follow the
// input expression order, while errors are independent per expression.
type BatchEvalItemResult struct {
	Result []netstorage.Result
	Err    error
}

type executableBatchItem struct {
	index    int
	query    string
	expr     metricsql.Expr
	volatile map[metricsql.Expr]struct{}
}

// BatchEval evaluates the given queries within a single bounded request,
// reusing deterministic shared subgraphs across the expressions.
//
// All the expressions share the same base EvalConfig (time grid, lookback,
// enforced tag filters and execution options); per-expression differences
// such as offset and @ modifiers are expressed in the AST itself and remain
// part of the shared node identity. The returned slice has exactly the same
// length and order as queries: every expression gets either its result or
// its own independent error.
//
// The call does not return a request-level error, except for invalid
// EvalConfig values, in which case every item receives the same error.
func BatchEval(qt *querytracer.Tracer, ec *EvalConfig, queries []string, opts BatchEvalOptions) []BatchEvalItemResult {
	ec.validate()

	limits := batchLimits{
		maxExpressions: opts.MaxExpressions,
		maxDagNodes:    opts.MaxDAGNodes,
		maxResultBytes: opts.MaxResultBytes,
		maxWorkUnits:   opts.MaxWorkUnits,
	}
	if limits.maxExpressions == 0 {
		limits.maxExpressions = *maxBatchEvalExpressions
	}
	if limits.maxDagNodes == 0 {
		limits.maxDagNodes = *maxBatchEvalDagNodes
	}
	if limits.maxResultBytes == 0 {
		limits.maxResultBytes = int64(maxBatchEvalResultBytes.N)
	}
	if limits.maxWorkUnits == 0 {
		limits.maxWorkUnits = int64(*maxBatchEvalWorkUnits)
	}

	results := make([]BatchEvalItemResult, len(queries))
	session := newBatchSession(ec.Context, limits)

	// Phase 1: sequential, deterministic planning. Parse errors and static
	// budget violations are attached to the corresponding items without
	// starting any evaluation.
	items, plannedNodeCount := planBatchItems(queries, results, limits)

	// Phase 2: build the set of structurally shared nodes from all planned
	// expressions. Volatile subtrees are excluded.
	structuralCounts := make(map[string]int)
	for _, item := range items {
		collectStructuralShares(item, structuralCounts)
	}
	for key, count := range structuralCounts {
		if count >= 2 {
			session.sharedStructural[key] = struct{}{}
		}
	}
	session.dagNodes.Store(int64(plannedNodeCount))

	// Phase 3: parallel evaluation with per-item contexts, query stats and
	// active query registration.
	ecRoot := copyEvalConfig(ec)
	ecRoot.batch = session

	var wg sync.WaitGroup
	for i := range items {
		item := items[i]
		idx := item.index
		wg.Go(func() {
			results[idx] = evalBatchItem(qt, ecRoot, item)
		})
	}
	wg.Wait()

	return results
}

func planBatchItems(queries []string,
	results []BatchEvalItemResult, limits batchLimits,
) ([]executableBatchItem, int) {
	items := make([]executableBatchItem, 0, len(queries))
	var totalNodeCount int
	for i, query := range queries {
		if i >= limits.maxExpressions {
			results[i].Err = &BatchBudgetError{
				msg: fmt.Sprintf("BatchEval expression budget exceeded: expression #%d is beyond the limit of %d expressions; "+
					"increase -search.maxBatchEvalExpressions or split the request into smaller batches", i+1, limits.maxExpressions),
			}
			continue
		}
		e, err := parsePromQLWithCache(query)
		if err != nil {
			results[i].Err = httpserver.InvalidParamError(err)
			continue
		}
		nodeCount := 0
		metricsql.VisitAll(e, func(_ metricsql.Expr) {
			nodeCount++
		})
		if totalNodeCount+nodeCount > limits.maxDagNodes {
			results[i].Err = &BatchBudgetError{
				msg: fmt.Sprintf("BatchEval DAG node budget exceeded: expression #%d would raise the total to %d nodes beyond the limit of %d; "+
					"increase -search.maxBatchEvalDagNodes or split the request into smaller batches", i+1, totalNodeCount+nodeCount, limits.maxDagNodes),
			}
			continue
		}
		totalNodeCount += nodeCount
		volatile := make(map[metricsql.Expr]struct{})
		markVolatileExprs(e, volatile)
		items = append(items, executableBatchItem{
			index:    i,
			query:    query,
			expr:     e,
			volatile: volatile,
		})
	}
	return items, totalNodeCount
}

func collectStructuralShares(item executableBatchItem, counts map[string]int) {
	metricsql.VisitAll(item.expr, func(e metricsql.Expr) {
		if _, ok := item.volatile[e]; ok {
			// Volatile subtrees must remain independent per expression.
			return
		}
		counts[planStructuralKey(e)]++
	})
}

func evalBatchItem(qt *querytracer.Tracer, ecRoot *EvalConfig, item executableBatchItem) BatchEvalItemResult {
	ctx, cancel := context.WithCancel(ecRoot.Context)
	defer cancel()

	ec := copyEvalConfig(ecRoot)
	ec.Context = ctx
	qs := NewQueryStats(item.query, nil, ec)
	ec.QueryStats = qs

	if querystats.Enabled() {
		startTime := time.Now()
		defer func() {
			querystats.RegisterQuery(item.query, ec.End-ec.Start, startTime, ec.QueryStats.memoryUsage())
			ec.QueryStats.addExecutionTimeMsec(startTime)
		}()
	}

	qid := activeQueriesV.Add(ec, item.query)
	rv, err := evalExpr(qt, ec, item.expr)
	activeQueriesV.Remove(qid)
	if err != nil {
		return BatchEvalItemResult{Err: err}
	}
	result, err := finalizeEvalResult(qt, ec, item.expr, rv, false)
	if err != nil {
		return BatchEvalItemResult{Err: err}
	}
	return BatchEvalItemResult{Result: result}
}
