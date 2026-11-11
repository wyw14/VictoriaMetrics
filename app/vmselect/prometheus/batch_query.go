package prometheus

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/stringsutil"
)

// BatchQueryHandler processes /api/v1/batch_query requests.
//
// It evaluates multiple MetricsQL expressions within a single bounded
// request, reusing deterministic shared subgraphs while preserving the input
// order and returning independent per-expression errors.
//
// The request accepts repeated `query` form parameters alongside the same
// time range and option arguments as /api/v1/query_range:
//
//	curl "http://<vmselect>/api/v1/batch_query?start=...&end=...&step=...&query=q1&query=q2"
//
// A single request shares one EvalConfig (time grid, lookback, enforced tag
// filters and execution options), while per-expression offset/@ modifiers
// remain part of the expression identity. Authentication and tenant routing
// are handled by the surrounding HTTP server, so requests of different
// tenants can never be merged into a single batch.
func BatchQueryHandler(qt *querytracer.Tracer, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	defer batchQueryDuration.UpdateDuration(startTime)

	if err := r.ParseForm(); err != nil {
		return httpserver.InvalidParamError(err)
	}
	queries := r.Form["query"]
	if len(queries) == 0 {
		return httpserver.InvalidParamError(fmt.Errorf("missing `query` arg"))
	}
	maxLen := searchutil.GetMaxQueryLen()
	for _, query := range queries {
		if len(query) > maxLen {
			return httpserver.InvalidParamError(fmt.Errorf("too long query; got %d bytes; mustn't exceed `-search.maxQueryLen=%d` bytes", len(query), maxLen))
		}
	}

	ct := startTime.UnixNano() / 1e6
	start, err := httputil.GetTime(r, "start", ct-defaultStep)
	if err != nil {
		return httpserver.InvalidParamError(err)
	}
	end, err := httputil.GetTime(r, "end", ct)
	if err != nil {
		return httpserver.InvalidParamError(err)
	}
	step, err := httputil.GetDuration(r, "step", defaultStep)
	if err != nil {
		return httpserver.InvalidParamError(err)
	}
	etfs, err := searchutil.GetExtraTagFilters(r)
	if err != nil {
		return httpserver.InvalidParamError(err)
	}
	lookbackDelta, err := getMaxLookback(r)
	if err != nil {
		return httpserver.InvalidParamError(err)
	}

	ctx, cancel := searchutil.GetContextForQuery(r, startTime)
	defer cancel()

	mayCache := !httputil.GetBool(r, "nocache")
	optimizeRepeatedBinaryOpSubexprs := httputil.GetBool(r, "optimize_repeated_binary_op_subexprs")
	if start > end {
		end = start + defaultStep
	}
	if err := promql.ValidateMaxPointsPerSeries(start, end, step, *maxPointsPerTimeseries); err != nil {
		return fmt.Errorf("%w; (see -search.maxPointsPerTimeseries command-line flag)", err)
	}
	if mayCache {
		start, end = promql.AdjustStartEnd(start, end, step)
	}

	ec := &promql.EvalConfig{
		Context:                          ctx,
		Start:                            start,
		End:                              end,
		Step:                             step,
		MaxPointsPerSeries:               *maxPointsPerTimeseries,
		MaxSeries:                        0, // let vmstorage use maxUniqueTimeseries by default
		QuotedRemoteAddr:                 httpserver.GetQuotedRemoteAddr(r),
		MayCache:                         mayCache,
		OptimizeRepeatedBinaryOpSubexprs: optimizeRepeatedBinaryOpSubexprs,
		LookbackDelta:                    lookbackDelta,
		RoundDigits:                      getRoundDigits(r),
		EnforcedTagFilterss:              etfs,
		CacheTagFilters:                  etfs,
		GetRequestURI: func() string {
			return httpserver.GetRequestURI(r)
		},
	}

	results := promql.BatchEval(qt, ec, queries, promql.BatchEvalOptions{})

	w.Header().Set("Content-Type", "application/json")
	httpserver.EnableCORS(w, r)
	WriteBatchQueryResponse(w, results)
	qt.Donef("batch_query: expressions=%d, start=%d, end=%d, step=%d", len(queries), start, end, step)
	return nil
}

var batchQueryDuration = metrics.NewSummary(`vm_request_duration_seconds{path="/api/v1/batch_query"}`)

// WriteBatchQueryResponse writes the ordered per-expression batch response.
//
// Every item is either a Prometheus-compatible matrix result or an
// independent error object; the overall HTTP request itself succeeds.
func WriteBatchQueryResponse(w http.ResponseWriter, results []promql.BatchEvalItemResult) {
	var sb strings.Builder
	sb.WriteString(`{"status":"success","data":{"resultType":"batch_matrix","results":[`)
	for i := range results {
		if i > 0 {
			sb.WriteByte(',')
		}
		item := &results[i]
		if item.Err != nil {
			statusCode := http.StatusUnprocessableEntity
			var esc *httpserver.ErrorWithStatusCode
			if errors.As(item.Err, &esc) {
				statusCode = esc.StatusCode
			}
			fmt.Fprintf(&sb, `{"status":"error","errorType":%q,"error":%s}`,
				strconv.Itoa(statusCode), stringsutil.JSONString(item.Err.Error()))
			continue
		}
		sb.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
		for j := range item.Result {
			if j > 0 {
				sb.WriteByte(',')
			}
			rs := &item.Result[j]
			sb.WriteString(`{"metric":`)
			sb.WriteString(metricNameObject(&rs.MetricName))
			sb.WriteString(`,"values":`)
			sb.WriteString(valuesWithTimestamps(rs.Values, rs.Timestamps))
			sb.WriteByte('}')
		}
		sb.WriteString(`]}}`)
	}
	sb.WriteString(`]}}`)
	_, _ = w.Write([]byte(sb.String()))
}
