package prometheus

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

func TestBatchQueryHandlerMissingQuery(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/batch_query?start=1000&end=2000&step=100", nil)
	rec := httptest.NewRecorder()
	qt := querytracer.New(false, "test")
	err := BatchQueryHandler(qt, time.Now(), rec, req)
	if err == nil {
		t.Fatalf("expected error for missing query arg")
	}
	var esc *httpserver.ErrorWithStatusCode
	if !errors.As(err, &esc) {
		t.Fatalf("expected ErrorWithStatusCode; got %T: %s", err, err)
	}
	if esc.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 status; got %d", esc.StatusCode)
	}
}

func TestBatchQueryHandlerTooLongQuery(t *testing.T) {
	// The default -search.maxQueryLen is 16KiB.
	longQuery := strings.Repeat("a", 17*1024)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/batch_query?start=1000&end=2000&step=100&query="+longQuery, nil)
	rec := httptest.NewRecorder()
	qt := querytracer.New(false, "test")
	err := BatchQueryHandler(qt, time.Now(), rec, req)
	if err == nil {
		t.Fatalf("expected error for too long query")
	}
	var esc *httpserver.ErrorWithStatusCode
	if !errors.As(err, &esc) || esc.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 ErrorWithStatusCode; got %v", err)
	}
}

func TestWriteBatchQueryResponse(t *testing.T) {
	results := []promql.BatchEvalItemResult{
		{
			Result: []netstorage.Result{
				{
					MetricName: storage.MetricName{
						MetricGroup: []byte("m"),
						Tags: []storage.Tag{
							{Key: []byte("job"), Value: []byte("api")},
						},
					},
					Values:     []float64{1.5, 2.5},
					Timestamps: []int64{1000, 2000},
				},
			},
		},
		{
			Err: errors.New("independent item failure"),
		},
	}

	rec := httptest.NewRecorder()
	WriteBatchQueryResponse(rec, results)
	body := rec.Body.String()

	// The overall request succeeds and per-item results keep input order.
	if !strings.HasPrefix(body, `{"status":"success","data":{"resultType":"batch_matrix","results":[`) {
		t.Fatalf("unexpected response prefix: %s", body)
	}
	if !strings.Contains(body, `"status":"success"`) {
		t.Fatalf("missing success item: %s", body)
	}
	if !strings.Contains(body, `"__name__":"m"`) {
		t.Fatalf("missing metric name object: %s", body)
	}
	if !strings.Contains(body, `"job":"api"`) {
		t.Fatalf("missing metric tags: %s", body)
	}
	if !strings.Contains(body, `"status":"error"`) || !strings.Contains(body, "independent item failure") {
		t.Fatalf("missing independent error item: %s", body)
	}
	if !strings.HasSuffix(body, `]}}`) {
		t.Fatalf("unexpected response suffix: %s", body)
	}
}
