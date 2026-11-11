package promscrape

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/chunkedbuffer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutil"
)

func testAdmissionConfig() admissionConfig {
	return admissionConfig{
		globalBytes:          4096,
		jobBytes:             4096,
		maxResponseBytes:     4096,
		quantum:              1024,
		waitTimeout:          time.Second,
		maxQueuedPerJob:      8,
		maxConcurrentProcess: 2,
		maxJobLabels:         4,
	}
}

func newTestAdmission(t *testing.T, mutate func(cfg *admissionConfig), enableMetrics bool) *scrapeProcessingAdmission {
	t.Helper()
	cfg := testAdmissionConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	adm := newScrapeProcessingAdmission(cfg, enableMetrics)
	t.Cleanup(adm.Stop)
	return adm
}

// withTestAdmission installs adm as the package-wide scrape admission singleton for the
// test and restores the previous value on cleanup.
func withTestAdmission(t *testing.T, adm *scrapeProcessingAdmission) {
	t.Helper()
	old := scrapeAdmission.Swap(adm)
	t.Cleanup(func() {
		scrapeAdmission.Store(old)
	})
}

func (a *scrapeProcessingAdmission) testInflightBytes() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inFlightBytes
}

func (a *scrapeProcessingAdmission) testPeakBytes() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.peakInFlightBytes
}

func (a *scrapeProcessingAdmission) testQueued() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.queuedRequests
}

func (a *scrapeProcessingAdmission) testJobInflight(job string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if js := a.jobs[job]; js != nil {
		return js.inflightBytes
	}
	return 0
}

func (a *scrapeProcessingAdmission) testProcessingInFlight() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.processingInFlight
}

func TestAdmissionConfigNormalize(t *testing.T) {
	cfg := admissionConfig{
		jobBytes:     2048,
		globalBytes:  1024,
		quantum:      0,
		waitTimeout:  -1,
		maxJobLabels: 0,
	}
	cfg.normalize()
	if cfg.jobBytes != cfg.globalBytes {
		t.Fatalf("job budget must be capped by global budget; got job=%d global=%d", cfg.jobBytes, cfg.globalBytes)
	}
	if cfg.quantum != cfg.jobBytes {
		t.Fatalf("invalid quantum must fall back to job budget; got %d", cfg.quantum)
	}
	if cfg.waitTimeout <= 0 || cfg.maxJobLabels <= 0 || cfg.maxConcurrentProcess <= 0 {
		t.Fatalf("non-positive values must get defaults; got %+v", cfg)
	}
	if cfg.maxResponseBytes <= 0 {
		t.Fatalf("maxResponseBytes must get a default")
	}
}

func TestAdmissionAcquireReleaseAccounting(t *testing.T) {
	adm := newTestAdmission(t, nil, false)

	t1, err := adm.Acquire(nil, "job-a", 1024)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if got := adm.testInflightBytes(); got != 1024 {
		t.Fatalf("unexpected in-flight bytes; got %d; want 1024", got)
	}

	t2, err := adm.Acquire(nil, "job-a", 2048)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if got := adm.testInflightBytes(); got != 3072 {
		t.Fatalf("unexpected in-flight bytes; got %d; want 3072", got)
	}
	if got := adm.testJobInflight("job-a"); got != 3072 {
		t.Fatalf("unexpected job in-flight bytes; got %d; want 3072", got)
	}

	// A third ticket does not fit and must wait instead of overflowing the budget.
	blocked := make(chan *admissionTicket, 1)
	blockedErr := make(chan error, 1)
	go func() {
		tk, err := adm.Acquire(nil, "job-a", 2048)
		if err != nil {
			blockedErr <- err
			return
		}
		blocked <- tk
	}()
	select {
	case <-blocked:
		t.Fatal("acquire must block while budget is full")
	case err := <-blockedErr:
		t.Fatalf("unexpected error while waiting: %s", err)
	case <-time.After(50 * time.Millisecond):
	}
	if got := adm.testPeakBytes(); got > 4096 {
		t.Fatalf("peak in-flight bytes exceeded the global budget; got %d", got)
	}

	t1.Release()
	t2.Release()

	var t3 *admissionTicket
	select {
	case t3 = <-blocked:
	case err := <-blockedErr:
		t.Fatalf("blocked ticket must be admitted after releases; got %s", err)
	case <-time.After(2 * time.Second):
		t.Fatalf("blocked ticket was never admitted")
	}
	if got := adm.testInflightBytes(); got != 2048 {
		t.Fatalf("unexpected in-flight bytes after release; got %d; want 2048", got)
	}
	t3.Release()
	if got := adm.testInflightBytes(); got != 0 {
		t.Fatalf("in-flight bytes must return to zero; got %d", got)
	}
}

func TestAdmissionImmediateRejects(t *testing.T) {
	adm := newTestAdmission(t, func(cfg *admissionConfig) {
		cfg.maxQueuedPerJob = 1
		cfg.waitTimeout = 10 * time.Second
	}, false)

	// A response bigger than every configured budget is rejected before queueing.
	_, err := adm.Acquire(nil, "job-oversize", 8192)
	if !errors.Is(err, errAdmissionSkipped) || admissionSkipReason(err) != admissionReasonResponseTooLarge {
		t.Fatalf("unexpected error: %v", err)
	}

	// Fill the only queue slot.
	holder, err := adm.Acquire(nil, "job-q", 4096)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	waiter := make(chan struct{})
	go func() {
		tk, err := adm.Acquire(nil, "job-q", 1024)
		if err == nil {
			tk.Release()
		}
		close(waiter)
	}()
	time.Sleep(50 * time.Millisecond)

	// The next request must hit the per-job queue bound immediately.
	_, err = adm.Acquire(nil, "job-q", 1024)
	if !errors.Is(err, errAdmissionSkipped) || admissionSkipReason(err) != admissionReasonQueueFull {
		t.Fatalf("unexpected error: %v", err)
	}
	holder.Release()
	select {
	case <-waiter:
	case <-time.After(2 * time.Second):
		t.Fatal("queued request did not finish after holder release")
	}
	if got := adm.testInflightBytes(); got != 0 {
		t.Fatalf("in-flight bytes must return to zero; got %d", got)
	}
}

func TestAdmissionWaitTimeout(t *testing.T) {
	adm := newTestAdmission(t, func(cfg *admissionConfig) {
		cfg.waitTimeout = 100 * time.Millisecond
	}, false)

	holder, err := adm.Acquire(nil, "job-t", 4096)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	start := time.Now()
	_, err = adm.Acquire(nil, "job-t", 1024)
	waited := time.Since(start)
	if !errors.Is(err, errAdmissionSkipped) || admissionSkipReason(err) != admissionReasonWaitTimeout {
		t.Fatalf("unexpected error: %v", err)
	}
	if waited < 100*time.Millisecond {
		t.Fatalf("wait returned before the deadline; waited %s", waited)
	}
	if got := adm.testQueued(); got != 0 {
		t.Fatalf("timed out ticket must leave the queue; queued=%d", got)
	}
	holder.Release()
	if got := adm.testInflightBytes(); got != 0 {
		t.Fatalf("in-flight bytes must return to zero; got %d", got)
	}
}

func TestAdmissionCancelWhileQueued(t *testing.T) {
	adm := newTestAdmission(t, func(cfg *admissionConfig) {
		cfg.waitTimeout = time.Minute
	}, false)

	holder, err := adm.Acquire(nil, "job-c", 4096)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	cancelCh := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := adm.Acquire(cancelCh, "job-c", 1024)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	close(cancelCh)
	select {
	case err := <-done:
		if !errors.Is(err, errAdmissionSkipped) || admissionSkipReason(err) != admissionReasonCanceled {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled ticket was never unblocked")
	}
	if got := adm.testQueued(); got != 0 {
		t.Fatalf("canceled ticket must leave the queue; queued=%d", got)
	}
	holder.Release()
}

// TestAdmissionDRRFairness proves that jobs queued on a saturated budget get admitted
// with a bounded wait once budget frees, and that two jobs under continuous load receive
// balanced long-term throughput instead of one job starving the other.
func TestAdmissionDRRFairness(t *testing.T) {
	adm := newTestAdmission(t, func(cfg *admissionConfig) {
		cfg.globalBytes = 1000
		cfg.jobBytes = 1000
		cfg.maxResponseBytes = 1000
		cfg.quantum = 500
		cfg.maxQueuedPerJob = 32
		cfg.waitTimeout = 5 * time.Second
	}, false)

	// Deterministic case: a holder saturates the whole budget while two jobs queue
	// equal-cost requests. Both must be admitted within a bounded wait after release.
	holder, err := adm.Acquire(nil, "holder-job", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	type acquireResult struct {
		tk  *admissionTicket
		err error
	}
	ra := make(chan acquireResult, 1)
	rb := make(chan acquireResult, 1)
	go func() {
		tk, err := adm.Acquire(nil, "fair-a", 500)
		ra <- acquireResult{tk: tk, err: err}
	}()
	go func() {
		tk, err := adm.Acquire(nil, "fair-b", 500)
		rb <- acquireResult{tk: tk, err: err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && adm.testQueued() != 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := adm.testQueued(); got != 2 {
		t.Fatalf("both requests must be queued; got queued=%d", got)
	}

	releaseAt := time.Now()
	holder.Release()
	var tickets []*admissionTicket
	for _, ch := range []chan acquireResult{ra, rb} {
		select {
		case res := <-ch:
			if res.err != nil {
				t.Fatalf("queued request failed: %s", res.err)
			}
			tickets = append(tickets, res.tk)
		case <-time.After(2 * time.Second):
			t.Fatal("queued request starved after budget became available")
		}
	}
	if wait := time.Since(releaseAt); wait > time.Second {
		t.Fatalf("fairness wait upper bound exceeded: %s", wait)
	}
	for _, tk := range tickets {
		tk.Release()
	}
	if got := adm.testPeakBytes(); got > 1000 {
		t.Fatalf("peak in-flight bytes exceeded the global budget; got %d", got)
	}

	// Throughput case: two jobs continuously contend for the same budget. Their
	// admission counts over a fixed window must stay balanced and every wait bounded.
	stop := make(chan struct{})
	var countA, countB atomic.Int64
	var maxWaitNanos atomic.Int64
	var wg sync.WaitGroup
	load := func(job string, counter *atomic.Int64) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			start := time.Now()
			tk, err := adm.Acquire(stop, job, 500)
			if err != nil {
				return
			}
			if d := time.Since(start); d > 0 {
				for {
					old := maxWaitNanos.Load()
					if d.Nanoseconds() <= old || maxWaitNanos.CompareAndSwap(old, d.Nanoseconds()) {
						break
					}
				}
			}
			counter.Add(1)
			time.Sleep(2 * time.Millisecond)
			tk.Release()
		}
	}
	wg.Add(2)
	go load("load-a", &countA)
	go load("load-b", &countB)
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	a, b := countA.Load(), countB.Load()
	total := a + b
	if total < 20 {
		t.Fatalf("too few admissions under load: %d", total)
	}
	share := float64(a) / float64(total)
	if share < 0.3 || share > 0.7 {
		t.Fatalf("unbalanced admission share for job-a: %.2f (a=%d b=%d)", share, a, b)
	}
	if maxWait := time.Duration(maxWaitNanos.Load()); maxWait > 2*time.Second {
		t.Fatalf("admission wait upper bound exceeded under load: %s", maxWait)
	}
	if got := adm.testInflightBytes(); got != 0 {
		t.Fatalf("in-flight bytes must return to zero; got %d", got)
	}
}

func TestAdmissionSettleAndReserveGuards(t *testing.T) {
	adm := newTestAdmission(t, nil, false)

	tk, err := adm.Acquire(nil, "job-s", 2048)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Over-estimation returns the difference.
	if err := tk.Settle(1024); err != nil {
		t.Fatalf("settle to smaller size failed: %s", err)
	}
	if got := adm.testInflightBytes(); got != 1024 {
		t.Fatalf("unexpected in-flight after settle; got %d; want 1024", got)
	}
	// Under-estimation reserves extra while budget allows.
	if err := tk.Settle(3000); err != nil {
		t.Fatalf("settle to bigger size failed: %s", err)
	}
	if got := adm.testInflightBytes(); got != 3000 {
		t.Fatalf("unexpected in-flight after grow; got %d; want 3000", got)
	}
	// Growth beyond the remaining global budget is rejected without mutation.
	if err := tk.Settle(8192); err == nil {
		t.Fatal("expecting memory budget error")
	} else if admissionSkipReason(err) != admissionReasonMemoryBudget {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := adm.testInflightBytes(); got != 3000 {
		t.Fatalf("failed settle must not mutate accounting; got %d", got)
	}
	// Repeated Release is a no-op: bytes never go negative or get returned twice.
	tk.Release()
	tk.Release()
	if got := adm.testInflightBytes(); got != 0 {
		t.Fatalf("in-flight bytes must be zero after release; got %d", got)
	}
	if got := adm.testJobInflight("job-s"); got != 0 {
		t.Fatalf("job in-flight bytes must be zero; got %d", got)
	}
}

func TestAdmissionJobRemovalCleanup(t *testing.T) {
	adm := newTestAdmission(t, func(cfg *admissionConfig) {
		cfg.maxJobLabels = 2
	}, true)
	withTestAdmission(t, adm)

	adm.JobStarted("job-1")
	adm.JobStarted("job-2")
	t1, err := adm.Acquire(nil, "job-1", 1024)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	t2, err := adm.Acquire(nil, "job-2", 1024)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// The third job exceeds the label cap and must not get per-job metrics.
	t3, err := adm.Acquire(nil, "job-3", 1024)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	adm.obs.mu.Lock()
	if len(adm.obs.m) != 2 {
		adm.obs.mu.Unlock()
		t.Fatalf("job label cap not enforced; got %d labelled jobs", len(adm.obs.m))
	}
	jm3 := adm.jobs["job-3"].jm
	adm.obs.mu.Unlock()
	if jm3 != nil {
		t.Fatal("job above the label cap must not receive per-job metrics")
	}

	t1.Release()
	t2.Release()
	t3.Release()
	adm.JobStopped("job-1")
	adm.JobStopped("job-2")

	// Idle jobs are removed together with their labelled metrics.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		adm.mu.Lock()
		left := len(adm.jobs)
		adm.mu.Unlock()
		if left == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	adm.obs.mu.Lock()
	labelled := len(adm.obs.m)
	adm.obs.mu.Unlock()
	if labelled != 0 {
		t.Fatalf("per-job metrics must be unregistered on job removal; left %d", labelled)
	}
}

func TestAdmissionStopRejectsWaitersAndIdempotent(t *testing.T) {
	adm := newTestAdmission(t, func(cfg *admissionConfig) {
		cfg.waitTimeout = time.Minute
	}, false)

	// Saturate the whole budget so the waiter is genuinely budget-blocked.
	holder, err := adm.Acquire(nil, "job-stop", 4096)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := adm.Acquire(nil, "job-stop", 1024)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if got := adm.testQueued(); got != 1 {
		t.Fatalf("waiter must be queued before shutdown; got queued=%d", got)
	}

	adm.Stop()
	adm.Stop() // must be idempotent

	select {
	case err := <-done:
		if !errors.Is(err, errAdmissionSkipped) || admissionSkipReason(err) != admissionReasonShutdown {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiting ticket was not unblocked by shutdown")
	}

	// In-flight tickets stay valid after Stop and still release exactly once.
	holder.Release()
	holder.Release()
	if got := adm.testInflightBytes(); got != 0 {
		t.Fatalf("in-flight bytes must settle to zero after shutdown; got %d", got)
	}

	// New requests after shutdown fail immediately.
	if _, err := adm.Acquire(nil, "job-stop", 1024); !errors.Is(err, errAdmissionSkipped) {
		t.Fatalf("unexpected error after shutdown: %v", err)
	}
}

// TestAdmissionConcurrentMixedLoad mixes fast and slow, big and small responses across
// several jobs and asserts the hard byte budget, exact-once release and zero leftovers.
func TestAdmissionConcurrentMixedLoad(t *testing.T) {
	adm := newTestAdmission(t, func(cfg *admissionConfig) {
		cfg.globalBytes = 64 * 1024
		cfg.jobBytes = 32 * 1024
		cfg.maxResponseBytes = 12 * 1024
		cfg.quantum = 8 * 1024
		cfg.maxQueuedPerJob = 64
		cfg.maxConcurrentProcess = 4
		cfg.waitTimeout = 5 * time.Second
	}, false)

	const goroutinesPerJob = 25
	jobs := []string{"mixed-a", "mixed-b", "mixed-c"}
	var wg sync.WaitGroup
	var skipped atomic.Int64
	var admitted atomic.Int64
	for _, job := range jobs {
		for range goroutinesPerJob {
			wg.Add(1)
			go func(job string) {
				defer wg.Done()
				cost := int64(1024 + rand.Intn(11*1024))
				tk, err := adm.Acquire(nil, job, cost)
				if err != nil {
					skipped.Add(1)
					return
				}
				admitted.Add(1)
				// Emulate variable processing time: fast and slow responses.
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
				// Double release must stay safe under concurrency.
				tk.Release()
				tk.Release()
			}(job)
		}
	}
	wg.Wait()

	if got := adm.testInflightBytes(); got != 0 {
		t.Fatalf("in-flight bytes must be zero after all work; got %d", got)
	}
	if got := adm.testPeakBytes(); got > 64*1024 {
		t.Fatalf("peak in-flight bytes %d exceeded the global budget", got)
	}
	if got := adm.testQueued(); got != 0 {
		t.Fatalf("queue must be empty; got %d", got)
	}
	if got := adm.testProcessingInFlight(); got != 0 {
		t.Fatalf("processing slots must all be returned; got %d", got)
	}
	if len(adm.processingSlots) != 0 {
		t.Fatalf("processing semaphore must be empty; got %d tokens", len(adm.processingSlots))
	}
	if admitted.Load() == 0 {
		t.Fatal("no requests were admitted")
	}
	t.Logf("admitted=%d skipped=%d peak=%d", admitted.Load(), skipped.Load(), adm.testPeakBytes())
}

func TestAdmissionNoGoroutineLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()
	for range 5 {
		adm := newTestAdmission(t, func(cfg *admissionConfig) {
			cfg.waitTimeout = 50 * time.Millisecond
			cfg.maxQueuedPerJob = 16
		}, false)
		var wg sync.WaitGroup
		for i := range 20 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				job := fmt.Sprintf("leak-job-%d", i%4)
				tk, err := adm.Acquire(nil, job, int64(512+(i%3)*512))
				if err != nil {
					return
				}
				time.Sleep(time.Duration(i%5) * time.Millisecond)
				tk.Release()
			}(i)
		}
		wg.Wait()
		adm.Stop()
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutine leak detected: baseline=%d current=%d", baseline, runtime.NumGoroutine())
}

func newAdmissionTestScrapeWork(t *testing.T, job string) *scrapeWork {
	t.Helper()
	sw := &scrapeWork{}
	sw.Config = &ScrapeWork{
		ScrapeTimeout: time.Second * 42,
		MaxScrapeSize: 512 * 1024,
		Labels: promutil.NewLabelsFromMap(map[string]string{
			"job": job,
		}),
	}
	sw.PushData = func(_ *auth.Token, _ *prompb.WriteRequest) {}
	tsmGlobal.Register(sw)
	t.Cleanup(func() {
		tsmGlobal.Unregister(sw)
	})
	return sw
}

// TestScrapeInternalAdmissionSkipBeforeNetwork verifies that backpressure never performs
// a network request, never pushes data or stale markers and leaves lastScrape state for
// the next normally scheduled scrape untouched.
func TestScrapeInternalAdmissionSkipBeforeNetwork(t *testing.T) {
	t.Run("immediate_response_too_large", func(t *testing.T) {
		adm := newTestAdmission(t, func(cfg *admissionConfig) {
			// Below the 64KiB conservative estimate floor, so every scrape is rejected.
			cfg.globalBytes = 32 * 1024
			cfg.jobBytes = 32 * 1024
			cfg.maxResponseBytes = 32 * 1024
		}, false)
		withTestAdmission(t, adm)

		sw := newAdmissionTestScrapeWork(t, "skip-job")
		var readCalls, pushCalls atomic.Int64
		sw.ReadData = func(_ *chunkedbuffer.Buffer) (bool, error) {
			readCalls.Add(1)
			return false, nil
		}
		sw.PushData = func(_ *auth.Token, _ *prompb.WriteRequest) {
			pushCalls.Add(1)
		}

		err := sw.scrapeInternal(123000, 123000)
		if !errors.Is(err, errAdmissionSkipped) {
			t.Fatalf("unexpected error: %v", err)
		}
		if readCalls.Load() != 0 || pushCalls.Load() != 0 {
			t.Fatalf("skipped scrape must not touch network or push data; reads=%d pushes=%d", readCalls.Load(), pushCalls.Load())
		}
		if sw.lastScrapeLen != 0 || sw.lastScrapeSuccess {
			t.Fatal("lastScrape state must not be mutated by skipped scrapes")
		}
		if got := adm.testInflightBytes(); got != 0 {
			t.Fatalf("in-flight bytes must be zero after skip; got %d", got)
		}
	})

	t.Run("wait_timeout_while_budget_saturated", func(t *testing.T) {
		adm := newTestAdmission(t, func(cfg *admissionConfig) {
			cfg.globalBytes = admissionMinResponseEstimate
			cfg.jobBytes = admissionMinResponseEstimate
			cfg.maxResponseBytes = admissionMinResponseEstimate
			cfg.waitTimeout = 150 * time.Millisecond
			cfg.maxConcurrentProcess = 4
		}, false)
		withTestAdmission(t, adm)

		holder, err := adm.Acquire(nil, "saturated-job", admissionMinResponseEstimate)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		defer holder.Release()

		sw := newAdmissionTestScrapeWork(t, "saturated-job")
		var readCalls atomic.Int64
		sw.ReadData = func(_ *chunkedbuffer.Buffer) (bool, error) {
			readCalls.Add(1)
			return false, nil
		}
		err = sw.scrapeInternal(123000, 123000)
		if !errors.Is(err, errAdmissionSkipped) || admissionSkipReason(err) != admissionReasonWaitTimeout {
			t.Fatalf("unexpected error: %v", err)
		}
		if readCalls.Load() != 0 {
			t.Fatalf("timed-out scrape must not perform network requests; reads=%d", readCalls.Load())
		}
	})

	t.Run("next_scrape_keeps_timestamp_and_lastScrape_semantics", func(t *testing.T) {
		// Small budget first, then a healthy admission: the skipped scrape must not affect
		// timestamps (chosen by the caller) nor lastScrape bookkeeping.
		small := newTestAdmission(t, func(cfg *admissionConfig) {
			cfg.globalBytes = 32 * 1024
			cfg.jobBytes = 32 * 1024
			cfg.maxResponseBytes = 32 * 1024
		}, false)
		withTestAdmission(t, small)
		sw := newAdmissionTestScrapeWork(t, "recover-job")
		sw.ReadData = func(dst *chunkedbuffer.Buffer) (bool, error) {
			dst.MustWrite([]byte("foo 1\nbar 2\n"))
			return false, nil
		}
		var pushCalls atomic.Int64
		sw.PushData = func(_ *auth.Token, _ *prompb.WriteRequest) {
			pushCalls.Add(1)
		}
		if err := sw.scrapeInternal(123000, 123000); !errors.Is(err, errAdmissionSkipped) {
			t.Fatalf("unexpected error: %v", err)
		}

		healthy := newScrapeProcessingAdmission(admissionConfig{
			globalBytes:          4 * 1024 * 1024,
			jobBytes:             1024 * 1024,
			maxResponseBytes:     1024 * 1024,
			quantum:              64 * 1024,
			waitTimeout:          time.Second,
			maxQueuedPerJob:      8,
			maxConcurrentProcess: 4,
			maxJobLabels:         4,
		}, false)
		t.Cleanup(healthy.Stop)
		withTestAdmission(t, healthy)

		const ts = int64(222000)
		if err := sw.scrapeInternal(ts, ts); err != nil {
			t.Fatalf("successful scrape after backpressure failed: %s", err)
		}
		if pushCalls.Load() == 0 {
			t.Fatal("successful scrape must push data")
		}
		if sw.lastScrapeLen == 0 || !sw.lastScrapeSuccess {
			t.Fatal("successful scrape must update lastScrape state")
		}
		lastScrapeLen := sw.lastScrapeLen

		// Re-introduce backpressure and confirm the stored lastScrape survives.
		withTestAdmission(t, small)
		err := sw.scrapeInternal(ts+10000, ts+10000)
		if !errors.Is(err, errAdmissionSkipped) {
			t.Fatalf("unexpected error: %v", err)
		}
		if sw.lastScrapeLen != lastScrapeLen || !sw.lastScrapeSuccess {
			t.Fatal("skipped scrape must not reset lastScrape state")
		}
	})
}

// TestScrapeInternalAdmissionGzipBomb verifies that a tiny gzip stream expanding beyond
// the single-response hard cap fails promptly on actual decompressed bytes, releases all
// reservations and pushes nothing.
func TestScrapeInternalAdmissionGzipBomb(t *testing.T) {
	adm := newTestAdmission(t, func(cfg *admissionConfig) {
		cfg.globalBytes = 1 * 1024 * 1024
		cfg.jobBytes = 512 * 1024
		cfg.maxResponseBytes = 128 * 1024
		cfg.quantum = 64 * 1024
		cfg.maxConcurrentProcess = 4
	}, false)
	withTestAdmission(t, adm)

	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	if _, err := gz.Write(bytes.Repeat([]byte("0"), 300*1024)); err != nil {
		t.Fatalf("cannot write gzip bomb: %s", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("cannot close gzip: %s", err)
	}
	if compressed.Len() >= 128*1024 {
		t.Fatalf("test precondition failed: bomb must be small when compressed; got %d bytes", compressed.Len())
	}

	sw := newAdmissionTestScrapeWork(t, "bomb-job")
	sw.ReadData = func(dst *chunkedbuffer.Buffer) (bool, error) {
		_, _ = dst.Write(compressed.Bytes())
		return true, nil
	}
	var pushCalls atomic.Int64
	var nonAutoSeries atomic.Int64
	sw.PushData = func(_ *auth.Token, wr *prompb.WriteRequest) {
		pushCalls.Add(1)
		for _, ts := range wr.Timeseries {
			name := ""
			for _, label := range ts.Labels {
				if label.Name == "__name__" {
					name = label.Value
					break
				}
			}
			if !isAutoMetric(name) {
				nonAutoSeries.Add(1)
			}
		}
	}

	err := sw.scrapeInternal(123000, 123000)
	if err == nil {
		t.Fatal("expecting gzip expansion error")
	}
	if !contains(err.Error(), "admission.maxResponseBytes") {
		t.Fatalf("unexpected error: %s", err)
	}
	// A downloaded response that violates the hard cap is a real target failure:
	// only up=0 style auto metrics are pushed, never the unprocessed series, and the
	// last successful scrape state is untouched.
	if pushCalls.Load() == 0 {
		t.Fatal("target failure auto metrics must still be pushed")
	}
	if nonAutoSeries.Load() != 0 {
		t.Fatalf("unprocessed bomb series must never be pushed; got %d", nonAutoSeries.Load())
	}
	if sw.lastScrapeLen != 0 || sw.lastScrapeSuccess {
		t.Fatal("failed decompression must not update lastScrape state")
	}
	if got := adm.testInflightBytes(); got != 0 {
		t.Fatalf("all reservations must be released; got %d in-flight bytes", got)
	}
	if got := adm.testProcessingInFlight(); got != 0 {
		t.Fatalf("processing slot must be released; got %d", got)
	}
}

// TestScrapeInternalAdmissionHappyPath verifies normal accounting across repeated
// scrapes, including waiting downloaded responses and settlement to actual sizes.
func TestScrapeInternalAdmissionHappyPath(t *testing.T) {
	adm := newTestAdmission(t, func(cfg *admissionConfig) {
		cfg.globalBytes = 4 * 1024 * 1024
		cfg.jobBytes = 1024 * 1024
		cfg.maxResponseBytes = 1024 * 1024
		cfg.quantum = 64 * 1024
		cfg.maxConcurrentProcess = 2
	}, false)
	withTestAdmission(t, adm)

	sw := newAdmissionTestScrapeWork(t, "happy-job")
	sw.ReadData = func(dst *chunkedbuffer.Buffer) (bool, error) {
		dst.MustWrite([]byte("foo 1\nbar 2\nbaz 3\n"))
		return false, nil
	}
	var pushCalls atomic.Int64
	sw.PushData = func(_ *auth.Token, _ *prompb.WriteRequest) {
		pushCalls.Add(1)
	}

	for i := range 3 {
		if err := sw.scrapeInternal(123000+int64(i)*10000, 123000+int64(i)*10000); err != nil {
			t.Fatalf("scrape %d failed: %s", i, err)
		}
		if got := adm.testInflightBytes(); got != 0 {
			t.Fatalf("scrape %d leaked reservations: %d bytes", i, got)
		}
		if got := adm.testProcessingInFlight(); got != 0 {
			t.Fatalf("scrape %d leaked processing slots: %d", i, got)
		}
	}
	if pushCalls.Load() == 0 {
		t.Fatal("no data was pushed")
	}
	if sw.prevBodyLen == 0 {
		t.Fatal("prevBodyLen must be updated from the actual response size")
	}
}

// TestScrapeInternalAdmissionDisabledPath ensures the default (disabled) configuration
// keeps using the legacy first-come-first-served path unchanged.
func TestScrapeInternalAdmissionDisabledPath(t *testing.T) {
	if getScrapeAdmission() != nil {
		t.Fatal("admission singleton must be absent when the feature is not enabled")
	}
	sw := newAdmissionTestScrapeWork(t, "legacy-job")
	sw.ReadData = func(dst *chunkedbuffer.Buffer) (bool, error) {
		dst.MustWrite([]byte("foo 1\n"))
		return false, nil
	}
	var pushCalls atomic.Int64
	sw.PushData = func(_ *auth.Token, _ *prompb.WriteRequest) {
		pushCalls.Add(1)
	}
	if err := sw.scrapeInternal(123000, 123000); err != nil {
		t.Fatalf("legacy scrape failed: %s", err)
	}
	if pushCalls.Load() == 0 {
		t.Fatal("legacy scrape must push data")
	}
	if got := len(processScrapedDataConcurrencyLimitCh); got != 0 {
		t.Fatalf("legacy concurrency channel must be drained after scrape; got %d tokens", got)
	}
}

func contains(s, substr string) bool {
	return bytes.Contains([]byte(s), []byte(substr))
}
