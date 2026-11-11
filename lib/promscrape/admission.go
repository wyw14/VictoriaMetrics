package promscrape

import (
	"errors"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
)

// ScrapeProcessingAdmission is an opt-in admission controller for scraped response processing.
//
// When enabled it replaces the first-come-first-served processScrapedDataConcurrencyLimitCh
// admission with:
//
//   - per-job bounded request queues scheduled with a bounded deficit round-robin policy,
//   - a global byte budget and a per-job byte budget accounting the estimated and the actual
//     memory occupied by scrape responses across their whole lifetime (network download,
//     waiting for processing, decompression and parsing),
//   - a per-response hard byte budget, so gzip-declared lengths are never trusted and
//     decompression bombs fail promptly,
//   - a bounded wait deadline, after which a scrape is explicitly skipped instead of
//     being buffered indefinitely,
//   - explicit skipped/backpressure observability. A skipped scrape never reaches the
//     network layer, never produces fake target failures and never sends stale markers
//     for series it has not processed.
//
// All the reserved bytes are released exactly once on cancellation, job removal and
// global shutdown.
var (
	admissionEnabled = flag.Bool("promscrape.admission.enable", false, "Whether to enable the ScrapeProcessingAdmission controller, which replaces the default first-come-first-served concurrency limiter "+
		"for scraped response processing with per-job deficit scheduling bounded by global and per-job memory byte budgets. "+
		"When disabled, scrape processing behaviour and sampled metrics are unchanged. "+
		"See also -promscrape.admission.* flags")
	admissionGlobalMemoryBytes = flagutil.NewBytes("promscrape.admission.globalMemoryBytes", 256*1024*1024, "The maximum total estimated and actual memory bytes of scrape responses admitted at once across all jobs. "+
		"Responses which cannot fit into the remaining budget wait in per-job queues until -promscrape.admission.waitTimeout elapses")
	admissionJobMemoryBytes = flagutil.NewBytes("promscrape.admission.jobMemoryBytes", 64*1024*1024, "The maximum memory bytes of scrape responses admitted at once per each scrape job. "+
		"See also -promscrape.admission.globalMemoryBytes")
	admissionMaxResponseBytes = flagutil.NewBytes("promscrape.admission.maxResponseBytes", 16*1024*1024, "The hard maximum size in bytes of a single decompressed scrape response accepted by the admission controller. "+
		"HTTP Content-Length and gzip headers are not trusted: the limit is enforced on the actual decompressed bytes, "+
		"so gzip expansion bombs fail promptly instead of buffering unbounded amounts of memory")
	admissionQuantumBytes = flagutil.NewBytes("promscrape.admission.quantumBytes", 1024*1024, "The per-round deficit quantum in bytes for the per-job deficit scheduler. "+
		"Smaller values improve fairness between jobs with differently sized responses at the cost of more scheduling rounds")
	admissionWaitTimeout = flag.Duration("promscrape.admission.waitTimeout", 5*time.Second, "The maximum time a scrape response may wait for admission and for a processing slot before it is skipped. "+
		"Skipped scrapes are reported via vm_promscrape_admission_skipped_total metrics instead of being reported as target network failures")
	admissionMaxQueuedPerJob = flag.Int("promscrape.admission.maxQueuedPerJob", 32, "The maximum number of waiting scrape responses per job. "+
		"Requests arriving when the per-job queue is full are skipped with the queue_full reason")
	admissionMaxConcurrentProcess = flag.Int("promscrape.admission.maxConcurrentProcess", 0, "The maximum number of concurrently processed scrape responses (decompression, parsing and pushing). "+
		"Zero means using the number of available CPUs like the default concurrency limiter")
	admissionMaxJobLabels = flag.Int("promscrape.admission.maxJobLabels", 100, "The maximum number of distinct job label values exposed by vm_promscrape_admission_job_* metrics. "+
		"Jobs above the cap are still scheduled and accounted globally, but do not get per-job labelled metrics")
)

// Skip reasons reported via vm_promscrape_admission_skipped_total{reason="..."}.
const (
	admissionReasonWaitTimeout       = "wait_timeout"
	admissionReasonCanceled          = "canceled"
	admissionReasonShutdown          = "shutdown"
	admissionReasonQueueFull         = "queue_full"
	admissionReasonResponseTooLarge  = "response_too_large"
	admissionReasonMemoryBudget      = "memory_budget"
	admissionReasonProcessingTimeout = "processing_slot_timeout"
)

// errAdmissionSkipped wraps every explicit admission backpressure decision.
//
// Callers must use errors.Is to distinguish an admission skip from a real target
// network or parsing failure: a skipped scrape does not update up, lastScrape state,
// target status and stale markers.
var errAdmissionSkipped = errors.New("scrape skipped by ScrapeProcessingAdmission")

type admissionSkipError struct {
	reason string
}

func (e *admissionSkipError) Error() string {
	return fmt.Sprintf("%s: %s", errAdmissionSkipped, e.reason)
}

func (e *admissionSkipError) Unwrap() error {
	return errAdmissionSkipped
}

func newAdmissionSkipError(reason string) error {
	return &admissionSkipError{reason: reason}
}

// admissionSkipReason returns the admission skip reason for err or an empty string
// when err is not an admission skip.
func admissionSkipReason(err error) string {
	var se *admissionSkipError
	if errors.As(err, &se) {
		return se.reason
	}
	return ""
}

// admissionConfig contains the immutable admission controller configuration.
type admissionConfig struct {
	globalBytes          int64
	jobBytes             int64
	maxResponseBytes     int64
	quantum              int64
	waitTimeout          time.Duration
	maxQueuedPerJob      int
	maxConcurrentProcess int
	maxJobLabels         int
}

func admissionConfigFromFlags() admissionConfig {
	maxConcurrent := *admissionMaxConcurrentProcess
	if maxConcurrent <= 0 {
		maxConcurrent = cgroup.AvailableCPUs()
	}
	cfg := admissionConfig{
		globalBytes:          admissionGlobalMemoryBytes.N,
		jobBytes:             admissionJobMemoryBytes.N,
		maxResponseBytes:     admissionMaxResponseBytes.N,
		quantum:              admissionQuantumBytes.N,
		waitTimeout:          *admissionWaitTimeout,
		maxQueuedPerJob:      *admissionMaxQueuedPerJob,
		maxConcurrentProcess: maxConcurrent,
		maxJobLabels:         *admissionMaxJobLabels,
	}
	cfg.normalize()
	return cfg
}

func (cfg *admissionConfig) normalize() {
	if cfg.globalBytes <= 0 {
		cfg.globalBytes = 256 * 1024 * 1024
	}
	if cfg.jobBytes <= 0 {
		cfg.jobBytes = 64 * 1024 * 1024
	}
	if cfg.jobBytes > cfg.globalBytes {
		// A per-job budget cannot be bigger than the global one without being ineffective.
		cfg.jobBytes = cfg.globalBytes
	}
	if cfg.maxResponseBytes <= 0 {
		cfg.maxResponseBytes = 16 * 1024 * 1024
	}
	if cfg.maxResponseBytes > cfg.jobBytes {
		logger.Warnf("-promscrape.admission.maxResponseBytes=%d exceeds -promscrape.admission.jobMemoryBytes=%d; "+
			"responses bigger than the job budget are always skipped", cfg.maxResponseBytes, cfg.jobBytes)
	}
	if cfg.quantum <= 0 || cfg.quantum > cfg.jobBytes {
		cfg.quantum = cfg.jobBytes
	}
	if cfg.waitTimeout <= 0 {
		cfg.waitTimeout = 5 * time.Second
	}
	if cfg.maxQueuedPerJob <= 0 {
		cfg.maxQueuedPerJob = 32
	}
	if cfg.maxConcurrentProcess <= 0 {
		cfg.maxConcurrentProcess = cgroup.AvailableCPUs()
	}
	if cfg.maxJobLabels <= 0 {
		cfg.maxJobLabels = 100
	}
}

// scrapeProcessingAdmission is the admission controller instance.
//
// All the queue and byte accounting state is guarded by mu. Metric emission is always
// performed after mu has been released (see sync* helpers), so scraping goroutines never
// block on the metrics registry lock while holding the admission lock.
type scrapeProcessingAdmission struct {
	cfg admissionConfig

	mu       sync.Mutex
	stopped  bool
	stopCh   chan struct{}
	stopOnce sync.Once

	jobs       map[string]*jobState
	activeJobs []*jobState

	// inFlightBytes is the sum of reserved bytes of all admitted tickets.
	// It is guarded by mu; peakInFlightBytes tracks its maximum for tests and gauges.
	inFlightBytes      int64
	peakInFlightBytes  int64
	queuedRequests     int
	processingInFlight int

	// processingSlots bounds concurrent decompression/parsing independently of byte budgets.
	processingSlots chan struct{}

	obs *admissionObserver
	mb  *admissionMetricsBundle
}

// jobState tracks the per-job queue and byte accounting.
type jobState struct {
	name string

	// refs is the number of running scraper goroutines belonging to the job.
	refs int

	// queue holds waiting, not yet admitted tickets.
	queue []*admissionTicket

	// inflightBytes is the sum of reserved bytes of admitted tickets for the job.
	inflightBytes int64

	// deficit is the current DRR deficit in bytes.
	deficit int64

	// active indicates the job is currently linked into the activeJobs scheduling ring.
	active bool

	// jm contains the optional per-job labelled metrics. It is nil when the configured
	// job label cap has been reached; global metrics still cover such jobs.
	jm *jobMetrics
}

// admissionTicket represents a single admitted or waiting scrape response.
//
// A ticket is released exactly once via Release, regardless of how many cancellation
// or shutdown events race with the scrape lifecycle.
type admissionTicket struct {
	mgr      *scrapeProcessingAdmission
	job      *jobState
	estimate int64
	ready    chan struct{}

	releaseOnce sync.Once

	// All the fields below are guarded by mgr.mu.
	reserved    int64
	admitted    bool
	inQueue     bool
	processing  bool
	rejectedErr error
}

func newScrapeProcessingAdmission(cfg admissionConfig, enableMetrics bool) *scrapeProcessingAdmission {
	cfg.normalize()
	a := &scrapeProcessingAdmission{
		cfg:             cfg,
		stopCh:          make(chan struct{}),
		jobs:            make(map[string]*jobState),
		processingSlots: make(chan struct{}, cfg.maxConcurrentProcess),
		obs:             newAdmissionObserver(cfg.maxJobLabels, enableMetrics),
	}
	if enableMetrics {
		a.mb = newAdmissionMetricsBundle()
	}
	return a
}

// JobStarted registers a running scraper for the given job name.
func (a *scrapeProcessingAdmission) JobStarted(jobName string) {
	a.mu.Lock()
	js := a.getOrCreateJobLocked(jobName)
	js.refs++
	a.mu.Unlock()
}

// JobStopped unregisters a stopped scraper and cleans up the job state once the job
// has no running scraper, no queued and no in-flight responses.
func (a *scrapeProcessingAdmission) JobStopped(jobName string) {
	a.mu.Lock()
	js := a.jobs[jobName]
	var removedJob *jobState
	if js != nil {
		js.refs--
		if js.refs < 0 {
			js.refs = 0
		}
		removedJob = a.removeJobIfIdleLocked(js)
	}
	a.mu.Unlock()
	a.obs.removeJob(removedJob)
}

// Acquire reserves estimate bytes for a scrape response belonging to jobName and waits
// until the per-job deficit scheduler admits it, the deadline elapses, cancelCh closes
// or the controller stops.
//
// cancelCh may be nil, in which case only the wait deadline and the global shutdown
// bound the wait. On success the returned ticket must be released exactly once.
func (a *scrapeProcessingAdmission) Acquire(cancelCh <-chan struct{}, jobName string, estimate int64) (*admissionTicket, error) {
	if estimate <= 0 {
		return nil, fmt.Errorf("admission estimate must be positive; got %d", estimate)
	}
	// Requests that can never fit any budget are rejected immediately instead of
	// occupying a queue slot until the wait deadline.
	if estimate > a.cfg.maxResponseBytes || estimate > a.cfg.jobBytes || estimate > a.cfg.globalBytes {
		a.incSkipped(admissionReasonResponseTooLarge)
		return nil, newAdmissionSkipError(admissionReasonResponseTooLarge)
	}
	// Do not enqueue callers which are already canceled. A nil cancelCh is never
	// ready, so the default branch runs in that case.
	select {
	case <-cancelCh:
		a.incSkipped(admissionReasonCanceled)
		return nil, newAdmissionSkipError(admissionReasonCanceled)
	default:
	}

	t := &admissionTicket{
		mgr:      a,
		estimate: estimate,
		ready:    make(chan struct{}),
	}

	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		a.incSkipped(admissionReasonShutdown)
		return nil, newAdmissionSkipError(admissionReasonShutdown)
	}
	js := a.getOrCreateJobLocked(jobName)
	t.job = js
	if len(js.queue) >= a.cfg.maxQueuedPerJob {
		a.mu.Unlock()
		a.incSkipped(admissionReasonQueueFull)
		return nil, newAdmissionSkipError(admissionReasonQueueFull)
	}
	if len(js.queue) == 0 {
		// A job starts every scheduling epoch with zero deficit, so jobs that become
		// active again cannot accumulate unfair historic credit.
		js.deficit = 0
	}
	js.queue = append(js.queue, t)
	t.inQueue = true
	if !js.active {
		js.active = true
		a.activeJobs = append(a.activeJobs, js)
	}
	a.queuedRequests++
	waitStart := time.Now()
	a.dispatchLocked()
	snap := a.snapshotLocked(js)
	alreadyAdmitted := t.admitted
	a.mu.Unlock()

	a.syncMetrics(snap)
	if alreadyAdmitted {
		a.observeAdmitted(waitStart)
		return t, nil
	}

	timer := time.NewTimer(a.cfg.waitTimeout)
	defer timer.Stop()
	select {
	case <-t.ready:
		if !t.admitted {
			err := t.rejectedErr
			if err == nil {
				err = newAdmissionSkipError(admissionReasonShutdown)
			}
			a.incSkipped(admissionSkipReason(err))
			return nil, err
		}
		a.observeAdmitted(waitStart)
		return t, nil
	case <-timer.C:
		if err := a.finishSkippedWait(t, admissionReasonWaitTimeout, waitStart); err != nil {
			return nil, err
		}
		// The ticket was admitted concurrently with the timeout: the admission wins,
		// so the caller remains responsible for releasing it.
		return t, nil
	case <-cancelCh:
		if err := a.finishSkippedWait(t, admissionReasonCanceled, waitStart); err != nil {
			return nil, err
		}
		return t, nil
	case <-a.stopCh:
		if err := a.finishSkippedWait(t, admissionReasonShutdown, waitStart); err != nil {
			return nil, err
		}
		return t, nil
	}
}

// finishSkippedWait removes a waiting ticket on timeout, cancellation or shutdown.
//
// If the ticket was admitted concurrently, the admission wins and the ticket is
// returned to the caller, which remains responsible for releasing it.
func (a *scrapeProcessingAdmission) finishSkippedWait(t *admissionTicket, reason string, waitStart time.Time) error {
	a.mu.Lock()
	if t.admitted {
		snap := a.snapshotLocked(t.job)
		a.mu.Unlock()
		a.syncMetrics(snap)
		a.observeAdmitted(waitStart)
		return nil
	}
	a.removeQueuedTicketLocked(t)
	a.dispatchLocked()
	snap := a.snapshotLocked(t.job)
	a.mu.Unlock()
	a.syncMetrics(snap)
	a.incSkipped(reason)
	return newAdmissionSkipError(reason)
}

// AcquireProcessing obtains a CPU-bound processing slot. The downloaded response keeps
// occupying its byte reservation while waiting here, so waiting responses are accounted
// against the memory budget instead of being buffered for free.
func (t *admissionTicket) AcquireProcessing(cancelCh <-chan struct{}) error {
	a := t.mgr
	timer := time.NewTimer(a.cfg.waitTimeout)
	defer timer.Stop()
	select {
	case a.processingSlots <- struct{}{}:
		a.mu.Lock()
		t.processing = true
		a.processingInFlight++
		snap := a.snapshotLocked(t.job)
		a.mu.Unlock()
		a.syncMetrics(snap)
		return nil
	case <-timer.C:
		a.incSkipped(admissionReasonProcessingTimeout)
		return newAdmissionSkipError(admissionReasonProcessingTimeout)
	case <-cancelCh:
		a.incSkipped(admissionReasonCanceled)
		return newAdmissionSkipError(admissionReasonCanceled)
	case <-a.stopCh:
		a.incSkipped(admissionReasonShutdown)
		return newAdmissionSkipError(admissionReasonShutdown)
	}
}

// Reserve adds n bytes to the ticket reservation, e.g. to cover the coexistence of
// compressed and decompressed response buffers. It fails without mutating any counter
// when the global or per-job budget would be exceeded.
func (t *admissionTicket) Reserve(n int64) error {
	if n <= 0 {
		return nil
	}
	a := t.mgr
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		a.incSkipped(admissionReasonShutdown)
		return newAdmissionSkipError(admissionReasonShutdown)
	}
	js := t.job
	if a.inFlightBytes+n > a.cfg.globalBytes || js.inflightBytes+n > a.cfg.jobBytes {
		snap := a.snapshotLocked(js)
		a.mu.Unlock()
		a.syncMetrics(snap)
		a.incSkipped(admissionReasonMemoryBudget)
		return newAdmissionSkipError(admissionReasonMemoryBudget)
	}
	js.inflightBytes += n
	a.inFlightBytes += n
	t.reserved += n
	if a.inFlightBytes > a.peakInFlightBytes {
		a.peakInFlightBytes = a.inFlightBytes
	}
	snap := a.snapshotLocked(js)
	a.mu.Unlock()
	a.syncMetrics(snap)
	return nil
}

// Settle adjusts the ticket reservation to the actual measured memory usage.
//
// Over-estimations return the difference; under-estimations try to reserve the extra
// bytes and fail explicitly when no budget is left. Counters are guarded against
// negative values: a settlement that would underflow returns an error instead of
// corrupting the global accounting.
func (t *admissionTicket) Settle(actual int64) error {
	a := t.mgr
	a.mu.Lock()
	js := t.job
	delta := actual - t.reserved
	switch {
	case delta == 0:
		a.mu.Unlock()
		return nil
	case delta > 0:
		if a.stopped || a.inFlightBytes+delta > a.cfg.globalBytes || js.inflightBytes+delta > a.cfg.jobBytes {
			snap := a.snapshotLocked(js)
			a.mu.Unlock()
			a.syncMetrics(snap)
			a.incSkipped(admissionReasonMemoryBudget)
			return newAdmissionSkipError(admissionReasonMemoryBudget)
		}
		a.inFlightBytes += delta
		js.inflightBytes += delta
		t.reserved = actual
		if a.inFlightBytes > a.peakInFlightBytes {
			a.peakInFlightBytes = a.inFlightBytes
		}
	default:
		release := -delta
		if release > a.inFlightBytes || release > js.inflightBytes || release > t.reserved {
			// Never let byte accounting go negative or return bytes twice.
			a.mu.Unlock()
			return fmt.Errorf("BUG: admission settlement would release %d bytes while only %d global, %d job and %d ticket bytes are reserved",
				release, a.inFlightBytes, js.inflightBytes, t.reserved)
		}
		a.inFlightBytes -= release
		js.inflightBytes -= release
		t.reserved = actual
	}
	snap := a.snapshotLocked(js)
	a.mu.Unlock()
	a.syncMetrics(snap)
	return nil
}

// Release returns all the reserved bytes and the processing slot exactly once.
//
// It is safe to call concurrently with cancellation, job removal and shutdown;
// repeated calls are no-ops.
func (t *admissionTicket) Release() {
	t.releaseOnce.Do(func() {
		a := t.mgr
		a.mu.Lock()
		js := t.job
		if t.reserved > 0 {
			if t.reserved > a.inFlightBytes || t.reserved > js.inflightBytes {
				// Defensive guard: accounting invariants must never go negative.
				logger.Panicf("BUG: admission ticket releases %d bytes exceeding global in-flight %d or job %q in-flight %d",
					t.reserved, a.inFlightBytes, js.name, js.inflightBytes)
			}
			a.inFlightBytes -= t.reserved
			js.inflightBytes -= t.reserved
			t.reserved = 0
		}
		if t.processing {
			t.processing = false
			a.processingInFlight--
			select {
			case <-a.processingSlots:
			default:
				logger.Panicf("BUG: processing slot cannot be returned for job %q", js.name)
			}
		}
		removedJob := a.removeJobIfIdleLocked(js)
		a.dispatchLocked()
		snap := a.snapshotLocked(js)
		a.mu.Unlock()

		a.syncMetrics(snap)
		a.obs.removeJob(removedJob)
	})
}

// Stop shuts the controller down. All waiting requests are rejected with the shutdown
// reason; admitted tickets keep their reservations until their own Release calls, which
// remain valid. Stop is idempotent.
func (a *scrapeProcessingAdmission) Stop() {
	a.stopOnce.Do(func() {
		close(a.stopCh)
	})
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return
	}
	a.stopped = true
	var rejected []*admissionTicket
	for _, js := range a.jobs {
		for _, t := range js.queue {
			t.rejectedErr = newAdmissionSkipError(admissionReasonShutdown)
			rejected = append(rejected, t)
		}
		js.queue = nil
		js.active = false
	}
	a.activeJobs = nil
	a.queuedRequests = 0
	a.mu.Unlock()

	for _, t := range rejected {
		close(t.ready)
	}
	a.obs.removeAll()
}

// dispatchLocked implements bounded deficit round-robin scheduling over active jobs.
//
// Every active job receives one quantum of deficit per ring sweep. The head request of
// a job is admitted while it fits both the accumulated deficit and the global and
// per-job byte budgets. Sweeps are repeated while at least one request is blocked only
// by its deficit (not by the budgets): this bounds the wait of every request that fits
// the configured budgets to ceil(cost/quantum) sweeps. When every remaining request is
// budget-blocked the dispatch stops and the next release event restarts it, which keeps
// inter-job fairness instead of degenerating into first-come-first-served admission.
func (a *scrapeProcessingAdmission) dispatchLocked() {
	if a.stopped {
		return
	}
	for {
		progress := false
		budgetBlocked := false
		for i := 0; i < len(a.activeJobs); {
			js := a.activeJobs[i]
			if len(js.queue) == 0 {
				// Compact the ring in place.
				copy(a.activeJobs[i:], a.activeJobs[i+1:])
				a.activeJobs[len(a.activeJobs)-1] = nil
				a.activeJobs = a.activeJobs[:len(a.activeJobs)-1]
				js.active = false
				continue
			}
			js.deficit += a.cfg.quantum
			for len(js.queue) > 0 {
				t := js.queue[0]
				cost := t.estimate
				if cost > js.deficit {
					// The request needs additional deficit from a later sweep.
					break
				}
				if a.inFlightBytes+cost > a.cfg.globalBytes || js.inflightBytes+cost > a.cfg.jobBytes {
					// No room for this job's head request; smaller requests of other jobs
					// may still fit the remaining budget, so continue the ring sweep.
					budgetBlocked = true
					break
				}
				js.queue = js.queue[1:]
				t.inQueue = false
				js.deficit -= cost
				js.inflightBytes += cost
				a.inFlightBytes += cost
				if a.inFlightBytes > a.peakInFlightBytes {
					a.peakInFlightBytes = a.inFlightBytes
				}
				a.queuedRequests--
				t.reserved = cost
				t.admitted = true
				close(t.ready)
				progress = true
			}
			i++
		}
		if progress {
			continue
		}
		if budgetBlocked {
			// Wait for a Release event to restart the dispatch.
			return
		}
		// Nothing was admitted and nothing is budget-blocked: every queued head request
		// is waiting for more deficit, so another sweep can make progress. The loop is
		// bounded: each sweep adds a quantum and requests have finite configured costs.
		if len(a.activeJobs) == 0 {
			return
		}
	}
}

func (a *scrapeProcessingAdmission) getOrCreateJobLocked(name string) *jobState {
	js := a.jobs[name]
	if js == nil {
		js = &jobState{
			name: name,
			jm:   a.obs.getJobMetrics(name),
		}
		a.jobs[name] = js
	}
	return js
}

// removeQueuedTicketLocked detaches a still-queued ticket and deactivates the job ring
// entry when its queue becomes empty.
func (a *scrapeProcessingAdmission) removeQueuedTicketLocked(t *admissionTicket) {
	js := t.job
	if !t.inQueue {
		return
	}
	queue := js.queue
	for i, qt := range queue {
		if qt == t {
			js.queue = append(queue[:i], queue[i+1:]...)
			break
		}
	}
	t.inQueue = false
	a.queuedRequests--
	if len(js.queue) == 0 && js.active {
		for i, aj := range a.activeJobs {
			if aj == js {
				copy(a.activeJobs[i:], a.activeJobs[i+1:])
				a.activeJobs[len(a.activeJobs)-1] = nil
				a.activeJobs = a.activeJobs[:len(a.activeJobs)-1]
				break
			}
		}
		js.active = false
		js.deficit = 0
	}
}

// removeJobIfIdleLocked drops the job state when nothing references it anymore.
// It returns the removed state (or nil) so the caller can unregister its metrics
// outside the admission lock.
func (a *scrapeProcessingAdmission) removeJobIfIdleLocked(js *jobState) *jobState {
	if js.refs != 0 || len(js.queue) != 0 || js.inflightBytes != 0 {
		return nil
	}
	delete(a.jobs, js.name)
	if js.active {
		for i, aj := range a.activeJobs {
			if aj == js {
				copy(a.activeJobs[i:], a.activeJobs[i+1:])
				a.activeJobs[len(a.activeJobs)-1] = nil
				a.activeJobs = a.activeJobs[:len(a.activeJobs)-1]
				break
			}
		}
		js.active = false
	}
	return js
}

// admissionSnapshot is a lock-consistent snapshot of the counters needed for metrics.
type admissionSnapshot struct {
	js                 *jobState
	globalInflight     int64
	peakInflight       int64
	queued             int
	processingInFlight int
	jobInflight        int64
	jobQueued          int64
}

func (a *scrapeProcessingAdmission) snapshotLocked(js *jobState) admissionSnapshot {
	snap := admissionSnapshot{
		js:                 js,
		globalInflight:     a.inFlightBytes,
		peakInflight:       a.peakInFlightBytes,
		queued:             a.queuedRequests,
		processingInFlight: a.processingInFlight,
	}
	if js != nil {
		snap.jobInflight = js.inflightBytes
		snap.jobQueued = int64(len(js.queue))
	}
	return snap
}

// syncMetrics emits the snapshot counters outside the admission lock.
func (a *scrapeProcessingAdmission) syncMetrics(snap admissionSnapshot) {
	if a.mb == nil {
		return
	}
	a.mb.inflightBytes.Set(float64(snap.globalInflight))
	a.mb.peakInflightBytes.Set(float64(snap.peakInflight))
	a.mb.queuedRequests.Set(float64(snap.queued))
	a.mb.processingInflight.Set(float64(snap.processingInFlight))
	if snap.js != nil && snap.js.jm != nil {
		snap.js.jm.inflightBytes.Set(float64(snap.jobInflight))
		snap.js.jm.queuedRequests.Set(float64(snap.jobQueued))
	}
}

func (a *scrapeProcessingAdmission) observeAdmitted(start time.Time) {
	if a.mb == nil {
		return
	}
	a.mb.admitted.Inc()
	a.mb.waitDuration.UpdateDuration(start)
}

func (a *scrapeProcessingAdmission) incSkipped(reason string) {
	if a.mb == nil {
		return
	}
	if c := a.mb.skipped[reason]; c != nil {
		c.Inc()
	}
}

// maxResponseBytes exposes the single-response hard cap to response readers.
func (t *admissionTicket) maxResponseBytes() int64 {
	return t.mgr.cfg.maxResponseBytes
}

// admissionMetricsBundle contains global, label-free metrics.
type admissionMetricsBundle struct {
	inflightBytes      *metrics.Gauge
	peakInflightBytes  *metrics.Gauge
	queuedRequests     *metrics.Gauge
	processingInflight *metrics.Gauge
	admitted           *metrics.Counter
	skipped            map[string]*metrics.Counter
	waitDuration       *metrics.Histogram
}

func newAdmissionMetricsBundle() *admissionMetricsBundle {
	skippedReasons := []string{
		admissionReasonWaitTimeout,
		admissionReasonCanceled,
		admissionReasonShutdown,
		admissionReasonQueueFull,
		admissionReasonResponseTooLarge,
		admissionReasonMemoryBudget,
		admissionReasonProcessingTimeout,
	}
	skipped := make(map[string]*metrics.Counter, len(skippedReasons))
	for _, reason := range skippedReasons {
		name := fmt.Sprintf(`vm_promscrape_admission_skipped_total{reason=%q}`, reason)
		skipped[reason] = metrics.GetOrCreateCounter(name)
	}
	return &admissionMetricsBundle{
		// GetOrCreate* is used so repeated construction in tests does not panic on
		// duplicate registration. Nil callback gauges are updated via atomic Set.
		inflightBytes:      metrics.GetOrCreateGauge("vm_promscrape_admission_inflight_bytes", nil),
		peakInflightBytes:  metrics.GetOrCreateGauge("vm_promscrape_admission_peak_inflight_bytes", nil),
		queuedRequests:     metrics.GetOrCreateGauge("vm_promscrape_admission_queued_requests", nil),
		processingInflight: metrics.GetOrCreateGauge("vm_promscrape_admission_processing_inflight", nil),
		admitted:           metrics.GetOrCreateCounter("vm_promscrape_admission_admitted_total"),
		skipped:            skipped,
		waitDuration:       metrics.GetOrCreateHistogram("vm_promscrape_admission_wait_duration_seconds"),
	}
}

// jobMetrics contains per-job labelled gauges.
type jobMetrics struct {
	inflightBytes  *metrics.Gauge
	queuedRequests *metrics.Gauge
}

// admissionObserver owns per-job metric registration and enforces the configured cap
// on the number of distinct job label values. It has its own lock, independent from the
// admission scheduling lock, and gauge updates themselves are atomic lock-free writes.
type admissionObserver struct {
	mu           sync.Mutex
	m            map[string]*jobMetrics
	maxJobLabels int
	enabled      bool
}

func newAdmissionObserver(maxJobLabels int, enabled bool) *admissionObserver {
	return &admissionObserver{
		m:            make(map[string]*jobMetrics),
		maxJobLabels: maxJobLabels,
		enabled:      enabled,
	}
}

// getJobMetrics returns the labelled gauges for the job or nil when the job label cap
// is reached. The job remains fully accounted by global metrics in that case.
func (o *admissionObserver) getJobMetrics(jobName string) *jobMetrics {
	if !o.enabled {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if jm := o.m[jobName]; jm != nil {
		return jm
	}
	if len(o.m) >= o.maxJobLabels {
		return nil
	}
	jm := &jobMetrics{
		inflightBytes:  metrics.GetOrCreateGauge(fmt.Sprintf(`vm_promscrape_admission_job_inflight_bytes{job=%q}`, jobName), nil),
		queuedRequests: metrics.GetOrCreateGauge(fmt.Sprintf(`vm_promscrape_admission_job_queued_requests{job=%q}`, jobName), nil),
	}
	o.m[jobName] = jm
	return jm
}

// removeJob unregisters the metrics of an idle removed job outside the admission lock.
func (o *admissionObserver) removeJob(js *jobState) {
	if js == nil || !o.enabled || js.jm == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.m[js.name]; !ok {
		return
	}
	delete(o.m, js.name)
	metrics.UnregisterMetric(fmt.Sprintf(`vm_promscrape_admission_job_inflight_bytes{job=%q}`, js.name))
	metrics.UnregisterMetric(fmt.Sprintf(`vm_promscrape_admission_job_queued_requests{job=%q}`, js.name))
}

// removeAll unregisters every per-job metric on controller shutdown.
func (o *admissionObserver) removeAll() {
	if !o.enabled {
		return
	}
	o.mu.Lock()
	names := make([]string, 0, len(o.m))
	for name := range o.m {
		names = append(names, name)
	}
	o.m = make(map[string]*jobMetrics)
	o.mu.Unlock()
	for _, name := range names {
		metrics.UnregisterMetric(fmt.Sprintf(`vm_promscrape_admission_job_inflight_bytes{job=%q}`, name))
		metrics.UnregisterMetric(fmt.Sprintf(`vm_promscrape_admission_job_queued_requests{job=%q}`, name))
	}
}

// scrapeAdmission holds the optional process-wide admission controller singleton.
// An atomic pointer load keeps the disabled hot path allocation-free.
var scrapeAdmission atomic.Pointer[scrapeProcessingAdmission]

func getScrapeAdmission() *scrapeProcessingAdmission {
	return scrapeAdmission.Load()
}

func initScrapeAdmission() {
	if !*admissionEnabled {
		return
	}
	if getScrapeAdmission() != nil {
		return
	}
	adm := newScrapeProcessingAdmission(admissionConfigFromFlags(), true)
	scrapeAdmission.Store(adm)
}

func stopScrapeAdmission() {
	if adm := getScrapeAdmission(); adm != nil {
		adm.Stop()
	}
}
