package promscrape

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
)

// writeGenerationFile writes content to a file under the test directory.
func writeGenerationFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("cannot create dirs for %q: %s", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("cannot write %q: %s", path, err)
	}
}

// staticJobYAML returns a minimal static scrape config job.
func staticJobYAML(name, target string) string {
	return fmt.Sprintf(`- job_name: %s
  static_configs:
  - targets:
    - %s
`, name, target)
}

func mainConfigYAML(includes []string, scrapeInterval string, inlineJobs ...string) string {
	var b strings.Builder
	if scrapeInterval != "" {
		b.WriteString("global:\n  scrape_interval: " + scrapeInterval + "\n")
	}
	if len(includes) > 0 {
		b.WriteString("scrape_config_files:\n")
		for _, inc := range includes {
			b.WriteString("- " + inc + "\n")
		}
	}
	if len(inlineJobs) > 0 {
		b.WriteString("scrape_configs:\n")
		for _, j := range inlineJobs {
			b.WriteString(j)
		}
	}
	return b.String()
}

func newTestManager(t *testing.T, factory SDControllerFactory, opts generationManagerOptions) *generationManager {
	t.Helper()
	opts.factory = factory
	if opts.reader == nil {
		opts.reader = osFileReader{}
	}
	return newGenerationManager(opts)
}

func mustBuildCandidate(t *testing.T, mgr *generationManager, mainPath string) *scrapeConfigGeneration {
	t.Helper()
	g, err := mgr.buildCandidate(context.Background(), mainPath)
	if err != nil {
		t.Fatalf("unexpected buildCandidate error: %s", err)
	}
	return g
}

func jobNames(g *scrapeConfigGeneration) []string {
	a := make([]string, 0, len(g.cfg.ScrapeConfigs))
	for _, sc := range g.cfg.ScrapeConfigs {
		a = append(a, sc.JobName)
	}
	return a
}

// fakeSDFactory is a controllable SD controller factory used to verify the
// publish/handoff protocol without launching any real discovery goroutine.
type fakeSDFactory struct {
	mu      sync.Mutex
	creates int
	starts  int
	stops   int
	// live holds every currently running controller. During a start-new-then-stop-old
	// handoff two controllers of the same job can be live briefly, hence the set is
	// keyed by controller identity rather than by job name.
	live map[*fakeController]struct{}
	// failJobs maps a job name to the number of remaining Start calls that must fail.
	failJobs map[string]int
}

func newFakeSDFactory() *fakeSDFactory {
	return &fakeSDFactory{
		live:     make(map[*fakeController]struct{}),
		failJobs: make(map[string]int),
	}
}

func (f *fakeSDFactory) NewController(_ *Config, sc *ScrapeConfig) SDController {
	f.mu.Lock()
	f.creates++
	f.mu.Unlock()
	return &fakeController{factory: f, job: sc.JobName, doneCh: make(chan struct{})}
}

func (f *fakeSDFactory) failNextStart(job string) {
	f.mu.Lock()
	f.failJobs[job]++
	f.mu.Unlock()
}

func (f *fakeSDFactory) liveJobs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := make(map[string]struct{}, len(f.live))
	for c := range f.live {
		seen[c.job] = struct{}{}
	}
	a := make([]string, 0, len(seen))
	for name := range seen {
		a = append(a, name)
	}
	slices.Sort(a)
	return a
}

type fakeController struct {
	factory  *fakeSDFactory
	job      string
	startCnt atomic.Int32
	stopOnce sync.Once
	doneCh   chan struct{}
}

func (c *fakeController) JobName() string { return c.job }

func (c *fakeController) Start() error {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()
	if c.factory.failJobs[c.job] > 0 {
		c.factory.failJobs[c.job]--
		return fmt.Errorf("injected SD start failure for job %q", c.job)
	}
	if c.startCnt.Add(1) > 1 {
		return fmt.Errorf("controller for job %q started twice", c.job)
	}
	if _, ok := c.factory.live[c]; ok {
		return fmt.Errorf("controller for job %q registered twice", c.job)
	}
	c.factory.live[c] = struct{}{}
	c.factory.starts++
	return nil
}

func (c *fakeController) Stop() {
	c.stopOnce.Do(func() {
		c.factory.mu.Lock()
		if _, ok := c.factory.live[c]; ok {
			delete(c.factory.live, c)
			c.factory.stops++
		}
		c.factory.mu.Unlock()
		close(c.doneCh)
	})
}

func (c *fakeController) Done() <-chan struct{} { return c.doneCh }

// faultFileReader wraps the real filesystem reader and injects read faults to
// emulate concurrent rename/replace conditions deterministically.
type faultFileReader struct {
	inner fileReader

	mu sync.Mutex
	// statCalls counts Stat invocations per path.
	statCalls map[string]int
	// mismatchStat maps path to the number of upcoming Stat calls whose result must
	// report a different size/mtime, emulating a replacement during read.
	mismatchStat map[string]int
	// blockRead maps path to a channel that ReadFile waits on, enabling cancellation
	// tests.
	blockRead map[string]chan struct{}
}

func newFaultFileReader() *faultFileReader {
	return &faultFileReader{
		inner:        osFileReader{},
		statCalls:    make(map[string]int),
		mismatchStat: make(map[string]int),
		blockRead:    make(map[string]chan struct{}),
	}
}

func (r *faultFileReader) Stat(path string) (os.FileInfo, error) {
	r.mu.Lock()
	callIdx := r.statCalls[path]
	r.statCalls[path] = callIdx + 1
	mismatch := r.mismatchStat[path]
	if mismatch > 0 {
		r.mismatchStat[path] = mismatch - 1
	}
	r.mu.Unlock()
	fi, err := r.inner.Stat(path)
	if err != nil {
		return nil, err
	}
	// Each read attempt performs two Stat calls. Faking only the odd (pre-read) call
	// makes pre/post stats differ, which is exactly the replacement signal the
	// consistency protocol looks for.
	if mismatch > 0 && callIdx%2 == 0 {
		return fakeFileInfo{FileInfo: fi, size: fi.Size() + 1024, modTime: fi.ModTime().Add(time.Second)}, nil
	}
	return fi, nil
}

func (r *faultFileReader) ReadFile(path string) ([]byte, error) {
	r.mu.Lock()
	ch := r.blockRead[path]
	r.mu.Unlock()
	if ch != nil {
		<-ch
	}
	return r.inner.ReadFile(path)
}

func (r *faultFileReader) Glob(pattern string) ([]string, error) {
	return r.inner.Glob(pattern)
}

type fakeFileInfo struct {
	os.FileInfo
	size    int64
	modTime time.Time
}

func (f fakeFileInfo) Size() int64        { return f.size }
func (f fakeFileInfo) ModTime() time.Time { return f.modTime }

func TestGenerationFlagDisabledByDefault(t *testing.T) {
	// The legacy reload entry must remain the default.
	if *configGenerationEnabled {
		t.Fatalf("-promscrape.configGeneration must default to false")
	}
}

func TestGenerationBuildCandidateDigestAndSortedFiles(t *testing.T) {
	root := t.TempDir()
	incDir := filepath.Join(root, "includes")
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, filepath.Join(incDir, "z.yml"), staticJobYAML("job-z", "localhost:9100"))
	writeGenerationFile(t, filepath.Join(incDir, "a.yml"), staticJobYAML("job-a", "localhost:9101"))
	writeGenerationFile(t, filepath.Join(incDir, "m.yml"), staticJobYAML("job-m", "localhost:9102"))
	writeGenerationFile(t, mainPath, mainConfigYAML([]string{"includes/*.yml"}, "10s"))

	mgr := newTestManager(t, newFakeSDFactory(), generationManagerOptions{})
	g1 := mustBuildCandidate(t, mgr, mainPath)

	if got := jobNames(g1); len(got) != 3 {
		t.Fatalf("unexpected jobs: %v", got)
	}
	// The digested file list must be sorted by absolute path regardless of glob order.
	paths := make([]string, 0, len(g1.files))
	for _, cf := range g1.files {
		paths = append(paths, cf.absPath)
	}
	if !slices.IsSorted(paths) {
		t.Fatalf("generation files are not sorted: %v", paths)
	}
	if len(g1.files) != 4 {
		t.Fatalf("unexpected file count: got %d, want 4 (main + 3 includes)", len(g1.files))
	}

	// Rebuilding from identical contents yields the same digest.
	g2 := mustBuildCandidate(t, mgr, mainPath)
	if g1.digest != g2.digest {
		t.Fatalf("digest changed without file changes: %q vs %q", g1.digest, g2.digest)
	}

	// Changing one file changes the digest; the per-file digest is xxhash of raw bytes.
	writeGenerationFile(t, filepath.Join(incDir, "a.yml"), staticJobYAML("job-a", "localhost:9999"))
	g3 := mustBuildCandidate(t, mgr, mainPath)
	if g3.digest == g1.digest {
		t.Fatalf("digest did not change after include modification")
	}
	changedAbs := filepath.Join(incDir, "a.yml")
	raw, err := os.ReadFile(changedAbs)
	if err != nil {
		t.Fatalf("cannot read changed include: %s", err)
	}
	if got := g3.fileByPath[changedAbs].digest; got != xxhash.Sum64(raw) {
		t.Fatalf("unexpected per-file digest: got %x, want %x", got, xxhash.Sum64(raw))
	}
}

func TestGenerationCandidateStartsNoSDAndNoActiveSideEffect(t *testing.T) {
	root := t.TempDir()
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s", staticJobYAML("job-a", "localhost:9100")))
	factory := newFakeSDFactory()
	mgr := newTestManager(t, factory, generationManagerOptions{})

	_ = mustBuildCandidate(t, mgr, mainPath)
	if factory.creates != 0 || factory.starts != 0 {
		t.Fatalf("candidate preparation must not create or start SD controllers; creates=%d starts=%d", factory.creates, factory.starts)
	}
	if mgr.activeGeneration() != nil {
		t.Fatalf("candidate preparation must not publish a generation")
	}
}

func TestGenerationLastIncludeCorrupted(t *testing.T) {
	root := t.TempDir()
	incDir := filepath.Join(root, "includes")
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, filepath.Join(incDir, "a.yml"), staticJobYAML("job-a", "localhost:9100"))
	writeGenerationFile(t, filepath.Join(incDir, "b.yml"), staticJobYAML("job-b", "localhost:9101"))
	writeGenerationFile(t, filepath.Join(incDir, "z.yml"), "this is: [not: valid: yaml: [\n")
	writeGenerationFile(t, mainPath, mainConfigYAML([]string{"includes/*.yml"}, "10s"))

	factory := newFakeSDFactory()
	mgr := newTestManager(t, factory, generationManagerOptions{})
	if _, err := mgr.buildCandidate(context.Background(), mainPath); err == nil {
		t.Fatalf("expected error when the last include is corrupted")
	} else if !strings.Contains(err.Error(), "z.yml") {
		t.Fatalf("error should mention the corrupted file; got: %s", err)
	}
	// A failed preparation must not touch SD at all.
	if factory.creates != 0 || factory.starts != 0 || factory.stops != 0 {
		t.Fatalf("failed candidate preparation must not touch SD controllers: %+v", factory)
	}

	// Fixing the file allows a normal publish; the failure left no active generation.
	writeGenerationFile(t, filepath.Join(incDir, "z.yml"), staticJobYAML("job-z", "localhost:9102"))
	g := mustBuildCandidate(t, mgr, mainPath)
	active, _, err := mgr.publish(g)
	if err != nil {
		t.Fatalf("unexpected publish error after fixing include: %s", err)
	}
	if got := jobNames(active); len(got) != 3 {
		t.Fatalf("unexpected jobs after recovery: %v", got)
	}
}

func TestGenerationIdempotentPublish(t *testing.T) {
	root := t.TempDir()
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s", staticJobYAML("job-a", "localhost:9100")))
	factory := newFakeSDFactory()
	mgr := newTestManager(t, factory, generationManagerOptions{})

	g1 := mustBuildCandidate(t, mgr, mainPath)
	active1, stats, err := mgr.publish(g1)
	if err != nil {
		t.Fatalf("first publish failed: %s", err)
	}
	if stats.idempotent || factory.creates != 1 {
		t.Fatalf("unexpected first publish stats: %+v creates=%d", stats, factory.creates)
	}

	g2 := mustBuildCandidate(t, mgr, mainPath)
	active2, stats, err := mgr.publish(g2)
	if err != nil {
		t.Fatalf("second publish failed: %s", err)
	}
	if !stats.idempotent {
		t.Fatalf("same-digest publish must be idempotent")
	}
	if active1 != active2 {
		t.Fatalf("idempotent publish must return the same generation")
	}
	if factory.creates != 1 || factory.starts != 1 || factory.stops != 0 {
		t.Fatalf("idempotent publish must not touch jobs: creates=%d starts=%d stops=%d", factory.creates, factory.starts, factory.stops)
	}
}

func TestGenerationReuseUnchangedJobsAndHandoff(t *testing.T) {
	root := t.TempDir()
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s",
		staticJobYAML("keep", "localhost:9100"),
		staticJobYAML("change", "localhost:9101")))
	factory := newFakeSDFactory()
	mgr := newTestManager(t, factory, generationManagerOptions{})

	g1 := mustBuildCandidate(t, mgr, mainPath)
	gen1, _, err := mgr.publish(g1)
	if err != nil {
		t.Fatalf("first publish failed: %s", err)
	}
	keepPrev := gen1.cfg.ScrapeConfigs[0]
	changePrev := gen1.cfg.ScrapeConfigs[1]

	// Only the second job changes; a new job appears and none is removed.
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s",
		staticJobYAML("keep", "localhost:9100"),
		staticJobYAML("change", "localhost:9999"),
		staticJobYAML("added", "localhost:9102")))
	g2 := mustBuildCandidate(t, mgr, mainPath)
	gen2, stats, err := mgr.publish(g2)
	if err != nil {
		t.Fatalf("second publish failed: %s", err)
	}
	if stats.reused != 1 || stats.restarted != 1 || stats.started != 1 || stats.stopped != 0 {
		t.Fatalf("unexpected handoff stats: %+v", stats)
	}
	// The unchanged job keeps its exact previous ScrapeConfig object.
	var keepNow *ScrapeConfig
	for _, sc := range gen2.cfg.ScrapeConfigs {
		if sc.JobName == "keep" {
			keepNow = sc
		}
	}
	if keepNow != keepPrev {
		t.Fatalf("unchanged job object was replaced")
	}
	for _, sc := range gen2.cfg.ScrapeConfigs {
		if sc.JobName == "change" && sc == changePrev {
			t.Fatalf("changed job object was not replaced")
		}
	}
	// Live set reflects exactly the new job set.
	if got := factory.liveJobs(); strings.Join(got, ",") != "added,change,keep" {
		t.Fatalf("unexpected live jobs after handoff: %v", got)
	}
}

func TestGenerationRemovedJobsAreStopped(t *testing.T) {
	root := t.TempDir()
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s",
		staticJobYAML("keep", "localhost:9100"),
		staticJobYAML("drop", "localhost:9101")))
	factory := newFakeSDFactory()
	mgr := newTestManager(t, factory, generationManagerOptions{})
	gen1, _, err := mgr.publish(mustBuildCandidate(t, mgr, mainPath))
	if err != nil {
		t.Fatalf("first publish failed: %s", err)
	}

	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s", staticJobYAML("keep", "localhost:9100")))
	_, stats, err := mgr.publish(mustBuildCandidate(t, mgr, mainPath))
	if err != nil {
		t.Fatalf("second publish failed: %s", err)
	}
	if stats.reused != 1 || stats.stopped != 1 {
		t.Fatalf("unexpected stats when removing a job: %+v", stats)
	}
	if got := factory.liveJobs(); len(got) != 1 || got[0] != "keep" {
		t.Fatalf("removed job must be stopped, live=%v", got)
	}
	// The previous generation is retired and immediately reclaimable (no holders).
	if !gen1.isRetired() {
		t.Fatalf("previous generation must be retired")
	}
	select {
	case <-gen1.done():
	default:
		t.Fatalf("previous generation without holders must be reclaimable")
	}
	if reclaimed := mgr.reapRetired(); reclaimed != 1 {
		t.Fatalf("unexpected reclaimed count: %d", reclaimed)
	}
	if mgr.retiredCount() != 0 {
		t.Fatalf("retired generations leaked: %d", mgr.retiredCount())
	}
}

func TestGenerationGlobalConfigChangeRestartsAllJobs(t *testing.T) {
	root := t.TempDir()
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s", staticJobYAML("job-a", "localhost:9100")))
	factory := newFakeSDFactory()
	mgr := newTestManager(t, factory, generationManagerOptions{})
	if _, _, err := mgr.publish(mustBuildCandidate(t, mgr, mainPath)); err != nil {
		t.Fatalf("first publish failed: %s", err)
	}

	// Identical jobs but a changed global section: every job must restart.
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "30s", staticJobYAML("job-a", "localhost:9100")))
	_, stats, err := mgr.publish(mustBuildCandidate(t, mgr, mainPath))
	if err != nil {
		t.Fatalf("second publish failed: %s", err)
	}
	if stats.reused != 0 || stats.restarted != 1 {
		t.Fatalf("global config change must restart every job; stats=%+v", stats)
	}
}

func TestGenerationPublishFailureRollsBackHalfBatch(t *testing.T) {
	root := t.TempDir()
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s",
		staticJobYAML("old-a", "localhost:9100"),
		staticJobYAML("old-b", "localhost:9101")))
	factory := newFakeSDFactory()
	mgr := newTestManager(t, factory, generationManagerOptions{})
	gen1, _, err := mgr.publish(mustBuildCandidate(t, mgr, mainPath))
	if err != nil {
		t.Fatalf("first publish failed: %s", err)
	}
	beforeLive := factory.liveJobs()

	// New candidate: one reusable job, one replacement and one brand new job whose
	// Start is programmed to fail.
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s",
		staticJobYAML("old-a", "localhost:9100"),
		staticJobYAML("old-b", "localhost:9999"),
		staticJobYAML("brand-new", "localhost:9102")))
	cand := mustBuildCandidate(t, mgr, mainPath)
	factory.failNextStart("brand-new")

	active, stats, err := mgr.publish(cand)
	if err == nil {
		t.Fatalf("publish must fail when a controller cannot start")
	}
	if !strings.Contains(err.Error(), "brand-new") {
		t.Fatalf("error must name the failing job; got: %s", err)
	}
	// The active generation is untouched and all old jobs stay live: no half batch.
	if active != gen1 {
		t.Fatalf("failed publish must keep the previous generation active")
	}
	if got := factory.liveJobs(); strings.Join(got, ",") != strings.Join(beforeLive, ",") {
		t.Fatalf("old jobs changed after failed publish; got=%v want=%v", got, beforeLive)
	}
	if stats.stopped != 0 {
		t.Fatalf("no old job may be stopped on failed publish; stats=%+v", stats)
	}
	// Controllers started during the failed attempt were rolled back.
	if got := factory.liveJobs(); slices.Contains(got, "brand-new") {
		t.Fatalf("rolled-back controller leaked into live set: %v", got)
	}
	if mgr.retiredCount() != 0 {
		t.Fatalf("failed publish must not retire any generation")
	}

	// Clearing the fault allows the same candidate content to publish cleanly:
	// candidate preparation never consumed or mutated the candidate.
	gen2, stats, err := mgr.publish(mustBuildCandidate(t, mgr, mainPath))
	if err != nil {
		t.Fatalf("publish after fault clearing failed: %s", err)
	}
	if got := jobNames(gen2); len(got) != 3 {
		t.Fatalf("unexpected jobs after recovery: %v", got)
	}
	if got := factory.liveJobs(); len(got) != 3 {
		t.Fatalf("expected 3 live jobs after recovery, got %v", got)
	}
}

func TestGenerationOldGenerationReclaimedOnlyAfterUsersExit(t *testing.T) {
	root := t.TempDir()
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s", staticJobYAML("job-a", "localhost:9100")))
	factory := newFakeSDFactory()
	mgr := newTestManager(t, factory, generationManagerOptions{})
	gen1, _, err := mgr.publish(mustBuildCandidate(t, mgr, mainPath))
	if err != nil {
		t.Fatalf("first publish failed: %s", err)
	}
	// A long-lived holder (for example an SD loop still consuming gen1's Config).
	if !gen1.acquire() {
		t.Fatalf("cannot acquire active generation")
	}

	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "20s", staticJobYAML("job-a", "localhost:9100")))
	gen2, _, err := mgr.publish(mustBuildCandidate(t, mgr, mainPath))
	if err != nil {
		t.Fatalf("second publish failed: %s", err)
	}
	if !gen1.isRetired() {
		t.Fatalf("gen1 must be retired after gen2 publish")
	}
	if reclaimed := mgr.reapRetired(); reclaimed != 0 {
		t.Fatalf("retired generation with a live holder must not be reclaimed")
	}
	select {
	case <-gen1.done():
		t.Fatalf("gen1 doneCh must remain open while a holder exists")
	default:
	}

	gen1.release()
	select {
	case <-gen1.done():
	case <-time.After(time.Second):
		t.Fatalf("gen1 must become reclaimable after the last holder exits")
	}
	if reclaimed := mgr.reapRetired(); reclaimed != 1 {
		t.Fatalf("unexpected reclaimed count: %d", reclaimed)
	}
	if mgr.activeGeneration() != gen2 {
		t.Fatalf("gen2 must remain active")
	}
	// No controller leaks: the gen1 controller was stopped during handoff and the
	// gen2 one is the single live controller.
	if got := factory.liveJobs(); len(got) != 1 || got[0] != "job-a" {
		t.Fatalf("unexpected live jobs after reclamation: %v", got)
	}
}

func TestGenerationPathEscapeRejected(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(filepath.Dir(root), "outside-"+filepath.Base(root)+".yml")
	writeGenerationFile(t, outside, staticJobYAML("outside-job", "localhost:9100"))
	t.Cleanup(func() { _ = os.Remove(outside) })

	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML([]string{"../" + filepath.Base(outside)}, "10s"))
	mgr := newTestManager(t, newFakeSDFactory(), generationManagerOptions{})
	_, err := mgr.buildCandidate(context.Background(), mainPath)
	if err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("expected path escape rejection; got: %v", err)
	}

	// Absolute path outside the root is rejected too.
	writeGenerationFile(t, mainPath, mainConfigYAML([]string{outside}, "10s"))
	_, err = mgr.buildCandidate(context.Background(), mainPath)
	if err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("expected absolute path escape rejection; got: %v", err)
	}
}

func TestGenerationIncludeCycleRejected(t *testing.T) {
	root := t.TempDir()
	incDir := filepath.Join(root, "includes")
	// Mapping documents with transitive scrape_config_files: a -> b -> a.
	// Include paths are resolved relative to the main config directory.
	writeGenerationFile(t, filepath.Join(incDir, "a.yml"), `
scrape_config_files:
- includes/b.yml
scrape_configs:
- job_name: job-a
  static_configs:
  - targets:
    - localhost:9100
`)
	writeGenerationFile(t, filepath.Join(incDir, "b.yml"), `
scrape_config_files:
- includes/a.yml
scrape_configs:
- job_name: job-b
  static_configs:
  - targets:
    - localhost:9101
`)
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML([]string{"includes/a.yml"}, "10s"))
	mgr := newTestManager(t, newFakeSDFactory(), generationManagerOptions{})
	_, err := mgr.buildCandidate(context.Background(), mainPath)
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("expected include cycle error; got: %v", err)
	}
}

func TestGenerationTotalBytesLimitRejected(t *testing.T) {
	root := t.TempDir()
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s", staticJobYAML("job-a", "localhost:9100")))
	mgr := newTestManager(t, newFakeSDFactory(), generationManagerOptions{maxTotalBytes: 16})
	_, err := mgr.buildCandidate(context.Background(), mainPath)
	if err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("expected total byte limit error; got: %v", err)
	}
}

func TestGenerationHTTPReferencesRejected(t *testing.T) {
	root := t.TempDir()
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML([]string{"https://example.invalid/scrape.yml"}, "10s"))
	mgr := newTestManager(t, newFakeSDFactory(), generationManagerOptions{})
	if _, err := mgr.buildCandidate(context.Background(), mainPath); err == nil {
		t.Fatalf("http include must be rejected in the generation workflow")
	}

	mgr2 := newTestManager(t, newFakeSDFactory(), generationManagerOptions{})
	if _, err := mgr2.buildCandidate(context.Background(), "http://example.invalid/main.yml"); err == nil {
		t.Fatalf("http main config must be rejected in the generation workflow")
	}
}

func TestGenerationConsistencyProtocolRereadsReplacedFile(t *testing.T) {
	root := t.TempDir()
	mainPath := filepath.Join(root, "main.yml")
	incPath := filepath.Join(root, "inc.yml")
	writeGenerationFile(t, incPath, staticJobYAML("job-a", "localhost:9100"))
	writeGenerationFile(t, mainPath, mainConfigYAML([]string{"inc.yml"}, "10s"))

	reader := newFaultFileReader()
	// The first post-read Stat reports a replacement, forcing exactly one re-read.
	// Stat calls per attempt: pre-read (1), post-read (2); fault the second call.
	reader.mu.Lock()
	reader.mismatchStat[incPath] = 1
	reader.mu.Unlock()

	mgr := newTestManager(t, newFakeSDFactory(), generationManagerOptions{reader: reader, maxReadAttempts: 3})
	g, err := mgr.buildCandidate(context.Background(), mainPath)
	if err != nil {
		t.Fatalf("re-read after mismatch must succeed: %s", err)
	}
	raw, err := os.ReadFile(incPath)
	if err != nil {
		t.Fatalf("cannot read include: %s", err)
	}
	if got := g.fileByPath[incPath].digest; got != xxhash.Sum64(raw) {
		t.Fatalf("digest after re-read does not match stable file contents")
	}

	// Persistent mismatch across all attempts must fail cleanly instead of accepting
	// a torn read.
	reader2 := newFaultFileReader()
	reader2.mu.Lock()
	reader2.mismatchStat[incPath] = 100
	reader2.mu.Unlock()
	mgr2 := newTestManager(t, newFakeSDFactory(), generationManagerOptions{reader: reader2, maxReadAttempts: 3})
	if _, err := mgr2.buildCandidate(context.Background(), mainPath); err == nil {
		t.Fatalf("persistent read instability must reject the candidate")
	} else if !strings.Contains(err.Error(), "consistent read") {
		t.Fatalf("unexpected error for unstable file: %s", err)
	}
}

func TestGenerationConcurrentRename(t *testing.T) {
	root := t.TempDir()
	incPath := filepath.Join(root, "inc.yml")
	altPath := filepath.Join(root, "inc.alt.yml")
	contentA := staticJobYAML("job-a", "localhost:9100")
	contentB := staticJobYAML("job-a", "localhost:9999")
	writeGenerationFile(t, incPath, contentA)
	writeGenerationFile(t, altPath, contentB)
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML([]string{"inc.yml"}, "10s"))

	stop := make(chan struct{})
	var renameWG sync.WaitGroup
	renameWG.Add(1)
	go func() {
		defer renameWG.Done()
		bakPath := incPath + ".bak"
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Swap the include with its alternative via renames, so readers can
			// observe either contents or a momentarily missing path.
			if err := os.Rename(incPath, bakPath); err != nil {
				continue
			}
			if err := os.Rename(altPath, incPath); err != nil {
				_ = os.Rename(bakPath, incPath)
				continue
			}
			if err := os.Rename(incPath, altPath); err != nil {
				continue
			}
			if err := os.Rename(bakPath, incPath); err != nil {
				continue
			}
		}
	}()

	digestA := fmt.Sprintf("%016x", xxhash.Sum64([]byte(contentA)))
	digestB := fmt.Sprintf("%016x", xxhash.Sum64([]byte(contentB)))
	mgr := newTestManager(t, newFakeSDFactory(), generationManagerOptions{maxReadAttempts: 50})

	var g *scrapeConfigGeneration
	var lastErr error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		g, lastErr = mgr.buildCandidate(context.Background(), mainPath)
		if lastErr == nil {
			break
		}
	}
	close(stop)
	renameWG.Wait()

	if lastErr != nil {
		// A clean failure is acceptable; a torn accept is not. Nothing was published.
		if mgr.activeGeneration() != nil {
			t.Fatalf("failed candidate build must not publish a generation")
		}
		return
	}
	gotDigest := g.fileByPath[incPath].Digest()
	if gotDigest != digestA && gotDigest != digestB {
		t.Fatalf("accepted include digest %q matches neither stable variant; torn read was published", gotDigest)
	}
}

func TestGenerationCancellation(t *testing.T) {
	root := t.TempDir()
	incPath := filepath.Join(root, "inc.yml")
	writeGenerationFile(t, incPath, staticJobYAML("job-a", "localhost:9100"))
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML([]string{"inc.yml"}, "10s"))

	reader := newFaultFileReader()
	block := make(chan struct{})
	reader.mu.Lock()
	reader.blockRead[incPath] = block
	reader.mu.Unlock()

	mgr := newTestManager(t, newFakeSDFactory(), generationManagerOptions{reader: reader})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := mgr.buildCandidate(ctx, mainPath)
		errCh <- err
	}()
	// Unblock the pending read only after cancellation; the next consistency-loop
	// iteration must observe the canceled context.
	go func() {
		<-ctx.Done()
		close(block)
	}()
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %s", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("canceled candidate build did not return")
	}
	if mgr.activeGeneration() != nil {
		t.Fatalf("canceled candidate must not become an active generation")
	}
}

func TestGenerationRelabelFiles(t *testing.T) {
	root := t.TempDir()
	relabelPath := filepath.Join(root, "relabel.yml")
	writeGenerationFile(t, relabelPath, `
- source_labels: [__address__]
  target_label: foo
  replacement: bar
`)
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, `
scrape_configs:
- job_name: job-a
  relabel_config_files:
  - relabel.yml
  static_configs:
  - targets:
    - localhost:9100
`)
	mgr := newTestManager(t, newFakeSDFactory(), generationManagerOptions{})
	g := mustBuildCandidate(t, mgr, mainPath)
	if _, ok := g.fileByPath[relabelPath]; !ok {
		t.Fatalf("explicitly referenced relabel file must be part of the generation")
	}
	var sc *ScrapeConfig
	for _, candidate := range g.cfg.ScrapeConfigs {
		if candidate.JobName == "job-a" {
			sc = candidate
		}
	}
	if sc == nil {
		t.Fatalf("job-a missing from generation")
	}
	if sc.swc.relabelConfigs == nil || sc.swc.relabelConfigs.Len() != 1 {
		t.Fatalf("relabel file rules were not merged into the job; len=%v", sc.swc.relabelConfigs)
	}

	// A broken relabel file rejects the candidate before publish.
	writeGenerationFile(t, relabelPath, "- this is: [broken: yaml: [\n")
	if _, err := mgr.buildCandidate(context.Background(), mainPath); err == nil {
		t.Fatalf("broken relabel file must reject the candidate")
	}

	// Touching the relabel file changes the generation digest.
	writeGenerationFile(t, relabelPath, `
- source_labels: [__address__]
  target_label: foo
  replacement: baz
`)
	g2 := mustBuildCandidate(t, mgr, mainPath)
	if g2.digest == g.digest {
		t.Fatalf("digest must change when a relabel file changes")
	}
}

func TestGenerationRelabelFilePathEscapeRejected(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(filepath.Dir(root), "relabel-outside-"+filepath.Base(root)+".yml")
	writeGenerationFile(t, outside, "- source_labels: [__address__]\n  target_label: foo\n")
	t.Cleanup(func() { _ = os.Remove(outside) })
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, fmt.Sprintf(`
scrape_configs:
- job_name: job-a
  relabel_config_files:
  - %s
  static_configs:
  - targets:
    - localhost:9100
`, filepath.ToSlash(filepath.Join("..", filepath.Base(outside)))))
	mgr := newTestManager(t, newFakeSDFactory(), generationManagerOptions{})
	_, err := mgr.buildCandidate(context.Background(), mainPath)
	if err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("expected relabel path escape rejection; got: %v", err)
	}
}

func TestGenerationNilCandidateAndManagerStop(t *testing.T) {
	factory := newFakeSDFactory()
	mgr := newTestManager(t, factory, generationManagerOptions{})
	if _, _, err := mgr.publish(nil); err == nil {
		t.Fatalf("publishing a nil candidate must fail")
	}

	root := t.TempDir()
	mainPath := filepath.Join(root, "main.yml")
	writeGenerationFile(t, mainPath, mainConfigYAML(nil, "10s", staticJobYAML("job-a", "localhost:9100")))
	g, _, err := mgr.publish(mustBuildCandidate(t, mgr, mainPath))
	if err != nil {
		t.Fatalf("publish failed: %s", err)
	}
	mgr.stop()
	if mgr.activeGeneration() != nil {
		t.Fatalf("active generation must be cleared after stop")
	}
	if !g.isRetired() {
		t.Fatalf("generation must be retired after stop")
	}
	if got := factory.liveJobs(); len(got) != 0 {
		t.Fatalf("all controllers must be stopped after manager stop; live=%v", got)
	}
}
