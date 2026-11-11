package promscrape

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/cespare/xxhash/v2"
	"gopkg.in/yaml.v2"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envtemplate"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs/fscore"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promrelabel"
)

// ScrapeConfigGeneration is an optional, opt-in workflow for loading and reloading
// scrape configuration as immutable, content-addressed generations.
//
// The workflow is explicitly split into two phases:
//
//  1. Candidate preparation. The main config file, every scrape_config_files entry
//     (including transitive includes) and every explicitly referenced local relabel
//     file are read into immutable configFile objects carrying an xxhash digest,
//     fully parsed and validated. No service discovery goroutine is started and no
//     running job is stopped during this phase, so a failed candidate preparation
//     can never affect the active generation.
//  2. Publish. A fully prepared candidate is either rejected before any side effect
//     or committed as the new active generation in one step. Semantically unchanged
//     jobs keep their previous ScrapeConfig object and SD controller; jobs that must
//     change follow an explicit start-new-then-stop-old handoff. Publishing a
//     candidate with the same digest as the active generation is a no-op (idempotent).
//
// Important non-guarantees, by design:
//
//   - The individual files are read with a stat-read-stat consistency protocol and
//     re-read when they change while being read, but the resulting set is NOT an
//     atomic snapshot of the configuration directory. Every file only proves that
//     its own bytes were stable across the two stat calls; different files may still
//     reflect different points in time. The code must never claim otherwise.
//   - Dynamic service discovery results (targets returned by file_sd, http_sd,
//     kubernetes_sd and similar) are not part of the generation and are not frozen.
//     External services keep being queried at runtime after the publish.
var (
	configGenerationEnabled = flag.Bool("promscrape.configGeneration", false, "Whether to enable the optional immutable scrape config generation workflow for loading and reloading -promscrape.config. "+
		"When enabled, the main config, scrape_config_files and explicitly referenced local relabel files are read into digested immutable candidates, fully validated and only then published as an atomic generation; "+
		"failed candidate preparation never stops running jobs. When disabled (default), the legacy reload behavior is preserved. "+
		"See https://docs.victoriametrics.com/victoriametrics/vmagent/#configuration-update")
	configGenerationMaxTotalBytes = flagutil.NewBytes("promscrape.configGeneration.maxTotalBytes", 256*1024*1024, "The maximum total size in bytes of all the local files (main config, scrape_config_files and explicitly referenced local relabel files) "+
		"read while preparing a scrape config generation. Candidates exceeding the limit are rejected before publish. "+
		"Only used when -promscrape.configGeneration is enabled")
)

const (
	// defaultMaxReadAttempts bounds the stat-read-stat re-read loop for a single file.
	defaultMaxReadAttempts = 5

	// generationMetrics... names keep the vm_promscrape_ metrics naming convention.
	generationPublishesMetric       = `vm_promscrape_config_generation_publishes_total`
	generationPublishErrorsMetric   = `vm_promscrape_config_generation_publish_errors_total`
	generationCandidatesMetric      = `vm_promscrape_config_generation_candidates_total`
	generationCandidateErrorsMetric = `vm_promscrape_config_generation_candidate_errors_total`
	generationRetiredMetric         = `vm_promscrape_config_generation_retired_total`
)

// fileReader abstracts the local filesystem operations needed while preparing a
// candidate generation. The production implementation uses os; tests inject fault
// readers to deterministically emulate concurrent rename/replace conditions.
type fileReader interface {
	Stat(path string) (os.FileInfo, error)
	ReadFile(path string) ([]byte, error)
	Glob(pattern string) ([]string, error)
}

// osFileReader is the production fileReader implementation backed by the local filesystem.
type osFileReader struct{}

func (osFileReader) Stat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}

func (osFileReader) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (osFileReader) Glob(pattern string) ([]string, error) {
	return filepath.Glob(pattern)
}

// configFile is an immutable, digested snapshot of a single local configuration file.
//
// The stored data is never mutated after construction. The digest is computed over
// the raw on-disk bytes, before environment variable substitution, so identical file
// contents always produce the same digest regardless of the process environment.
type configFile struct {
	absPath string
	data    []byte
	digest  uint64
	size    int64
}

// Digest returns the hexadecimal xxhash digest of the raw file bytes.
func (cf *configFile) Digest() string {
	return fmt.Sprintf("%016x", cf.digest)
}

// scrapeConfigGeneration is an immutable candidate or active scrape config generation.
//
// A generation becomes reachable for scraping only after generationManager.publish
// commits it. The parsed Config is fully validated but none of its service discovery
// routines is running until the controllers stored in controllers are Started during
// publish.
//
// Old generations are kept alive until every holder (the manager and the service
// discovery loops using the parsed Config) has released them via release; only then
// the doneCh is closed and the generation may be reclaimed.
type scrapeConfigGeneration struct {
	id         uint64
	digest     string
	files      []*configFile // sorted by absPath; immutable after construction
	fileByPath map[string]*configFile
	totalBytes int64
	cfg        *Config
	createdAt  time.Time

	// controllers maps job name to the SD controller created for this generation.
	// It is populated exclusively during publish, never during candidate preparation.
	controllers map[string]SDController

	// refs counts the holders of the generation. A generation can only be reclaimed
	// once it has been retired and the last holder released it.
	refs atomic.Int64

	retireOnce sync.Once
	stopOnce   sync.Once
	retiredCh  chan struct{}
	doneCh     chan struct{}
}

// acquire registers a new holder of the generation.
//
// It returns false when the generation has already been retired, in which case the
// caller must not use it.
func (g *scrapeConfigGeneration) acquire() bool {
	for {
		n := g.refs.Load()
		if n < 0 {
			return false
		}
		if g.refs.CompareAndSwap(n, n+1) {
			select {
			case <-g.retiredCh:
				// The generation was retired concurrently; undo the acquisition.
				g.release()
				return false
			default:
				return true
			}
		}
	}
}

// release unregisters a holder previously registered with acquire.
//
// When the released generation is retired and no holders remain, its doneCh is
// closed exactly once and the generation becomes reclaimable.
func (g *scrapeConfigGeneration) release() {
	if g.refs.Add(-1) == 0 {
		g.stopOnce.Do(func() {
			close(g.doneCh)
		})
	}
}

// retire marks the generation as no longer active.
//
// Existing holders may keep using it until they release it; new acquire calls are
// rejected. If no holder remains at retirement time, the generation is immediately
// reclaimable.
func (g *scrapeConfigGeneration) retire() {
	g.retireOnce.Do(func() {
		close(g.retiredCh)
		if g.refs.Load() == 0 {
			g.stopOnce.Do(func() {
				close(g.doneCh)
			})
		}
	})
}

// done returns the channel closed once the retired generation has no holders left.
func (g *scrapeConfigGeneration) done() <-chan struct{} {
	return g.doneCh
}

// isRetired reports whether the generation has been retired.
func (g *scrapeConfigGeneration) isRetired() bool {
	select {
	case <-g.retiredCh:
		return true
	default:
		return false
	}
}

// SDController controls the service discovery routines of a single scrape job.
//
// The interface exists so that the publish/handoff protocol can be exercised with
// deterministic, controllable implementations in tests instead of real network-backed
// SD goroutines.
type SDController interface {
	// JobName returns the scrape job name controlled by the controller.
	JobName() string
	// Start launches the service discovery routines. It must not be called twice.
	Start() error
	// Stop requests the service discovery routines to stop. It must not block until
	// the routines have fully exited; use Done for that.
	Stop()
	// Done is closed after Stop has completed and all routines have exited.
	Done() <-chan struct{}
}

// SDControllerFactory creates SD controllers for the jobs of a generation.
//
// Factory methods are called only during publish; candidate preparation never
// reaches the factory, which is what guarantees that preparing a candidate starts
// no service discovery goroutine.
type SDControllerFactory interface {
	NewController(cfg *Config, sc *ScrapeConfig) SDController
}

// defaultSDControllerFactory builds controllers backed by the real ScrapeConfig
// mustStart/mustStop service discovery lifecycle.
type defaultSDControllerFactory struct{}

func (defaultSDControllerFactory) NewController(cfg *Config, sc *ScrapeConfig) SDController {
	return &defaultSDController{
		sc:      sc,
		baseDir: cfg.baseDir,
		doneCh:  make(chan struct{}),
	}
}

type defaultSDController struct {
	sc       *ScrapeConfig
	baseDir  string
	stopOnce sync.Once
	doneCh   chan struct{}
}

func (c *defaultSDController) JobName() string {
	return c.sc.JobName
}

func (c *defaultSDController) Start() error {
	c.sc.mustStart(c.baseDir)
	return nil
}

func (c *defaultSDController) Stop() {
	c.stopOnce.Do(func() {
		c.sc.mustStop()
		close(c.doneCh)
	})
}

func (c *defaultSDController) Done() <-chan struct{} {
	return c.doneCh
}

// generationManagerOptions customizes a generationManager. Zero-value fields are
// replaced with production defaults in newGenerationManager.
type generationManagerOptions struct {
	// maxTotalBytes is the cumulative byte budget for all files of a candidate.
	maxTotalBytes int64
	// maxReadAttempts bounds consistency re-reads for a single file.
	maxReadAttempts int
	// reader is the filesystem implementation used for candidate preparation.
	reader fileReader
	// factory builds SD controllers during publish.
	factory SDControllerFactory
	// now is the clock used for generation timestamps.
	now func() time.Time
}

// generationManager owns the active generation and serializes candidate publication.
type generationManager struct {
	opts generationManagerOptions

	mu      sync.Mutex
	active  *scrapeConfigGeneration
	retired []*scrapeConfigGeneration
	nextID  uint64
}

func newGenerationManager(opts generationManagerOptions) *generationManager {
	if opts.maxTotalBytes <= 0 {
		opts.maxTotalBytes = configGenerationMaxTotalBytes.N
	}
	if opts.maxReadAttempts <= 0 {
		opts.maxReadAttempts = defaultMaxReadAttempts
	}
	if opts.reader == nil {
		opts.reader = osFileReader{}
	}
	if opts.factory == nil {
		opts.factory = defaultSDControllerFactory{}
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	return &generationManager{
		opts:   opts,
		nextID: 1,
	}
}

// activeGeneration returns the generation currently committed for scraping.
func (m *generationManager) activeGeneration() *scrapeConfigGeneration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active
}

// publishStats summarizes a publish attempt.
type publishStats struct {
	// idempotent is true when the candidate had the same digest as the active
	// generation and no job was touched.
	idempotent bool
	started    int
	stopped    int
	restarted  int
	reused     int
}

// buildCandidate prepares a fully read, digested, parsed and validated immutable
// candidate generation from mainPath without starting any SD goroutine and without
// touching the active generation.
func (m *generationManager) buildCandidate(ctx context.Context, mainPath string) (*scrapeConfigGeneration, error) {
	metrics.GetOrCreateCounter(generationCandidatesMetric).Inc()
	g, err := m.buildCandidateLocked(ctx, mainPath)
	if err != nil {
		metrics.GetOrCreateCounter(generationCandidateErrorsMetric).Inc()
		return nil, err
	}
	return g, nil
}

func (m *generationManager) buildCandidateLocked(ctx context.Context, mainPath string) (*scrapeConfigGeneration, error) {
	if isHTTPConfigPath(mainPath) {
		return nil, fmt.Errorf("scrape config generation requires local files; http/https config path is not supported: %q", mainPath)
	}
	absMain, err := filepath.Abs(filepath.Clean(mainPath))
	if err != nil {
		return nil, fmt.Errorf("cannot resolve absolute path for %q: %w", mainPath, err)
	}
	rootDir := filepath.Dir(absMain)

	b := &candidateBuilder{
		manager: m,
		reader:  m.opts.reader,
		rootDir: rootDir,
		visited: make(map[string]*configFile),
		stack:   make(map[string]struct{}),
	}

	// Read the main config first; it defines the include tree root.
	mainFile, err := b.readConsistent(ctx, absMain)
	if err != nil {
		return nil, fmt.Errorf("cannot read main scrape config %q: %w", mainPath, err)
	}
	mainCfg := &Config{}
	if err := mainCfg.unmarshal(append([]byte(nil), mainFile.data...), *strictParse); err != nil {
		return nil, fmt.Errorf("cannot parse main scrape config %q: %w", mainPath, err)
	}
	mainCfg.baseDir = rootDir

	// Expand the include tree (scrape_config_files, transitively) with cycle and
	// path-escape detection. Included scrape configs are collected in deterministic
	// first-visit order.
	var includedScrapeConfigs []*ScrapeConfig
	if err := b.expandIncludes(ctx, mainCfg.ScrapeConfigFiles, func(doc *scrapeConfigFileDocument) {
		includedScrapeConfigs = append(includedScrapeConfigs, doc.ScrapeConfigs...)
	}); err != nil {
		return nil, err
	}
	mainCfg.ScrapeConfigFiles = nil
	mainCfg.ScrapeConfigs = append(mainCfg.ScrapeConfigs, includedScrapeConfigs...)

	// Read and parse the explicitly referenced local relabel files and append their
	// rules to the owning jobs. This happens before validation so that a broken
	// relabel file rejects the candidate before publish.
	if err := b.loadRelabelFiles(ctx, mainCfg.ScrapeConfigs); err != nil {
		return nil, err
	}

	// Full parse/validation gate. Duplicate job names and invalid jobs reject the
	// whole candidate; the active generation is untouched.
	if err := validateScrapeConfigs(mainCfg, mainPath); err != nil {
		return nil, err
	}

	// Build the deterministic, sorted file list and the generation digest.
	paths := make([]string, 0, len(b.visited))
	for p := range b.visited {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	files := make([]*configFile, 0, len(paths))
	h := xxhash.New()
	var totalBytes int64
	for _, p := range paths {
		cf := b.visited[p]
		files = append(files, cf)
		totalBytes += cf.size
		var hdr [16]byte
		binary.LittleEndian.PutUint64(hdr[:8], uint64(len(cf.absPath)))
		binary.LittleEndian.PutUint64(hdr[8:], cf.digest)
		_, _ = h.Write([]byte(cf.absPath))
		_, _ = h.Write(hdr[:])
	}

	m.mu.Lock()
	id := m.nextID
	m.nextID++
	m.mu.Unlock()

	return &scrapeConfigGeneration{
		id:          id,
		digest:      fmt.Sprintf("%016x", h.Sum64()),
		files:       files,
		fileByPath:  b.visited,
		totalBytes:  totalBytes,
		cfg:         mainCfg,
		createdAt:   m.opts.now(),
		controllers: make(map[string]SDController),
		retiredCh:   make(chan struct{}),
		doneCh:      make(chan struct{}),
	}, nil
}

// validateScrapeConfigs enforces unique job names and fully initializes every
// scrape work config, which is the last parsing/validation gate before publish.
//
// Unlike the legacy load path, a job that fails validation rejects the entire
// candidate instead of being silently skipped: an all-or-nothing publish cannot be
// built on a partially valid config.
func validateScrapeConfigs(cfg *Config, path string) error {
	jobNames := make(map[string]struct{}, len(cfg.ScrapeConfigs))
	for _, sc := range cfg.ScrapeConfigs {
		if _, ok := jobNames[sc.JobName]; ok {
			return fmt.Errorf("duplicate `job_name` in scrape configs loaded from %q: %q", path, sc.JobName)
		}
		jobNames[sc.JobName] = struct{}{}
	}
	validScrapeConfigs := cfg.ScrapeConfigs[:0]
	for _, sc := range cfg.ScrapeConfigs {
		// Copy sc to drop references to the raw file memory, preventing memory
		// leaks across generations.
		sc = sc.clone()
		swc, err := getScrapeWorkConfig(sc, cfg.baseDir, &cfg.Global)
		if err != nil {
			return fmt.Errorf("cannot initialize scrape config for job_name=%q: %w", sc.JobName, err)
		}
		sc.swc = swc
		validScrapeConfigs = append(validScrapeConfigs, sc)
	}
	cfg.ScrapeConfigs = validScrapeConfigs
	return nil
}

// candidateBuilder performs the graph expansion and consistency-checked reads for
// one candidate generation.
type candidateBuilder struct {
	manager *generationManager
	reader  fileReader
	rootDir string

	// visited contains every file already read for this candidate, keyed by the
	// cleaned absolute path. A file is read at most once per candidate.
	visited map[string]*configFile
	// stack contains the absolute paths currently being expanded, for cycle
	// detection.
	stack map[string]struct{}

	totalBytes int64
}

// readConsistent reads a single local file and verifies that it was not replaced
// while being read.
//
// The protocol is stat before read, read, stat after read: when size, mode or
// modification time differ between the two stats the file may have been replaced
// (for example by a concurrent rename), so the file is re-read. The loop is bounded
// by maxReadAttempts. A returned configFile therefore corresponds to bytes that were
// observed stable across two stats. This does NOT make the multi-file candidate an
// atomic directory snapshot: stability is only proven per individual file.
func (b *candidateBuilder) readConsistent(ctx context.Context, absPath string) (*configFile, error) {
	if cf, ok := b.visited[absPath]; ok {
		return cf, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var lastErr error
	for attempt := 1; attempt <= b.manager.opts.maxReadAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stBefore, err := b.reader.Stat(absPath)
		if err != nil {
			// The file may have been renamed away between the directory listing and
			// the read; retry instead of accepting an inconsistent candidate.
			lastErr = fmt.Errorf("cannot stat %q before read: %w", absPath, err)
			continue
		}
		if stBefore.IsDir() {
			return nil, fmt.Errorf("scrape config path %q is a directory, want a file", absPath)
		}
		data, err := b.reader.ReadFile(absPath)
		if err != nil {
			lastErr = fmt.Errorf("cannot read %q: %w", absPath, err)
			continue
		}
		stAfter, err := b.reader.Stat(absPath)
		if err != nil {
			// The file disappeared or was renamed away right after the read; retry
			// instead of accepting bytes of unknown provenance.
			lastErr = fmt.Errorf("cannot stat %q after read: %w", absPath, err)
			continue
		}
		if stBefore.Size() != stAfter.Size() || stBefore.Mode() != stAfter.Mode() || !stBefore.ModTime().Equal(stAfter.ModTime()) {
			// The file was replaced while being read. Re-read the current contents
			// rather than publishing a possibly torn per-file view.
			lastErr = fmt.Errorf("file %q changed while being read (size/mtime mismatch); re-reading", absPath)
			continue
		}
		if b.totalBytes+int64(len(data)) > b.manager.opts.maxTotalBytes {
			return nil, fmt.Errorf("scrape config generation exceeds the total file size limit of %d bytes while reading %q: "+
				"already read %d bytes, file size is %d bytes; increase -promscrape.configGeneration.maxTotalBytes if the limit is too small",
				b.manager.opts.maxTotalBytes, absPath, b.totalBytes, len(data))
		}
		data = append([]byte(nil), data...)
		cf := &configFile{
			absPath: absPath,
			data:    data,
			digest:  xxhash.Sum64(data),
			size:    int64(len(data)),
		}
		b.visited[absPath] = cf
		b.totalBytes += cf.size
		return cf, nil
	}
	return nil, fmt.Errorf("cannot obtain a consistent read of %q after %d attempts: %w",
		absPath, b.manager.opts.maxReadAttempts, lastErr)
}

// expandIncludes expands a list of scrape_config_files references relative to the
// root directory, recursively. Glob matches are sorted for a fixed, reproducible
// file order; cycles and path escapes are rejected.
func (b *candidateBuilder) expandIncludes(ctx context.Context, refs []string, onDoc func(doc *scrapeConfigFileDocument)) error {
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if isHTTPConfigPath(ref) {
			return fmt.Errorf("scrape config generation supports local files only; http/https include is not allowed: %q", ref)
		}
		fullPath := fscore.GetFilepath(b.rootDir, ref)
		paths := []string{fullPath}
		if strings.Contains(fullPath, "*") || strings.Contains(fullPath, "?") || strings.Contains(fullPath, "[") {
			matches, err := b.reader.Glob(fullPath)
			if err != nil {
				return fmt.Errorf("cannot expand scrape_config_files pattern %q: %w", ref, err)
			}
			// Fixed sorting of glob-expanded file lists.
			sort.Strings(matches)
			paths = matches
		}
		for _, p := range paths {
			absPath, err := filepath.Abs(filepath.Clean(p))
			if err != nil {
				return fmt.Errorf("cannot resolve absolute path for include %q: %w", p, err)
			}
			if err := ensureWithinRoot(b.rootDir, absPath); err != nil {
				return err
			}
			if _, ok := b.stack[absPath]; ok {
				return fmt.Errorf("scrape_config_files reference cycle detected at %q (include chain: %s)",
					absPath, b.chainDescription(absPath))
			}
			if _, ok := b.visited[absPath]; ok {
				// The same file reached through another include branch: reads are
				// de-duplicated, and it cannot introduce a cycle anymore.
				continue
			}
			cf, err := b.readConsistent(ctx, absPath)
			if err != nil {
				return err
			}
			doc, err := parseScrapeConfigFileDocument(cf.data, *strictParse)
			if err != nil {
				return fmt.Errorf("cannot parse scrape_config_files entry %q: %w", absPath, err)
			}
			b.stack[absPath] = struct{}{}
			onDoc(doc)
			err = b.expandIncludes(ctx, doc.ScrapeConfigFiles, onDoc)
			delete(b.stack, absPath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// chainDescription renders the current include stack for cycle error messages.
func (b *candidateBuilder) chainDescription(cyclePath string) string {
	if len(b.stack) == 0 {
		return cyclePath
	}
	paths := make([]string, 0, len(b.stack)+1)
	for p := range b.stack {
		paths = append(paths, p)
	}
	// The stack map has no ordering; sort for deterministic error messages.
	sort.Strings(paths)
	paths = append(paths, cyclePath)
	return strings.Join(paths, " -> ")
}

// loadRelabelFiles reads the explicitly referenced local relabel files of every
// scrape job and appends the parsed rules to the inline relabel configs.
func (b *candidateBuilder) loadRelabelFiles(ctx context.Context, scrapeConfigs []*ScrapeConfig) error {
	for _, sc := range scrapeConfigs {
		if err := b.loadJobRelabelFiles(ctx, sc, sc.RelabelConfigFiles, false); err != nil {
			return err
		}
		if err := b.loadJobRelabelFiles(ctx, sc, sc.MetricRelabelConfigFiles, true); err != nil {
			return err
		}
		// The file lists stay in the struct for digest/equality purposes; the rules
		// have now been merged into the inline configs.
	}
	return nil
}

func (b *candidateBuilder) loadJobRelabelFiles(ctx context.Context, sc *ScrapeConfig, refs []string, metric bool) error {
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if isHTTPConfigPath(ref) {
			return fmt.Errorf("relabel config files must be local; http/https reference is not allowed for job_name=%q: %q", sc.JobName, ref)
		}
		absPath, err := filepath.Abs(filepath.Clean(fscore.GetFilepath(b.rootDir, ref)))
		if err != nil {
			return fmt.Errorf("cannot resolve absolute path for relabel file %q: %w", ref, err)
		}
		if err := ensureWithinRoot(b.rootDir, absPath); err != nil {
			return fmt.Errorf("relabel file reference for job_name=%q escapes the config directory: %w", sc.JobName, err)
		}
		if _, onStack := b.stack[absPath]; onStack {
			return fmt.Errorf("relabel file reference cycle detected at %q", absPath)
		}
		cf, err := b.readConsistent(ctx, absPath)
		if err != nil {
			return fmt.Errorf("cannot read relabel file %q referenced by job_name=%q: %w", ref, sc.JobName, err)
		}
		var rcs []promrelabel.RelabelConfig
		data := envtemplate.ReplaceBytes(append([]byte(nil), cf.data...))
		if *strictParse {
			err = yaml.UnmarshalStrict(data, &rcs)
		} else {
			err = yaml.Unmarshal(data, &rcs)
		}
		if err != nil {
			return fmt.Errorf("cannot parse relabel configs from %q referenced by job_name=%q: %w", ref, sc.JobName, err)
		}
		if metric {
			sc.MetricRelabelConfigs = append(slices.Clone(sc.MetricRelabelConfigs), rcs...)
		} else {
			sc.RelabelConfigs = append(slices.Clone(sc.RelabelConfigs), rcs...)
		}
	}
	return nil
}

// scrapeConfigFileDocument is the on-disk representation of a scrape_config_files
// entry. The legacy bare-list format (a YAML list of scrape configs) is also
// accepted; the mapping format additionally allows transitive scrape_config_files
// includes, which is what makes reference cycles detectable.
type scrapeConfigFileDocument struct {
	ScrapeConfigs     []*ScrapeConfig `yaml:"scrape_configs,omitempty"`
	ScrapeConfigFiles []string        `yaml:"scrape_config_files,omitempty"`
}

func parseScrapeConfigFileDocument(data []byte, isStrict bool) (*scrapeConfigFileDocument, error) {
	data = envtemplate.ReplaceBytes(data)
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) > 0 && (trimmed[0] == '-' || trimmed[0] == '[') {
		// Legacy bare-list format: a YAML sequence of scrape configs.
		var scs []*ScrapeConfig
		var err error
		if isStrict {
			err = yaml.UnmarshalStrict(data, &scs)
		} else {
			err = yaml.Unmarshal(data, &scs)
		}
		if err != nil {
			return nil, err
		}
		return &scrapeConfigFileDocument{ScrapeConfigs: scs}, nil
	}
	var doc scrapeConfigFileDocument
	var err error
	if isStrict {
		err = yaml.UnmarshalStrict(data, &doc)
	} else {
		err = yaml.Unmarshal(data, &doc)
	}
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

// ensureWithinRoot rejects local paths that escape the configuration root directory.
func ensureWithinRoot(rootDir, absPath string) error {
	rel, err := filepath.Rel(rootDir, absPath)
	if err != nil {
		return fmt.Errorf("cannot check path %q against config root %q: %w", absPath, rootDir, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("path %q escapes the configuration root directory %q; references must stay inside it", absPath, rootDir)
	}
	return nil
}

func isHTTPConfigPath(path string) bool {
	return strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://")
}

// publish commits cand as the new active generation.
//
// The handoff protocol is:
//  1. Same-digest candidates are rejected as no-ops: publishing identical contents
//     is idempotent and never touches a job.
//  2. Controllers for new and changed jobs are created and Started while every old
//     job keeps running. A Start failure rolls back every controller started during
//     this attempt and aborts the publish; the active generation, and therefore all
//     old jobs, stays intact (no half-batch of jobs can ever take effect).
//  3. Only after every new controller is running, the active pointer is committed and
//     the superseded/removed old controllers are Stopped. Unchanged jobs keep their
//     previous ScrapeConfig object and controller.
//  4. The previous generation is retired; it is reclaimed only after its last holder
//     releases it.
//
// Credentials are never read into logs: only file paths, digests and job names are
// logged.
func (m *generationManager) publish(cand *scrapeConfigGeneration) (*scrapeConfigGeneration, publishStats, error) {
	metrics.GetOrCreateCounter(generationPublishesMetric).Inc()
	m.mu.Lock()
	defer m.mu.Unlock()

	if cand == nil {
		return m.active, publishStats{}, fmt.Errorf("cannot publish a nil candidate generation")
	}
	if m.active != nil && m.active.digest == cand.digest {
		// Idempotent re-publish of identical file contents.
		return m.active, publishStats{idempotent: true}, nil
	}

	prevGen := m.active
	prevByName := make(map[string]*ScrapeConfig)
	globalChanged := true
	if prevGen != nil {
		for _, scPrev := range prevGen.cfg.ScrapeConfigs {
			prevByName[scPrev.JobName] = scPrev
		}
		globalChanged = !areEqualGlobalConfigs(&cand.cfg.Global, &prevGen.cfg.Global)
	}

	type startItem struct {
		sc   *ScrapeConfig
		prev *ScrapeConfig
		ctrl SDController
	}
	var toStart []startItem
	var stats publishStats
	for i, sc := range cand.cfg.ScrapeConfigs {
		scPrev := prevByName[sc.JobName]
		if scPrev != nil && !globalChanged && areEqualScrapeConfigs(scPrev, sc) {
			// Semantically unchanged job: reuse the previous object and its running
			// controller verbatim.
			cand.cfg.ScrapeConfigs[i] = scPrev
			cand.controllers[sc.JobName] = prevGen.controllers[sc.JobName]
			stats.reused++
			continue
		}
		ctrl := m.opts.factory.NewController(cand.cfg, sc)
		if ctrl == nil {
			return m.active, stats, fmt.Errorf("SD controller factory returned nil for job_name=%q", sc.JobName)
		}
		cand.controllers[sc.JobName] = ctrl
		toStart = append(toStart, startItem{sc: sc, prev: scPrev, ctrl: ctrl})
	}

	// Start every new/changed controller while the old generation is untouched.
	started := make(map[string]SDController, len(toStart))
	for _, item := range toStart {
		if err := item.ctrl.Start(); err != nil {
			// Roll back the controllers started during this failed attempt. No old
			// controller was stopped, so the active generation remains fully in
			// effect. The candidate never becomes reachable for scraping.
			for name, ctrl := range started {
				ctrl.Stop()
				<-ctrl.Done()
				delete(started, name)
			}
			metrics.GetOrCreateCounter(generationPublishErrorsMetric).Inc()
			return m.active, stats, fmt.Errorf("cannot start service discovery for job_name=%q; publish aborted, keeping the previous generation: %w",
				item.sc.JobName, err)
		}
		started[item.sc.JobName] = item.ctrl
		if item.prev != nil {
			stats.restarted++
		} else {
			stats.started++
		}
	}

	// Commit point: from here on cand is the active generation.
	m.active = cand

	// Handoff cleanup: stop controllers of jobs that changed or disappeared. Jobs
	// that were reused keep running inside the new generation and are not stopped.
	if prevGen != nil {
		var stoppedChs []<-chan struct{}
		for name, ctrl := range prevGen.controllers {
			if _, ok := cand.controllers[name]; ok {
				// Transferred unchanged controller; it now belongs to cand.
				continue
			}
			ctrl.Stop()
			stoppedChs = append(stoppedChs, ctrl.Done())
			stats.stopped++
		}
		for _, ch := range stoppedChs {
			<-ch
		}
		m.retireLocked(prevGen)
	}

	logger.Infof("scrape config generation #%d digest=%s published: started=%d, stopped=%d, restarted=%d, reused=%d, files=%d, totalBytes=%d",
		cand.id, cand.digest, stats.started, stats.stopped, stats.restarted, stats.reused, len(cand.files), cand.totalBytes)
	return cand, stats, nil
}

// retireLocked marks a generation as retired and keeps it until reclamation.
func (m *generationManager) retireLocked(g *scrapeConfigGeneration) {
	g.retire()
	metrics.GetOrCreateCounter(generationRetiredMetric).Inc()
	m.retired = append(m.retired, g)
}

// reapRetired drops generations whose holders have all exited.
//
// It returns the number of reclaimed generations. Active generations are never
// reaped.
func (m *generationManager) reapRetired() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	remaining := m.retired[:0]
	reclaimed := 0
	for _, g := range m.retired {
		select {
		case <-g.done():
			reclaimed++
		default:
			remaining = append(remaining, g)
		}
	}
	m.retired = remaining
	return reclaimed
}

// retiredCount returns the number of retired generations still held by someone.
func (m *generationManager) retiredCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.retired)
}

// stop retires the active generation and stops every controller it owns.
//
// Transferred controllers belong to the latest generation by the time stop runs;
// the method stops exactly the controllers still referenced by the active
// generation, which includes every running job at shutdown.
func (m *generationManager) stop() {
	m.mu.Lock()
	g := m.active
	m.active = nil
	if g != nil {
		m.retireLocked(g)
	}
	m.mu.Unlock()

	if g == nil {
		return
	}
	var wg sync.WaitGroup
	for _, ctrl := range g.controllers {
		wg.Add(1)
		go func(ctrl SDController) {
			defer wg.Done()
			ctrl.Stop()
			<-ctrl.Done()
		}(ctrl)
	}
	wg.Wait()
}

// runScraperWithGenerations runs the scraper main loop on top of the immutable
// generation workflow. It mirrors runScraper's SIGHUP/ticker behavior while all
// config loading goes through generationManager, so failed candidate preparation
// never disturbs the active generation.
func runScraperWithGenerations(configFile string, pushData func(at *auth.Token, wr *prompb.WriteRequest), globalStopCh <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-globalStopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	// Register SIGHUP handler before the first candidate build, like runScraper does.
	sighupCh := procutil.NewSighupChan()

	mgr := newGenerationManager(generationManagerOptions{})

	logger.Infof("reading scrape config generation from %q", configFile)
	cand, err := mgr.buildCandidate(ctx, configFile)
	if err != nil {
		logger.Fatalf("cannot build scrape config generation from %q: %s", configFile, err)
	}
	gen, _, err := mgr.publish(cand)
	if err != nil {
		logger.Fatalf("cannot publish initial scrape config generation from %q: %s", configFile, err)
	}
	marshaledData := gen.cfg.marshal()
	configData.Store(&marshaledData)
	configSuccess.Set(1)
	configTimestamp.Set(fasttime.UnixTimestamp())

	scs := newScrapeConfigs(pushData, globalStopCh)
	addBuiltinSDTypes(scs)

	// The service discovery loops hold a reference to the generation whose Config
	// they consume. The reference is released only after every loop has switched to
	// the next generation, which is what allows the old generation to be reclaimed.
	if !gen.acquire() {
		logger.Fatalf("initial scrape config generation was retired before service discovery routines started")
	}
	scs.updateConfig(gen.cfg)

	var tickerCh <-chan time.Time
	if *configCheckInterval > 0 {
		ticker := time.NewTicker(*configCheckInterval)
		tickerCh = ticker.C
		defer ticker.Stop()
	}

	reload := func() {
		cand, err := mgr.buildCandidate(ctx, configFile)
		if err != nil {
			configReloadErrors.Inc()
			configSuccess.Set(0)
			logger.Errorf("cannot prepare scrape config generation from %q: %s; continuing with generation #%d",
				configFile, err, gen.id)
			mgr.reapRetired()
			return
		}
		newGen, stats, err := mgr.publish(cand)
		if err != nil {
			configReloadErrors.Inc()
			configSuccess.Set(0)
			logger.Errorf("cannot publish scrape config generation from %q: %s; continuing with generation #%d",
				configFile, err, gen.id)
			mgr.reapRetired()
			return
		}
		configSuccess.Set(1)
		if stats.idempotent {
			logger.Infof("scrape config generation digest unchanged in %q", configFile)
			mgr.reapRetired()
			return
		}
		if !newGen.acquire() {
			logger.Errorf("newly published scrape config generation was retired before service discovery routines switched to it")
			return
		}
		// updateConfig blocks until every discovery loop has received the new Config,
		// so after this point no loop may use the previous generation anymore.
		scs.updateConfig(newGen.cfg)
		gen.release()
		gen = newGen
		marshaledData = gen.cfg.marshal()
		configData.Store(&marshaledData)
		configReloads.Inc()
		configTimestamp.Set(fasttime.UnixTimestamp())
		mgr.reapRetired()
	}

	for {
		select {
		case <-globalStopCh:
			scs.stop()
			gen.release()
			mgr.stop()
			mgr.reapRetired()
			return
		case <-sighupCh:
			logger.Infof("SIGHUP received; reloading scrape config generation from %q", configFile)
			reload()
		case <-tickerCh:
			reload()
		}
	}
}
