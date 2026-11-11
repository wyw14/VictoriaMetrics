package promscrape

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promrelabel"
	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
	"github.com/cespare/xxhash/v2"
)

// MetricRelabelCollisionAudit audits a bounded Prometheus scrape response against an explicit
// ScrapeWork configuration and reports collision groups: distinct original series mapped onto
// the same final complete labelset by the honor_labels / extra labels / metric relabel pipeline.
//
// The audit reuses the real production building blocks (appendLabels, appendExtraLabels,
// promrelabel.FinalizeLabels and ParsedConfigs.ApplyDebug) and the real Prometheus parser,
// but never pushes data, emits staleness markers, updates the series limiter or the last scrape
// state of the real scrapeWork. See AuditBoundaries in CollisionAuditReport.
//
// Collision detection is always performed by exact comparison of the canonical (sorted, order
// independent) final labelset strings. Hashes are used for bucketing only.

var (
	// ErrAuditSeriesLimitExceeded is returned when the audited response contains more unique
	// original series than CollisionAuditOptions.MaxSeries.
	ErrAuditSeriesLimitExceeded = errors.New("metric relabel collision audit: max series limit exceeded")

	// ErrAuditResponseSizeLimitExceeded is returned when the audited response exceeds
	// CollisionAuditOptions.MaxResponseBytes.
	ErrAuditResponseSizeLimitExceeded = errors.New("metric relabel collision audit: max response size limit exceeded")
)

const (
	defaultAuditMaxResponseBytes     = 64 << 20 // 64 MiB
	defaultAuditMaxSeries            = 100_000
	defaultAuditMaxGroups            = 100
	defaultAuditMaxMembersPerGroup   = 20
	defaultAuditMaxDroppedExamples   = 100
	defaultAuditMaxErrorExamples     = 100
	defaultAuditMaxTimestampExamples = 10

	auditRuleScraped         = "scraped labels"
	auditRuleRemoveEmpty     = "remove empty labels"
	auditRuleFinalizeLabels  = "remove labels with __ prefix except of __name__"
	auditRuleExternalLabels  = "add external labels"
	auditRuleMergeLabelsFmt  = "merge target labels (honor_labels=%v)"
	auditRuleLabelLimitFmt   = "label_limit exceeded (limit=%d)"
	auditRedactedURLFallback = "[unparseable-scrape-url-redacted]"
)

// CollisionAuditOptions contains limits and output truncation settings for the collision audit.
//
// Zero values are replaced with the defaults documented on each field.
type CollisionAuditOptions struct {
	// MaxResponseBytes is the maximum accepted size of the decompressed scrape response.
	// Overflow is rejected with ErrAuditResponseSizeLimitExceeded instead of being spilled.
	MaxResponseBytes int64

	// MaxSeries is the maximum number of unique original series accepted by the audit.
	// Overflow is rejected with ErrAuditSeriesLimitExceeded.
	MaxSeries int

	// MaxGroups limits the number of collision groups included in the report.
	MaxGroups int

	// MaxMembersPerGroup limits the number of member examples per collision group.
	MaxMembersPerGroup int

	// MaxDroppedExamples limits the number of dropped series examples in the report.
	MaxDroppedExamples int

	// MaxParseErrorExamples limits the number of parse error examples in the report.
	MaxParseErrorExamples int

	// MaxTimestampExamples limits the number of timestamp examples kept per original series.
	MaxTimestampExamples int

	// hashFunc buckets canonical final labelsets. It must never be used as the collision
	// criterion itself: entries in the same bucket are always compared exactly.
	// It is overridable in tests to force artificial hash collisions.
	hashFunc func(canonicalLabels string) uint64
}

func (opts *CollisionAuditOptions) withDefaults() *CollisionAuditOptions {
	if opts == nil {
		opts = &CollisionAuditOptions{}
	}
	clone := *opts
	if clone.MaxResponseBytes <= 0 {
		clone.MaxResponseBytes = defaultAuditMaxResponseBytes
	}
	if clone.MaxSeries <= 0 {
		clone.MaxSeries = defaultAuditMaxSeries
	}
	if clone.MaxGroups <= 0 {
		clone.MaxGroups = defaultAuditMaxGroups
	}
	if clone.MaxMembersPerGroup <= 0 {
		clone.MaxMembersPerGroup = defaultAuditMaxMembersPerGroup
	}
	if clone.MaxDroppedExamples <= 0 {
		clone.MaxDroppedExamples = defaultAuditMaxDroppedExamples
	}
	if clone.MaxParseErrorExamples <= 0 {
		clone.MaxParseErrorExamples = defaultAuditMaxErrorExamples
	}
	if clone.MaxTimestampExamples <= 0 {
		clone.MaxTimestampExamples = defaultAuditMaxTimestampExamples
	}
	if clone.hashFunc == nil {
		clone.hashFunc = defaultAuditHash
	}
	return &clone
}

// CollisionAuditReport is the JSON-marshalable result of a collision audit.
type CollisionAuditReport struct {
	// ScrapeURLRedacted is the target URL with any userinfo password removed.
	ScrapeURLRedacted string `json:"scrape_url_redacted"`

	// Tenant is the tenant routing scope of the audited ScrapeWork (accountID[:projectID]
	// or "multitenant"). Routing itself is never performed by the audit.
	Tenant string `json:"tenant"`

	// HonorLabels mirrors ScrapeWork.HonorLabels used during the audited pipeline.
	HonorLabels bool `json:"honor_labels"`

	// Complete is false when at least one response line failed to parse. In that case
	// the audit must not be treated as a full picture of the response.
	Complete bool `json:"complete"`

	Stats           AuditStats        `json:"stats"`
	CollisionGroups []CollisionGroup  `json:"collision_groups,omitempty"`
	DroppedSeries   DroppedSummary    `json:"dropped_series"`
	ParseErrors     ParseErrorSummary `json:"parse_errors"`
	Boundaries      AuditBoundaries   `json:"boundaries"`
	Truncated       AuditTruncation   `json:"truncated"`
}

// AuditStats contains aggregate counters for the audited response.
type AuditStats struct {
	// RowsRead is the number of successfully parsed rows, including repeated samples
	// of the same original series.
	RowsRead int `json:"rows_read"`

	// UniqueOriginalSeries is the number of distinct scraped identities (metric name plus
	// labels, label order independent).
	UniqueOriginalSeries int `json:"unique_original_series"`

	// SeriesWithMultipleSamples is the number of original series seen with more than one
	// row (for example several timestamps). Such repetitions are not collisions.
	SeriesWithMultipleSamples int `json:"series_with_multiple_samples"`

	// SurvivingSeries is the number of original series that reach a non-empty final labelset.
	SurvivingSeries int `json:"surviving_series"`

	// DroppedSeries is the number of original series removed by relabel rules, finalization
	// or label_limit.
	DroppedSeries int `json:"dropped_series"`

	// CollisionGroups is the total number of collision groups before output truncation.
	CollisionGroups int `json:"collision_groups"`

	// SeriesInCollisions is the total number of original series participating in a
	// collision group before output truncation.
	SeriesInCollisions int `json:"series_in_collisions"`
}

// CollisionGroup describes distinct original series mapped to the same final labelset.
type CollisionGroup struct {
	// FinalLabels is the canonical complete final labelset shared by all group members.
	FinalLabels string `json:"final_labels"`

	// Size is the number of distinct original series in the group.
	Size int `json:"size"`

	// FirstCollisionStep is the earliest pipeline stage at which any two members of the
	// group become indistinguishable.
	FirstCollisionStep StepRef `json:"first_collision_step"`

	// Members contains up to CollisionAuditOptions.MaxMembersPerGroup member examples.
	Members []CollisionMember `json:"members"`

	// MembersTruncated is true if additional members exist beyond Members.
	MembersTruncated bool `json:"members_truncated"`
}

// CollisionMember is a single original series participating in a CollisionGroup.
type CollisionMember struct {
	// OriginalLabels is the canonical identity of the scraped series before any target
	// label merge or relabeling.
	OriginalLabels string `json:"original_labels"`

	// SampleCount is the number of rows (samples/timestamps) observed for the identity.
	SampleCount int `json:"sample_count"`

	// Timestamps contains a bounded number of observed timestamp examples.
	Timestamps []int64 `json:"timestamps,omitempty"`

	// FirstCollisionStep is the earliest pipeline stage at which this member becomes
	// indistinguishable from another member of the same group.
	FirstCollisionStep StepRef `json:"first_collision_step"`
}

// StepRef references a pipeline stage by its aligned index and rule description.
type StepRef struct {
	// StageIndex is the zero-based aligned stage index. The stage layout is identical
	// for every surviving series of the same audit run.
	StageIndex int `json:"stage_index"`

	// Rule is the description of the rule or fixed pipeline stage producing the labelset.
	// Only rule text and stage names are exposed; intermediate label values hidden by the
	// configuration are never serialized.
	Rule string `json:"rule"`
}

// DroppedSummary aggregates series removed before reaching a final labelset.
type DroppedSummary struct {
	Total     int                 `json:"total"`
	Examples  []DroppedSeriesInfo `json:"examples,omitempty"`
	Truncated bool                `json:"truncated"`
}

// DroppedSeriesInfo describes one dropped original series example.
type DroppedSeriesInfo struct {
	OriginalLabels string  `json:"original_labels"`
	SampleCount    int     `json:"sample_count"`
	DropStep       StepRef `json:"drop_step"`
}

// ParseErrorSummary aggregates response lines rejected by the Prometheus parser.
type ParseErrorSummary struct {
	Total     int          `json:"total"`
	Examples  []ParseError `json:"examples,omitempty"`
	Truncated bool         `json:"truncated"`
}

// ParseError references one invalid response line.
type ParseError struct {
	// LineNumber is the 1-based line number in the response, when known.
	LineNumber int `json:"line_number"`

	// Message is the error produced by the existing Prometheus parser.
	Message string `json:"message"`
}

// AuditBoundaries states the side-effect boundaries of the audit.
//
// All fields are always false by construction: the audit neither performs remote writes nor
// touches the mutable state of the real scrapeWork.
type AuditBoundaries struct {
	RemoteWritePerformed   bool `json:"remote_write_performed"`
	StaleMarkersEmitted    bool `json:"stale_markers_emitted"`
	SeriesLimiterUpdated   bool `json:"series_limiter_updated"`
	LastScrapeUpdated      bool `json:"last_scrape_updated"`
	AutoMetricsIncluded    bool `json:"auto_metrics_included"`
	TenantRoutingPerformed bool `json:"tenant_routing_performed"`
}

// AuditTruncation indicates which sections of CollisionAuditReport were truncated by options.
type AuditTruncation struct {
	Groups        bool `json:"groups"`
	GroupMembers  bool `json:"group_members"`
	DroppedSeries bool `json:"dropped_series"`
	ParseErrors   bool `json:"parse_errors"`
}

// AuditMetricRelabelCollisions runs the collision audit over a streaming Prometheus scrape
// response. The reader is expected to provide already decompressed exposition text.
//
// The response is consumed line by line with bounded memory. When MaxResponseBytes or
// MaxSeries are exceeded, the function returns the corresponding sentinel error and no report:
// over-sized input is explicitly rejected instead of silently truncated or spilled.
func AuditMetricRelabelCollisions(cfg *ScrapeWork, r io.Reader, opts *CollisionAuditOptions) (*CollisionAuditReport, error) {
	if cfg == nil {
		return nil, errors.New("metric relabel collision audit: cfg cannot be nil")
	}
	if r == nil {
		return nil, errors.New("metric relabel collision audit: reader cannot be nil")
	}
	options := opts.withDefaults()
	p := newCollisionAuditProcessor(cfg, options)
	if err := p.processReader(r); err != nil {
		return nil, err
	}
	return p.finalizeReport(), nil
}

// processReader consumes the response line by line with bounded memory. Over-sized input is
// explicitly rejected via a sentinel error instead of being silently truncated or spilled.
func (p *collisionAuditProcessor) processReader(r io.Reader) error {
	// Hard-cap the amount of data buffered by the line reader, then verify the exact byte
	// count while consuming lines.
	lr := io.LimitReader(r, p.opts.MaxResponseBytes+1)
	cr := &countingReader{r: lr}
	br := bufio.NewReaderSize(cr, 64*1024)

	lineNumber := 0
	for {
		line, readErr := br.ReadBytes('\n')
		if len(line) > 0 {
			lineNumber++
			if cr.n > p.opts.MaxResponseBytes {
				return ErrAuditResponseSizeLimitExceeded
			}
			if err := p.processLine(line, lineNumber); err != nil {
				return err
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return fmt.Errorf("metric relabel collision audit: cannot read response: %w", readErr)
		}
	}
	if cr.n > p.opts.MaxResponseBytes {
		return ErrAuditResponseSizeLimitExceeded
	}
	return nil
}

// AuditMetricRelabelCollisionsFromString runs the collision audit over an in-memory Prometheus
// scrape response. It applies the same bounded pipeline as AuditMetricRelabelCollisions.
func AuditMetricRelabelCollisionsFromString(cfg *ScrapeWork, body string, opts *CollisionAuditOptions) (*CollisionAuditReport, error) {
	if cfg == nil {
		return nil, errors.New("metric relabel collision audit: cfg cannot be nil")
	}
	return AuditMetricRelabelCollisions(cfg, strings.NewReader(body), opts)
}

type countingReader struct {
	r io.Reader
	n int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.n += int64(n)
	return n, err
}

type collisionAuditProcessor struct {
	cfg  *ScrapeWork
	opts *CollisionAuditOptions

	// entries keeps one audited identity keyed by its canonical original labelset.
	entries map[string]*auditEntry
	// entryOrder preserves first-seen order of all identities, including dropped ones.
	entryOrder []*auditEntry

	parseErrorTotal int
	parseErrors     []ParseError
}

// auditEntry is the per-original-series audit state.
type auditEntry struct {
	seq         int
	originalKey string
	sampleCount int
	timestamps  []int64
	dropped     bool
	dropStep    StepRef
	finalKey    string
	stages      []string
	stageRules  []string
}

func newCollisionAuditProcessor(cfg *ScrapeWork, opts *CollisionAuditOptions) *collisionAuditProcessor {
	return &collisionAuditProcessor{
		cfg:     cfg,
		opts:    opts,
		entries: make(map[string]*auditEntry),
	}
}

// processLine parses a single response line with the existing Prometheus parser, so bad lines
// follow exactly the same error semantics as the production one-shot parser.
func (p *collisionAuditProcessor) processLine(line []byte, lineNumber int) error {
	line = bytes.TrimSuffix(line, []byte{'\n'})
	if len(bytes.TrimSpace(line)) == 0 {
		return nil
	}
	var rows parser.Rows
	rows.UnmarshalWithErrLogger(bytesutil.ToUnsafeString(line), func(msg string) {
		p.recordParseError(lineNumber, msg)
	})
	for i := range rows.Rows {
		if err := p.processRow(&rows.Rows[i]); err != nil {
			return err
		}
	}
	return nil
}

func (p *collisionAuditProcessor) recordParseError(lineNumber int, msg string) {
	p.parseErrorTotal++
	if len(p.parseErrors) < p.opts.MaxParseErrorExamples {
		p.parseErrors = append(p.parseErrors, ParseError{
			LineNumber: lineNumber,
			Message:    msg,
		})
	}
}

// processRow deduplicates repeated samples of the same original series and audits new identities.
func (p *collisionAuditProcessor) processRow(r *parser.Row) error {
	originalKey := canonicalRowIdentity(r)
	if e := p.entries[originalKey]; e != nil {
		// The same original series may appear several times (for example with several
		// timestamps). This is repetition, never a collision.
		e.sampleCount++
		if len(e.timestamps) < p.opts.MaxTimestampExamples {
			e.timestamps = append(e.timestamps, r.Timestamp)
		}
		return nil
	}
	if len(p.entries) >= p.opts.MaxSeries {
		return ErrAuditSeriesLimitExceeded
	}
	e := p.buildEntry(r, originalKey)
	p.entries[originalKey] = e
	p.entryOrder = append(p.entryOrder, e)
	return nil
}

// buildEntry mirrors the label pipeline of writeRequestCtx.addRow for needRelabel=true.
//
// It deliberately uses the same building blocks (appendLabels, appendExtraLabels,
// FinalizeLabels and ParsedConfigs.ApplyDebug) instead of re-implementing their semantics.
func (p *collisionAuditProcessor) buildEntry(r *parser.Row, originalKey string) *auditEntry {
	cfg := p.cfg
	e := &auditEntry{
		seq:         len(p.entryOrder),
		originalKey: originalKey,
		sampleCount: 1,
		timestamps:  append([]int64(nil), r.Timestamp),
	}

	metric := r.Metric
	// Mirror the exported_ prefix handling of addRow for metric names clashing with the
	// automatically generated scrape metrics when honor_labels is disabled.
	if !cfg.HonorLabels && len(r.Tags) == 0 && isAutoMetric(metric) {
		metric = "exported_" + metric
	}

	// Stage 1: merge __name__, scraped tags and target labels according to honor_labels.
	var labels []prompb.Label
	labels = appendLabels(labels, metric, r.Tags, cfg.Labels.GetLabels(), cfg.HonorLabels)
	mergedKey := promrelabel.LabelsToString(labels)

	// Stages 2..2+n-1: metric relabel rules. ApplyDebug records one canonical snapshot per
	// rule for this single series; the snapshots are reused later only to locate the earliest
	// distinguishability loss, never to decide collisions.
	relabeled, dss := cfg.MetricRelabelConfigs.ApplyDebug(labels)
	ruleSteps, removeEmptyStep := splitDebugSteps(dss)

	stages := make([]string, 0, len(ruleSteps)+5)
	stageRules := make([]string, 0, len(ruleSteps)+5)
	stages = append(stages, originalKey)
	stageRules = append(stageRules, auditRuleScraped)
	stages = append(stages, mergedKey)
	stageRules = append(stageRules, fmt.Sprintf(auditRuleMergeLabelsFmt, cfg.HonorLabels))
	for _, ds := range ruleSteps {
		stages = append(stages, ds.Out)
		stageRules = append(stageRules, ds.Rule)
	}
	// Conditional "remove empty labels" stage, aligned to a fixed position for every series.
	if removeEmptyStep != nil {
		stages = append(stages, removeEmptyStep.Out)
	} else if len(ruleSteps) > 0 {
		stages = append(stages, ruleSteps[len(ruleSteps)-1].Out)
	} else {
		stages = append(stages, mergedKey)
	}
	stageRules = append(stageRules, auditRuleRemoveEmpty)

	if len(relabeled) == 0 {
		// A drop/keep-like rule removed every label. ApplyDebug returns early, so the last
		// rule step is precisely the dropping rule.
		dropRule := auditRuleRemoveEmpty
		dropStage := len(stages) - 1
		if len(ruleSteps) > 0 {
			dropRule = ruleSteps[len(ruleSteps)-1].Rule
			dropStage = 1 + len(ruleSteps)
		}
		e.dropped = true
		e.dropStep = StepRef{StageIndex: dropStage, Rule: dropRule}
		e.stages = stages
		e.stageRules = stageRules
		return e
	}

	// Finalization stage: drop __-prefixed labels except __name__.
	finalized := promrelabel.FinalizeLabels(nil, relabeled)
	if len(finalized) == 0 {
		stages = append(stages, promrelabel.LabelsToString(finalized))
		stageRules = append(stageRules, auditRuleFinalizeLabels)
		e.dropped = true
		e.dropStep = StepRef{StageIndex: len(stages) - 1, Rule: auditRuleFinalizeLabels}
		e.stages = stages
		e.stageRules = stageRules
		return e
	}
	stages = append(stages, promrelabel.LabelsToString(finalized))
	stageRules = append(stageRules, auditRuleFinalizeLabels)

	// External labels stage, after relabeling and finalization, exactly like addRow.
	withExternal := appendExtraLabels(finalized, cfg.ExternalLabels.GetLabels(), 0, cfg.HonorLabels)
	if cfg.LabelLimit > 0 && len(withExternal) > cfg.LabelLimit {
		stageRules = append(stageRules, fmt.Sprintf(auditRuleLabelLimitFmt, cfg.LabelLimit))
		e.dropped = true
		e.dropStep = StepRef{StageIndex: len(stages), Rule: fmt.Sprintf(auditRuleLabelLimitFmt, cfg.LabelLimit)}
		e.stages = stages
		e.stageRules = stageRules
		return e
	}
	finalKey := promrelabel.LabelsToString(withExternal)
	stages = append(stages, finalKey)
	stageRules = append(stageRules, auditRuleExternalLabels)

	e.finalKey = finalKey
	e.stages = stages
	e.stageRules = stageRules
	return e
}

// splitDebugSteps separates per-rule debug steps from the trailing "remove empty labels"
// maintenance step emitted by ParsedConfigs.ApplyDebug.
func splitDebugSteps(dss []promrelabel.DebugStep) (ruleSteps []promrelabel.DebugStep, removeEmptyStep *promrelabel.DebugStep) {
	if n := len(dss); n > 0 && dss[n-1].Rule == auditRuleRemoveEmpty {
		return dss[:n-1], &dss[n-1]
	}
	return dss, nil
}

// canonicalRowIdentity returns the order-independent canonical identity of a scraped row.
func canonicalRowIdentity(r *parser.Row) string {
	labels := make([]prompb.Label, 0, len(r.Tags)+1)
	labels = append(labels, prompb.Label{Name: "__name__", Value: r.Metric})
	for i := range r.Tags {
		t := &r.Tags[i]
		labels = append(labels, prompb.Label{Name: t.Key, Value: t.Value})
	}
	return promrelabel.LabelsToString(labels)
}

func defaultAuditHash(canonicalLabels string) uint64 {
	return xxhash.Sum64(bytesutil.ToUnsafeBytes(canonicalLabels))
}

func (p *collisionAuditProcessor) finalizeReport() *CollisionAuditReport {
	groups, stats := p.buildCollisionGroups()

	droppedExamples := make([]DroppedSeriesInfo, 0)
	droppedTruncated := false
	for _, e := range p.entryOrder {
		if !e.dropped {
			continue
		}
		if len(droppedExamples) >= p.opts.MaxDroppedExamples {
			droppedTruncated = true
			break
		}
		droppedExamples = append(droppedExamples, DroppedSeriesInfo{
			OriginalLabels: e.originalKey,
			SampleCount:    e.sampleCount,
			DropStep:       e.dropStep,
		})
	}

	groupsTruncated := false
	if len(groups) > p.opts.MaxGroups {
		groupsTruncated = true
		groups = groups[:p.opts.MaxGroups]
	}
	membersTruncated := false
	for i := range groups {
		if groups[i].MembersTruncated {
			membersTruncated = true
			break
		}
	}

	return &CollisionAuditReport{
		ScrapeURLRedacted: redactScrapeURL(p.cfg.ScrapeURL),
		Tenant:            p.cfg.AuthToken.String(),
		HonorLabels:       p.cfg.HonorLabels,
		Complete:          p.parseErrorTotal == 0,
		Stats:             stats,
		CollisionGroups:   groups,
		DroppedSeries: DroppedSummary{
			Total:     stats.DroppedSeries,
			Examples:  droppedExamples,
			Truncated: droppedTruncated,
		},
		ParseErrors: ParseErrorSummary{
			Total:     p.parseErrorTotal,
			Examples:  p.parseErrors,
			Truncated: p.parseErrorTotal > len(p.parseErrors),
		},
		// Every boundary is false by construction: the audit never reaches PushData,
		// stale marker generation, the bloomfilter limiter, last scrape storage,
		// auto metric generation or tenant routing.
		Boundaries: AuditBoundaries{},
		Truncated: AuditTruncation{
			Groups:        groupsTruncated,
			GroupMembers:  membersTruncated,
			DroppedSeries: droppedTruncated,
			ParseErrors:   p.parseErrorTotal > len(p.parseErrors),
		},
	}
}

// buildCollisionGroups buckets surviving series by hash and then confirms membership with
// exact canonical labelset comparison.
func (p *collisionAuditProcessor) buildCollisionGroups() ([]CollisionGroup, AuditStats) {
	stats := AuditStats{
		RowsRead:             p.countRows(),
		UniqueOriginalSeries: len(p.entryOrder),
	}
	buckets := make(map[uint64][]*auditEntry)
	survivors := make([]*auditEntry, 0, len(p.entryOrder))
	multiSample := 0
	for _, e := range p.entryOrder {
		if e.sampleCount > 1 {
			multiSample++
		}
		if e.dropped {
			stats.DroppedSeries++
			continue
		}
		survivors = append(survivors, e)
		h := p.opts.hashFunc(e.finalKey)
		buckets[h] = append(buckets[h], e)
	}
	stats.SeriesWithMultipleSamples = multiSample
	stats.SurvivingSeries = len(survivors)

	var groups []CollisionGroup
	for _, bucket := range buckets {
		// Exact comparison inside the hash bucket. A hash collision can never merge
		// distinct canonical labelsets here.
		byKey := make(map[string][]*auditEntry)
		keyOrder := make([]string, 0)
		for _, e := range bucket {
			if _, ok := byKey[e.finalKey]; !ok {
				keyOrder = append(keyOrder, e.finalKey)
			}
			byKey[e.finalKey] = append(byKey[e.finalKey], e)
		}
		for _, key := range keyOrder {
			members := byKey[key]
			if len(members) < 2 {
				continue
			}
			groups = append(groups, p.makeCollisionGroup(key, members))
		}
	}

	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Size != groups[j].Size {
			return groups[i].Size > groups[j].Size
		}
		return groups[i].FinalLabels < groups[j].FinalLabels
	})
	stats.CollisionGroups = len(groups)
	for _, g := range groups {
		stats.SeriesInCollisions += g.Size
	}
	return groups, stats
}

func (p *collisionAuditProcessor) countRows() int {
	n := 0
	for _, e := range p.entryOrder {
		n += e.sampleCount
	}
	return n
}

func (p *collisionAuditProcessor) makeCollisionGroup(finalKey string, members []*auditEntry) CollisionGroup {
	sort.Slice(members, func(i, j int) bool {
		return members[i].seq < members[j].seq
	})
	g := CollisionGroup{
		FinalLabels: finalKey,
		Size:        len(members),
	}
	if len(members) > p.opts.MaxMembersPerGroup {
		g.MembersTruncated = true
	}
	exampleMembers := members
	if g.MembersTruncated {
		exampleMembers = members[:p.opts.MaxMembersPerGroup]
	}
	groupEarliest := StepRef{StageIndex: int(^uint(0) >> 1)}
	for _, m := range exampleMembers {
		step := p.earliestCollisionStep(m, members)
		if step.StageIndex < groupEarliest.StageIndex {
			groupEarliest = step
		}
		g.Members = append(g.Members, CollisionMember{
			OriginalLabels:     m.originalKey,
			SampleCount:        m.sampleCount,
			Timestamps:         m.timestamps,
			FirstCollisionStep: step,
		})
	}
	g.FirstCollisionStep = groupEarliest
	return g
}

// earliestCollisionStep returns the first aligned stage where member's canonical labelset
// equals that of any other group member.
//
// The pipeline is a deterministic function applied identically to every series, so once two
// labelsets become equal they stay equal at every later stage.
func (p *collisionAuditProcessor) earliestCollisionStep(member *auditEntry, others []*auditEntry) StepRef {
	for stageIdx, canonical := range member.stages {
		for _, other := range others {
			if other == member {
				continue
			}
			if stageIdx < len(other.stages) && other.stages[stageIdx] == canonical {
				rule := member.stageRules[stageIdx]
				return StepRef{StageIndex: stageIdx, Rule: rule}
			}
		}
	}
	// Unreachable for surviving group members: their final canonical labelset is equal
	// by construction. Fall back to the final stage instead of panicking.
	return StepRef{
		StageIndex: len(member.stages) - 1,
		Rule:       member.stageRules[len(member.stageRules)-1],
	}
}

// redactScrapeURL removes userinfo credentials from the target URL. Authentication headers
// are not part of the audit output at all.
func redactScrapeURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return auditRedactedURLFallback
	}
	return u.Redacted()
}
