package promscrape

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promrelabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/prometheus"
)

func auditTestCfg(relabelConfigs string, modify func(*ScrapeWork)) *ScrapeWork {
	cfg := &ScrapeWork{
		ScrapeURL: "http://localhost:8429/metrics",
		Labels: promutil.NewLabelsFromMap(map[string]string{
			"job":      "test",
			"instance": "localhost:8429",
		}),
		MetricRelabelConfigs: mustParseRelabelConfigs(relabelConfigs),
	}
	if modify != nil {
		modify(cfg)
	}
	return cfg
}

func mustAudit(t *testing.T, cfg *ScrapeWork, body string, opts *CollisionAuditOptions) *CollisionAuditReport {
	t.Helper()
	report, err := AuditMetricRelabelCollisionsFromString(cfg, body, opts)
	if err != nil {
		t.Fatalf("unexpected audit error: %s", err)
	}
	return report
}

// mustAuditDetailed additionally exposes the internal processor for white-box assertions.
func mustAuditDetailed(t *testing.T, cfg *ScrapeWork, body string, opts *CollisionAuditOptions) (*CollisionAuditReport, *collisionAuditProcessor) {
	t.Helper()
	options := opts.withDefaults()
	p := newCollisionAuditProcessor(cfg, options)
	if err := p.processReader(strings.NewReader(body)); err != nil {
		t.Fatalf("unexpected audit error: %s", err)
	}
	return p.finalizeReport(), p
}

func findCollisionGroup(t *testing.T, report *CollisionAuditReport, finalLabels string) *CollisionGroup {
	t.Helper()
	for i := range report.CollisionGroups {
		g := &report.CollisionGroups[i]
		if g.FinalLabels == finalLabels {
			return g
		}
	}
	t.Fatalf("cannot find collision group for %q; got groups: %#v", finalLabels, report.CollisionGroups)
	return nil
}

// TestMetricRelabelCollisionAuditLabelDrop verifies a labeldrop rule collapsing two distinct
// original series onto the same final labelset, including the earliest collision rule.
func TestMetricRelabelCollisionAuditLabelDrop(t *testing.T) {
	cfg := auditTestCfg(`
- action: labeldrop
  regex: id
`, nil)
	body := `
foo{id="1"} 1
foo{id="2"} 2
`
	report := mustAudit(t, cfg, body, nil)
	if !report.Complete {
		t.Fatalf("unexpected parse errors: %#v", report.ParseErrors)
	}
	if report.Stats.CollisionGroups != 1 || report.Stats.SeriesInCollisions != 2 {
		t.Fatalf("unexpected stats: %#v", report.Stats)
	}
	g := findCollisionGroup(t, report, `foo{instance="localhost:8429",job="test"}`)
	if g.Size != 2 || len(g.Members) != 2 || g.MembersTruncated {
		t.Fatalf("unexpected group shape: %#v", g)
	}
	wantOriginals := map[string]bool{
		`foo{id="1"}`: false,
		`foo{id="2"}`: false,
	}
	for _, m := range g.Members {
		if _, ok := wantOriginals[m.OriginalLabels]; !ok {
			t.Fatalf("unexpected member %q", m.OriginalLabels)
		}
		wantOriginals[m.OriginalLabels] = true
		if m.FirstCollisionStep.StageIndex != 2 || !strings.Contains(m.FirstCollisionStep.Rule, "labeldrop") {
			t.Fatalf("unexpected first collision step: %#v", m.FirstCollisionStep)
		}
	}
	for k, found := range wantOriginals {
		if !found {
			t.Fatalf("missing member %q", k)
		}
	}
	if g.FirstCollisionStep.StageIndex != 2 || !strings.Contains(g.FirstCollisionStep.Rule, "labeldrop") {
		t.Fatalf("unexpected group first collision step: %#v", g.FirstCollisionStep)
	}
}

// TestMetricRelabelCollisionAuditLabelMap verifies the realistic labelmap collision pattern:
// labelmap copies distinct label names to the same target name while retaining the sources
// (Prometheus semantics), so the series only become indistinguishable once a follow-up
// labeldrop removes the source labels.
func TestMetricRelabelCollisionAuditLabelMap(t *testing.T) {
	cfg := auditTestCfg(`
- action: labelmap
  regex: "^(a|b)$"
  replacement: x
- action: labeldrop
  regex: "^(a|b)$"
`, nil)
	body := `
foo{a="1"} 1
foo{b="1"} 2
`
	report := mustAudit(t, cfg, body, nil)
	if report.Stats.CollisionGroups != 1 {
		t.Fatalf("unexpected stats: %#v", report.Stats)
	}
	g := findCollisionGroup(t, report, `foo{instance="localhost:8429",job="test",x="1"}`)
	if g.Size != 2 {
		t.Fatalf("unexpected group size: %d", g.Size)
	}
	for _, m := range g.Members {
		// labelmap alone retains the distinct source labels; convergence happens at the
		// second rule (stage 3), not at labelmap itself (stage 2).
		if m.FirstCollisionStep.StageIndex != 3 || !strings.Contains(m.FirstCollisionStep.Rule, "labeldrop") {
			t.Fatalf("unexpected first collision step: %#v", m.FirstCollisionStep)
		}
	}
}

// TestMetricRelabelCollisionAuditLabelMapFinalize verifies labelmap onto a plain name followed
// by finalization of the __-prefixed source labels also yields an attributable collision.
func TestMetricRelabelCollisionAuditLabelMapFinalize(t *testing.T) {
	cfg := auditTestCfg(`
- action: labelmap
  regex: "^__(a|b)$"
  replacement: x
`, func(c *ScrapeWork) {
		c.Labels = promutil.NewLabelsFromMap(map[string]string{"job": "test"})
	})
	body := `
foo{__a="1"} 1
foo{__b="1"} 2
`
	report := mustAudit(t, cfg, body, nil)
	g := findCollisionGroup(t, report, `foo{job="test",x="1"}`)
	if g.Size != 2 {
		t.Fatalf("unexpected group size: %d", g.Size)
	}
	for _, m := range g.Members {
		// Stages: scraped(0), merge(1), labelmap(2), removeEmpty(3), finalize(4), external(5).
		if m.FirstCollisionStep.StageIndex != 4 || m.FirstCollisionStep.Rule != auditRuleFinalizeLabels {
			t.Fatalf("unexpected first collision step: %#v", m.FirstCollisionStep)
		}
	}
}

// TestMetricRelabelCollisionAuditReplace verifies a constant replace rule converges series.
func TestMetricRelabelCollisionAuditReplace(t *testing.T) {
	cfg := auditTestCfg(`
- target_label: id
  replacement: const
`, nil)
	body := `
foo{id="a"} 1
foo{id="b"} 2
`
	report := mustAudit(t, cfg, body, nil)
	g := findCollisionGroup(t, report, `foo{id="const",instance="localhost:8429",job="test"}`)
	if g.Size != 2 {
		t.Fatalf("unexpected group size: %d", g.Size)
	}
	for _, m := range g.Members {
		// The default replace action is omitted from the serialized rule YAML, so assert
		// on the aligned stage index and the target_label reference instead.
		if m.FirstCollisionStep.StageIndex != 2 || !strings.Contains(m.FirstCollisionStep.Rule, "target_label: id") {
			t.Fatalf("unexpected first collision step: %#v", m.FirstCollisionStep)
		}
	}
}

// TestMetricRelabelCollisionAuditHonorLabels covers both honor_labels modes and verifies that
// exported_ renaming, target label skipping and rule-attributed convergence match addRow.
func TestMetricRelabelCollisionAuditHonorLabels(t *testing.T) {
	body := `
foo{env="dev",id="1"} 1
foo{env="qa",id="2"} 2
`
	// honor_labels=false: clashing env is preserved as exported_env, so dropping id alone
	// must not yet collapse the series. The second rule removes exported_env.
	cfgNoHonor := auditTestCfg(`
- action: labeldrop
  regex: id
- action: labeldrop
  regex: exported_env
`, func(c *ScrapeWork) {
		c.HonorLabels = false
		c.Labels = promutil.NewLabelsFromMap(map[string]string{
			"job": "test",
			"env": "prod",
		})
	})
	report := mustAudit(t, cfgNoHonor, body, nil)
	g := findCollisionGroup(t, report, `foo{env="prod",job="test"}`)
	if g.Size != 2 {
		t.Fatalf("unexpected group size: %d", g.Size)
	}
	for _, m := range g.Members {
		// Stage 0 scraped, 1 merge, 2 first rule, 3 second rule.
		if m.FirstCollisionStep.StageIndex != 3 {
			t.Fatalf("unexpected collision stage: %#v", m.FirstCollisionStep)
		}
	}

	// honor_labels=true: scraped env wins and the target env is skipped, so the same rules
	// cannot collapse the series yet.
	cfgHonor := auditTestCfg(`
- action: labeldrop
  regex: id
- action: labeldrop
  regex: exported_env
- action: labeldrop
  regex: env
`, func(c *ScrapeWork) {
		c.HonorLabels = true
		c.Labels = promutil.NewLabelsFromMap(map[string]string{
			"job": "test",
			"env": "prod",
		})
	})
	reportHonor := mustAudit(t, cfgHonor, body, nil)
	gHonor := findCollisionGroup(t, reportHonor, `foo{job="test"}`)
	if gHonor.Size != 2 {
		t.Fatalf("unexpected group size: %d", gHonor.Size)
	}
	for _, m := range gHonor.Members {
		// Convergence only happens at the third relabel rule (stage 4).
		if m.FirstCollisionStep.StageIndex != 4 || !strings.Contains(m.FirstCollisionStep.Rule, "labeldrop") {
			t.Fatalf("unexpected collision step: %#v", m.FirstCollisionStep)
		}
	}
}

// TestMetricRelabelCollisionAuditFinalizeStage verifies that stripping __-prefixed labels
// during finalization can itself be the earliest distinguishability loss.
func TestMetricRelabelCollisionAuditFinalizeStage(t *testing.T) {
	cfg := auditTestCfg("", func(c *ScrapeWork) {
		c.Labels = promutil.NewLabelsFromMap(map[string]string{"job": "test"})
	})
	body := `
foo{__tmp="1"} 1
foo{__tmp="2"} 2
`
	report := mustAudit(t, cfg, body, nil)
	g := findCollisionGroup(t, report, `foo{job="test"}`)
	if g.Size != 2 {
		t.Fatalf("unexpected group size: %d", g.Size)
	}
	// No relabel rules: stages are scraped(0), merge(1), removeEmpty(2), finalize(3), external(4).
	for _, m := range g.Members {
		if m.FirstCollisionStep.StageIndex != 3 || m.FirstCollisionStep.Rule != auditRuleFinalizeLabels {
			t.Fatalf("unexpected collision step: %#v", m.FirstCollisionStep)
		}
	}
}

// TestMetricRelabelCollisionAuditForcedHashCollision proves hashes only select buckets:
// a constant hash forces every series into one bucket without creating false collision groups.
func TestMetricRelabelCollisionAuditForcedHashCollision(t *testing.T) {
	cfg := auditTestCfg(`
- action: labeldrop
  regex: id
`, nil)
	body := `
foo{id="1"} 1
foo{id="2"} 2
bar{id="3"} 3
baz{id="4"} 4
`
	opts := &CollisionAuditOptions{
		hashFunc: func(_ string) uint64 { return 42 },
	}
	reportForced := mustAudit(t, cfg, body, opts)
	reportDefault := mustAudit(t, cfg, body, nil)
	if !reflect.DeepEqual(reportForced.CollisionGroups, reportDefault.CollisionGroups) {
		t.Fatalf("forced hash changed collision detection:\nforced=%#v\ndefault=%#v",
			reportForced.CollisionGroups, reportDefault.CollisionGroups)
	}
	if len(reportForced.CollisionGroups) != 1 || reportForced.CollisionGroups[0].Size != 2 {
		t.Fatalf("unexpected groups under constant hash: %#v", reportForced.CollisionGroups)
	}
}

// TestMetricRelabelCollisionAuditParityWithAddRow cross-checks audit outcomes against the
// complete labels produced by the real production writeRequestCtx.addRow pipeline.
func TestMetricRelabelCollisionAuditParityWithAddRow(t *testing.T) {
	cfg := auditTestCfg(`
- action: labeldrop
  regex: id
- action: drop
  source_labels: [__name__]
  regex: "dropme|ghost"
`, func(c *ScrapeWork) {
		c.HonorLabels = false
		c.Labels = promutil.NewLabelsFromMap(map[string]string{
			"job": "test",
			"env": "prod",
		})
		c.ExternalLabels = promutil.NewLabelsFromMap(map[string]string{
			"cluster": "east",
		})
		c.LabelLimit = 6
	})
	body := `
foo{id="1",zone="a"} 1
foo{id="2",zone="a"} 2
foo{id="1",zone="a"} 3
bar{id="9",zone="b"} 10
dropme 1
ghost{__tmp="x"} 1
up 1
wide{a="1",b="2",c="3",d="4",e="5"} 1
`
	report, proc := mustAuditDetailed(t, cfg, body, nil)

	// Parse the same body and push every row through the real production addRow.
	var rows prometheus.Rows
	rows.UnmarshalWithErrLogger(body, func(s string) {
		t.Fatalf("unexpected parse error: %s", s)
	})
	prodOutcomes := make(map[string]struct {
		finalKey string
		dropped  bool
	})
	for i := range rows.Rows {
		r := &rows.Rows[i]
		key := canonicalRowIdentity(r)
		if _, ok := prodOutcomes[key]; ok {
			continue
		}
		finalKey, dropped := productionRowOutcome(t, cfg, r)
		prodOutcomes[key] = struct {
			finalKey string
			dropped  bool
		}{finalKey: finalKey, dropped: dropped}
	}

	if len(prodOutcomes) != report.Stats.UniqueOriginalSeries {
		t.Fatalf("unique series mismatch: audit=%d production=%d",
			report.Stats.UniqueOriginalSeries, len(prodOutcomes))
	}

	// Per-series parity: audit final labelsets and drop decisions must equal addRow.
	for _, e := range proc.entryOrder {
		outcome, ok := prodOutcomes[e.originalKey]
		if !ok {
			t.Fatalf("audit identity %q missing in production outcomes", e.originalKey)
		}
		if e.dropped != outcome.dropped {
			t.Fatalf("drop decision mismatch for %q: audit=%v production=%v",
				e.originalKey, e.dropped, outcome.dropped)
		}
		if !e.dropped && e.finalKey != outcome.finalKey {
			t.Fatalf("final labelset mismatch for %q:\naudit=%q\nproduction=%q",
				e.originalKey, e.finalKey, outcome.finalKey)
		}
	}

	// Independently rebuild collision groups from production addRow outputs.
	prodGroups := make(map[string]map[string]struct{})
	for key, outcome := range prodOutcomes {
		if outcome.dropped {
			continue
		}
		members, ok := prodGroups[outcome.finalKey]
		if !ok {
			members = make(map[string]struct{})
			prodGroups[outcome.finalKey] = members
		}
		members[key] = struct{}{}
	}
	auditGroupCount := 0
	for _, g := range report.CollisionGroups {
		prodMembers, ok := prodGroups[g.FinalLabels]
		if !ok {
			t.Fatalf("audit group %q has no production counterpart", g.FinalLabels)
		}
		if len(prodMembers) != g.Size {
			t.Fatalf("group size mismatch for %q: audit=%d production=%d",
				g.FinalLabels, g.Size, len(prodMembers))
		}
		for _, m := range g.Members {
			if _, ok := prodMembers[m.OriginalLabels]; !ok {
				t.Fatalf("production group %q does not contain %q", g.FinalLabels, m.OriginalLabels)
			}
		}
		if len(prodMembers) >= 2 {
			auditGroupCount++
		}
	}
	productionGroupCount := 0
	for _, members := range prodGroups {
		if len(members) >= 2 {
			productionGroupCount++
		}
	}
	if auditGroupCount != productionGroupCount {
		t.Fatalf("group count mismatch: audit=%d production=%d", auditGroupCount, productionGroupCount)
	}

	// The repeated foo sample must be counted, not treated as a collision.
	fooKey := `foo{id="1",zone="a"}`
	if e := proc.entries[fooKey]; e == nil || e.sampleCount != 2 {
		t.Fatalf("unexpected repeated-sample handling for %q: %#v", fooKey, e)
	}
	if report.Stats.SeriesWithMultipleSamples != 1 {
		t.Fatalf("unexpected multi-sample stats: %#v", report.Stats)
	}

	// label_limit drop and rule drops must be reported separately from collisions.
	if report.Stats.DroppedSeries != 3 { // dropme, ghost, wide
		t.Fatalf("unexpected dropped count: %#v", report.Stats)
	}
	if report.Stats.SurvivingSeries+report.Stats.DroppedSeries != report.Stats.UniqueOriginalSeries {
		t.Fatalf("surviving/dropped counts do not add up: %#v", report.Stats)
	}
}

// productionRowOutcome runs the real production addRow for a single row and returns its
// canonical final labelset, or dropped=true when addRow skips the row or hits label_limit.
func productionRowOutcome(t *testing.T, cfg *ScrapeWork, r *prometheus.Row) (string, bool) {
	t.Helper()
	var wc writeRequestCtx
	err := wc.addRow(cfg, r, r.Timestamp, true)
	if errors.Is(err, errLabelsLimitExceeded) {
		return "", true
	}
	if err != nil {
		t.Fatalf("unexpected addRow error: %s", err)
	}
	if len(wc.writeRequest.Timeseries) == 0 {
		return "", true
	}
	labels := wc.writeRequest.Timeseries[0].Labels
	return promrelabel.LabelsToString(labels), false
}

// TestMetricRelabelCollisionAuditTimestampsAndLabelOrder verifies repeated timestamps of the
// same identity are not collisions and that label order is irrelevant to identity.
func TestMetricRelabelCollisionAuditTimestampsAndLabelOrder(t *testing.T) {
	cfg := auditTestCfg("", nil)
	// Timestamps above 2^31 are treated as milliseconds and kept verbatim by the parser.
	body := `
foo{a="1"} 100 3000000000
foo{a="1"} 200 4000000000
foo{a="1",b="2"} 5 5000000000
foo{b="2",a="1"} 7 6000000000
`
	report, proc := mustAuditDetailed(t, cfg, body, nil)
	if report.Stats.UniqueOriginalSeries != 2 {
		t.Fatalf("unexpected unique series count: %#v", report.Stats)
	}
	if report.Stats.CollisionGroups != 0 {
		t.Fatalf("unexpected collisions: %#v", report.CollisionGroups)
	}
	if report.Stats.SeriesWithMultipleSamples != 2 {
		t.Fatalf("unexpected multi-sample count: %#v", report.Stats)
	}
	e := proc.entries[`foo{a="1"}`]
	if e == nil || e.sampleCount != 2 || len(e.timestamps) != 2 || e.timestamps[0] != 3000000000 || e.timestamps[1] != 4000000000 {
		t.Fatalf("unexpected timestamp handling: %#v", e)
	}
	e2 := proc.entries[`foo{a="1",b="2"}`]
	if e2 == nil || e2.sampleCount != 2 {
		t.Fatalf("label order was not treated as the same identity: %#v", e2)
	}
}

// TestMetricRelabelCollisionAuditDroppedSeries verifies rule drops and label_limit drops are
// summarized separately and never participate in collisions.
func TestMetricRelabelCollisionAuditDroppedSeries(t *testing.T) {
	cfg := auditTestCfg(`
- action: drop
  source_labels: [__name__]
  regex: dropme
- action: drop
  source_labels: [__tmp]
  regex: x
`, func(c *ScrapeWork) {
		c.Labels = promutil.NewLabelsFromMap(map[string]string{"job": "test"})
	})
	body := `
dropme 1
ghost{__tmp="x"} 1
alive 1
`
	report := mustAudit(t, cfg, body, nil)
	if report.Stats.DroppedSeries != 2 || report.Stats.SurvivingSeries != 1 {
		t.Fatalf("unexpected stats: %#v", report.Stats)
	}
	if report.Stats.CollisionGroups != 0 {
		t.Fatalf("dropped series must not form collision groups: %#v", report.CollisionGroups)
	}
	drops := map[string]StepRef{}
	for _, d := range report.DroppedSeries.Examples {
		drops[d.OriginalLabels] = d.DropStep
	}
	ruleDrop, ok := drops[`dropme`]
	if !ok || !strings.Contains(ruleDrop.Rule, "drop") {
		t.Fatalf("missing rule-based drop: %#v", drops)
	}
	tmpDrop, ok := drops[`ghost{__tmp="x"}`]
	if !ok || !strings.Contains(tmpDrop.Rule, "drop") {
		t.Fatalf("missing __tmp-based drop: %#v", drops)
	}

	// label_limit is enforced after external labels exactly like addRow.
	limitCfg := auditTestCfg("", func(c *ScrapeWork) {
		c.LabelLimit = 2
		c.Labels = promutil.NewLabelsFromMap(map[string]string{"job": "test"})
	})
	limitReport := mustAudit(t, limitCfg, `wide{a="1",b="2",c="3"} 1`, nil)
	if limitReport.Stats.DroppedSeries != 1 {
		t.Fatalf("expected label_limit drop, got stats: %#v", limitReport.Stats)
	}
	if !strings.Contains(limitReport.DroppedSeries.Examples[0].DropStep.Rule, "label_limit") {
		t.Fatalf("unexpected drop step: %#v", limitReport.DroppedSeries.Examples[0])
	}
}

// TestMetricRelabelCollisionAuditFinalizeDrop verifies series whose labels are entirely
// removed during finalization (only __-prefixed labels remain after relabeling) are reported
// as dropped at the finalization stage, matching addRow's empty-label skip.
func TestMetricRelabelCollisionAuditFinalizeDrop(t *testing.T) {
	cfg := auditTestCfg(`
- action: labeldrop
  regex: __name__
`, func(c *ScrapeWork) {
		c.Labels = promutil.NewLabelsFromMap(map[string]string{})
	})
	body := `
ghost{__tmp="x"} 1
plain 1
`
	report := mustAudit(t, cfg, body, nil)
	if report.Stats.DroppedSeries != 2 || report.Stats.SurvivingSeries != 0 {
		t.Fatalf("unexpected stats: %#v", report.Stats)
	}
	// ghost survives the relabel rule with only the __tmp label and is removed by finalization.
	for _, d := range report.DroppedSeries.Examples {
		if d.OriginalLabels == `ghost{__tmp="x"}` && d.DropStep.Rule != auditRuleFinalizeLabels {
			t.Fatalf("expected finalize drop for ghost, got %#v", d.DropStep)
		}
	}

	// The same rows must be skipped (no timeseries) by the real production addRow.
	var rows prometheus.Rows
	rows.UnmarshalWithErrLogger(body, func(s string) { t.Fatalf("unexpected parse error: %s", s) })
	for i := range rows.Rows {
		_, dropped := productionRowOutcome(t, cfg, &rows.Rows[i])
		if !dropped {
			t.Fatalf("production addRow must drop %q", canonicalRowIdentity(&rows.Rows[i]))
		}
	}
}

// TestMetricRelabelCollisionAuditParseErrors verifies bad lines follow the existing parser
// behavior and prevent the report from claiming completeness.
func TestMetricRelabelCollisionAuditParseErrors(t *testing.T) {
	cfg := auditTestCfg("", nil)
	body := "bad line\nfoo{a=\"1\"} 1\n# HELP foo help text\nanother bad\n"
	report := mustAudit(t, cfg, body, nil)
	if report.Complete {
		t.Fatalf("report must be incomplete when parse errors exist")
	}
	if report.ParseErrors.Total != 2 || len(report.ParseErrors.Examples) != 2 {
		t.Fatalf("unexpected parse errors: %#v", report.ParseErrors)
	}
	if report.ParseErrors.Examples[0].LineNumber != 1 || report.ParseErrors.Examples[1].LineNumber != 4 {
		t.Fatalf("unexpected line numbers: %#v", report.ParseErrors.Examples)
	}
	if !strings.Contains(report.ParseErrors.Examples[0].Message, "bad line") {
		t.Fatalf("error message must preserve the parser wording: %#v", report.ParseErrors.Examples[0])
	}
	if report.Stats.RowsRead != 1 || report.Stats.UniqueOriginalSeries != 1 {
		t.Fatalf("valid rows must still be audited: %#v", report.Stats)
	}
}

// TestMetricRelabelCollisionAuditLimits verifies explicit rejection of over-limit input.
func TestMetricRelabelCollisionAuditLimits(t *testing.T) {
	cfg := auditTestCfg("", nil)
	body := "foo{a=\"1\"} 1\nfoo{a=\"2\"} 1\n"

	if _, err := AuditMetricRelabelCollisionsFromString(cfg, body, &CollisionAuditOptions{MaxSeries: 1}); !errors.Is(err, ErrAuditSeriesLimitExceeded) {
		t.Fatalf("expected ErrAuditSeriesLimitExceeded, got %v", err)
	}
	if _, err := AuditMetricRelabelCollisions(cfg, strings.NewReader(body), &CollisionAuditOptions{MaxResponseBytes: 10}); !errors.Is(err, ErrAuditResponseSizeLimitExceeded) {
		t.Fatalf("expected ErrAuditResponseSizeLimitExceeded, got %v", err)
	}
	// The exact byte size must be accepted.
	exact := &CollisionAuditOptions{MaxResponseBytes: int64(len(body))}
	report := mustAudit(t, cfg, body, exact)
	if !report.Complete {
		t.Fatalf("response within the byte limit must be accepted: %#v", report.ParseErrors)
	}
}

// TestMetricRelabelCollisionAuditTruncation verifies totals stay accurate while examples
// are truncated with explicit flags.
func TestMetricRelabelCollisionAuditTruncation(t *testing.T) {
	cfg := auditTestCfg(`
- action: labeldrop
  regex: id
`, nil)
	body := `
foo{id="1"} 1
foo{id="2"} 1
bar{id="1"} 1
bar{id="2"} 1
baz{id="1"} 1
baz{id="2"} 1
`
	report := mustAudit(t, cfg, body, &CollisionAuditOptions{MaxGroups: 2})
	if report.Stats.CollisionGroups != 3 || len(report.CollisionGroups) != 2 || !report.Truncated.Groups {
		t.Fatalf("unexpected group truncation: stats=%#v groups=%d truncated=%v",
			report.Stats, len(report.CollisionGroups), report.Truncated.Groups)
	}

	memberBody := `
foo{id="1"} 1
foo{id="2"} 1
foo{id="3"} 1
`
	memberReport := mustAudit(t, cfg, memberBody, &CollisionAuditOptions{MaxMembersPerGroup: 1})
	g := findCollisionGroup(t, memberReport, `foo{instance="localhost:8429",job="test"}`)
	if g.Size != 3 || len(g.Members) != 1 || !g.MembersTruncated || !memberReport.Truncated.GroupMembers {
		t.Fatalf("unexpected member truncation: %#v truncated=%v", g, memberReport.Truncated)
	}
}

// TestMetricRelabelCollisionAuditSecretsAndBoundaries verifies credential redaction and the
// zero-side-effect guarantees of the audit.
func TestMetricRelabelCollisionAuditSecretsAndBoundaries(t *testing.T) {
	cfg := auditTestCfg(`
- action: labeldrop
  regex: id
`, func(c *ScrapeWork) {
		c.ScrapeURL = "https://user:secret@example.com:8429/metrics"
		c.AuthToken = &auth.Token{AccountID: 42, ProjectID: 7}
	})
	body := "foo{id=\"1\"} 1\nfoo{id=\"2\"} 1\n"

	var sw scrapeWork
	sw.Config = cfg
	pushCalls := 0
	sw.PushData = func(_ *auth.Token, _ *prompb.WriteRequest) {
		pushCalls++
	}

	report, err := AuditMetricRelabelCollisions(cfg, strings.NewReader(body), nil)
	if err != nil {
		t.Fatalf("unexpected audit error: %s", err)
	}
	if report.Tenant != "42:7" {
		t.Fatalf("unexpected tenant: %q", report.Tenant)
	}
	if strings.Contains(report.ScrapeURLRedacted, "secret") {
		t.Fatalf("URL password leaked: %q", report.ScrapeURLRedacted)
	}
	if !strings.Contains(report.ScrapeURLRedacted, "xxxxx") {
		t.Fatalf("URL must be redacted: %q", report.ScrapeURLRedacted)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("cannot marshal report: %s", err)
	}
	if strings.Contains(string(raw), "secret") {
		t.Fatalf("report JSON leaks secret material: %s", raw)
	}

	// No mutable scrapeWork state may change.
	if sw.seriesLimiter != nil {
		t.Fatalf("series limiter must not be initialized by the audit")
	}
	if len(sw.lastScrapeCompressed) != 0 {
		t.Fatalf("last scrape state must not be touched by the audit")
	}
	if pushCalls != 0 {
		t.Fatalf("remote write must not be performed; pushCalls=%d", pushCalls)
	}
	if report.Boundaries != (AuditBoundaries{}) {
		t.Fatalf("all boundary flags must be false: %#v", report.Boundaries)
	}

	// Nil tenant is reported as multitenant.
	cfg.AuthToken = nil
	multiReport := mustAudit(t, cfg, body, nil)
	if multiReport.Tenant != "multitenant" {
		t.Fatalf("unexpected nil tenant representation: %q", multiReport.Tenant)
	}
}

// TestMetricRelabelCollisionAuditAutoMetricsBoundary verifies scraped rows whose names clash
// with auto metrics follow the exported_ rule, while no auto metrics are generated by the audit.
func TestMetricRelabelCollisionAuditAutoMetricsBoundary(t *testing.T) {
	cfg := auditTestCfg("", func(c *ScrapeWork) {
		c.HonorLabels = false
		c.Labels = promutil.NewLabelsFromMap(map[string]string{"job": "test"})
	})
	body := "up 1\nscrape_duration_seconds 2\n"
	report, proc := mustAuditDetailed(t, cfg, body, nil)
	if report.Boundaries.AutoMetricsIncluded {
		t.Fatalf("auto metrics boundary flag must stay false")
	}
	for _, e := range proc.entryOrder {
		if e.dropped {
			t.Fatalf("clashing metric rows must survive via exported_ prefix: %q", e.originalKey)
		}
		if !strings.HasPrefix(e.finalKey, "exported_") {
			t.Fatalf("clashing metric must be renamed: identity=%q final=%q", e.originalKey, e.finalKey)
		}
	}
}

// TestMetricRelabelCollisionAuditStreamingEquivalence verifies the io.Reader and string
// entry points produce identical reports.
func TestMetricRelabelCollisionAuditStreamingEquivalence(t *testing.T) {
	cfg := auditTestCfg(`
- action: labeldrop
  regex: id
`, nil)
	body := "foo{id=\"1\"} 1\nfoo{id=\"2\"} 2\nbar{id=\"3\"} 3\n"

	fromString := mustAudit(t, cfg, body, nil)
	fromReader, err := AuditMetricRelabelCollisions(cfg, strings.NewReader(body), nil)
	if err != nil {
		t.Fatalf("unexpected streaming audit error: %s", err)
	}
	if !reflect.DeepEqual(fromString, fromReader) {
		t.Fatalf("streaming and in-memory reports differ:\nstring=%#v\nreader=%#v", fromString, fromReader)
	}

	// Rejecting nil inputs explicitly instead of panicking.
	if _, err := AuditMetricRelabelCollisions(nil, strings.NewReader(body), nil); err == nil {
		t.Fatalf("nil cfg must be rejected")
	}
	if _, err := AuditMetricRelabelCollisions(cfg, nil, nil); err == nil {
		t.Fatalf("nil reader must be rejected")
	}
}

// TestMetricRelabelCollisionAuditEmptyInput verifies empty and comment-only responses produce
// an empty but complete report.
func TestMetricRelabelCollisionAuditEmptyInput(t *testing.T) {
	cfg := auditTestCfg("", nil)
	report := mustAudit(t, cfg, "# HELP foo just a comment\n\n", nil)
	if !report.Complete {
		t.Fatalf("comment-only response must be complete: %#v", report.ParseErrors)
	}
	if report.Stats.UniqueOriginalSeries != 0 || report.Stats.CollisionGroups != 0 {
		t.Fatalf("unexpected stats for empty response: %#v", report.Stats)
	}
	if report.DroppedSeries.Total != 0 || len(report.CollisionGroups) != 0 {
		t.Fatalf("unexpected content for empty response: %#v", report)
	}
}
