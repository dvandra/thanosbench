package blockgen

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/model"
)

// fixedMaxTime returns a stable maxTime for the plan functions under test.
func fixedMaxTime(t *testing.T) model.TimeOrDurationValue {
	t.Helper()
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	return model.TimeOrDurationValue{Time: &ts}
}

// collectBlocks runs a PlanFn and returns the emitted block specs.
func collectBlocks(t *testing.T, fn PlanFn) []BlockSpec {
	t.Helper()
	var blocks []BlockSpec
	err := fn(context.Background(), fixedMaxTime(t), labels.Labels{}, func(b BlockSpec) error {
		blocks = append(blocks, b)
		return nil
	})
	if err != nil {
		t.Fatalf("plan returned error: %v", err)
	}
	if len(blocks) == 0 {
		t.Fatal("plan emitted no blocks")
	}
	return blocks
}

func TestGetEnvInt(t *testing.T) {
	const key = "TB_TEST_INT"
	for _, tc := range []struct {
		name    string
		set     bool
		val     string
		def     int
		want    int
		wantErr bool
	}{
		{name: "unset returns default", set: false, def: 7, want: 7},
		{name: "empty returns default", set: true, val: "", def: 7, want: 7},
		{name: "valid parses", set: true, val: "42", def: 7, want: 42},
		{name: "zero allowed", set: true, val: "0", def: 7, want: 0},
		{name: "non-numeric errors", set: true, val: "abc", def: 7, wantErr: true},
		{name: "negative errors", set: true, val: "-1", def: 7, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(key, tc.val)
			}
			got, err := getEnvInt(key, tc.def)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got value %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGetEnvFloat(t *testing.T) {
	const key = "TB_TEST_FLOAT"
	for _, tc := range []struct {
		name    string
		set     bool
		val     string
		def     float64
		want    float64
		wantErr bool
	}{
		{name: "unset returns default", set: false, def: 2.5, want: 2.5},
		{name: "empty returns default", set: true, val: "", def: 2.5, want: 2.5},
		{name: "valid parses", set: true, val: "8.25", def: 2.5, want: 8.25},
		{name: "non-numeric errors", set: true, val: "nope", def: 2.5, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(key, tc.val)
			}
			got, err := getEnvFloat(key, tc.def)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got value %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// countByName tallies series per __name__ across a block.
func countByName(b BlockSpec) map[string]int {
	counts := map[string]int{}
	for _, s := range b.Series {
		counts[s.Labels.Get("__name__")]++
	}
	return counts
}

func TestWorkloadPodProfilePreserved(t *testing.T) {
	if _, ok := Profiles["custom-continous-1-week-workload-pod"]; !ok {
		t.Fatal("upstream workload/pod profile was removed")
	}
}

func TestRightSizingLeveledCardinality(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "3")
	t.Setenv("NUM_WORKLOADS", "2")
	t.Setenv("NUM_PODS", "4")
	t.Setenv("NUM_EXTRA_METRICS", "0")

	const (
		ns  = 3
		wl  = 2
		pod = 4
	)
	p := len(rsProfiles) // each acm_rs metric is emitted once per profile

	blocks := collectBlocks(t, rightSizingLeveled([]time.Duration{2 * time.Hour}))
	counts := countByName(blocks[0])

	for _, m := range rsMeasures {
		checks := map[string]int{
			"acm_rs:cluster:" + m:   1 * p,
			"acm_rs:namespace:" + m: ns * p,
			"acm_rs:workload:" + m:  ns * wl * p,
			"acm_rs:pod:" + m:       ns * wl * pod * p,
		}
		for name, want := range checks {
			if got := counts[name]; got != want {
				t.Errorf("%s: got %d series, want %d", name, got, want)
			}
		}
	}

	// request_hard exists at the namespace level only, once per profile.
	for _, m := range []string{"cpu_request_hard", "memory_request_hard"} {
		name := "acm_rs:namespace:" + m
		if got := counts[name]; got != ns*p {
			t.Errorf("%s: got %d series, want %d", name, got, ns*p)
		}
		if got := counts["acm_rs:cluster:"+m]; got != 0 {
			t.Errorf("acm_rs:cluster:%s should not exist, got %d", m, got)
		}
		if got := counts["acm_rs:workload:"+m]; got != 0 {
			t.Errorf("acm_rs:workload:%s should not exist, got %d", m, got)
		}
	}
}

func TestRightSizingLeveledLabels(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "1")
	t.Setenv("NUM_WORKLOADS", "1")
	t.Setenv("NUM_PODS", "1")
	t.Setenv("NUM_EXTRA_METRICS", "0")

	blocks := collectBlocks(t, rightSizingLeveled([]time.Duration{2 * time.Hour}))

	// Expected breakdown label names per level, in canonical (sorted) order,
	// excluding __name__. Every acm_rs series carries a per-series `profile`.
	wantLabels := map[string][]string{
		"acm_rs:cluster:cpu_usage":   {"profile"},
		"acm_rs:namespace:cpu_usage": {"namespace", "profile"},
		"acm_rs:workload:cpu_usage":  {"namespace", "profile", "workload", "workload_type"},
		"acm_rs:pod:cpu_usage":       {"namespace", "pod", "profile", "workload", "workload_type"},
	}

	seen := map[string]bool{}
	for _, s := range blocks[0].Series {
		name := s.Labels.Get("__name__")
		want, ok := wantLabels[name]
		if !ok {
			continue
		}
		seen[name] = true

		var got []string
		for _, l := range s.Labels {
			if l.Name == "__name__" {
				continue
			}
			got = append(got, l.Name)
		}
		if !equalStrings(got, want) {
			t.Errorf("%s: got breakdown labels %v, want %v", name, got, want)
		}
		// Labels (including __name__) must be sorted for a canonical TSDB index.
		if !labelsSorted(s.Labels) {
			t.Errorf("%s: labels not sorted: %v", name, s.Labels)
		}
	}
	for name := range wantLabels {
		if !seen[name] {
			t.Errorf("expected to see series %q, but it was absent", name)
		}
	}
}

// TestRightSizingLeveledCapErrors ensures an over-large cardinality fails fast
// rather than attempting to build the block.
func TestRightSizingLeveledCapErrors(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "1000")
	t.Setenv("NUM_WORKLOADS", "1000")
	t.Setenv("NUM_PODS", "1000")

	err := rightSizingLeveled([]time.Duration{2 * time.Hour})(
		context.Background(), fixedMaxTime(t), labels.Labels{}, func(BlockSpec) error {
			t.Fatal("blockEncoder must not be called when the cap is exceeded")
			return nil
		})
	if err == nil {
		t.Fatal("expected cap error, got nil")
	}
}

// TestRightSizingLeveledInvalidEnv confirms a present-but-invalid NUM_* value is
// a hard error (fail fast) instead of silently defaulting to zero.
func TestRightSizingLeveledInvalidEnv(t *testing.T) {
	t.Setenv("NUM_WORKLOADS", "not-a-number")

	err := rightSizingLeveled([]time.Duration{2 * time.Hour})(
		context.Background(), fixedMaxTime(t), labels.Labels{}, func(BlockSpec) error {
			t.Fatal("blockEncoder must not be called on invalid env")
			return nil
		})
	if err == nil {
		t.Fatal("expected error for invalid NUM_WORKLOADS, got nil")
	}
}

// TestCustomContinuousFillerMigration verifies that the extra_metric_* filler is
// generated by custom_continuous (NUM_EXTRA_METRICS) rather than inlined, and
// that NUM_WORKLOADS/NUM_PODS do not affect the legacy profiles.
func TestCustomContinuousFillerMigration(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "2")
	t.Setenv("NUM_NAMES", "3")
	t.Setenv("NUM_EXTRA_METRICS", "5")
	// These must be ignored by custom_continuous.
	t.Setenv("NUM_WORKLOADS", "99")
	t.Setenv("NUM_PODS", "99")

	// A flat-scope core metric so its cardinality is namespace x name, matching
	// the filler. Being an acm_rs* metric it is additionally replicated per
	// profile.
	core := []string{"acm_rs_vm:namespace:cpu_usage"}
	blocks := collectBlocks(t, custom_continuous([]time.Duration{2 * time.Hour}, 1, core))
	counts := countByName(blocks[0])

	const nsTimesNames = 2 * 3

	if got, want := counts["acm_rs_vm:namespace:cpu_usage"], nsTimesNames*len(rsProfiles); got != want {
		t.Errorf("core: got %d, want %d", got, want)
	}
	// Filler metrics 1..5 exist with the flat cardinality (no profile); 6 does not.
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("extra_metric_%d", i)
		if got := counts[name]; got != nsTimesNames {
			t.Errorf("%s: got %d, want %d", name, got, nsTimesNames)
		}
	}
	if _, ok := counts["extra_metric_6"]; ok {
		t.Error("extra_metric_6 should not exist with NUM_EXTRA_METRICS=5")
	}

	total := len(core) + 5 // core + filler metric names
	if len(counts) != total {
		t.Errorf("distinct metric names: got %d, want %d", len(counts), total)
	}
}

// TestCustomContinuousLevelLabels verifies the level-aware label model for the
// legacy profiles: cluster metrics collapse to one series with no breakdown
// labels, non-VM namespace metrics carry only `namespace`, while VM namespace,
// kubevirt and filler metrics keep the flat namespace x name shape.
func TestCustomContinuousLevelLabels(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "2")
	t.Setenv("NUM_NAMES", "3")
	t.Setenv("NUM_EXTRA_METRICS", "1")

	const (
		ns           = 2
		nsTimesNames = 2 * 3
	)
	p := len(rsProfiles) // acm_rs* metrics are replicated per profile

	core := []string{
		"acm_rs:cluster:cpu_usage",
		"acm_rs_vm:cluster:cpu_usage",
		"acm_rs:namespace:cpu_usage",
		"acm_rs_vm:namespace:cpu_usage",
		"kubevirt_vm_running_status_last",
	}
	blocks := collectBlocks(t, custom_continuous([]time.Duration{2 * time.Hour}, 1, core))

	type want struct {
		count  int
		labels []string // breakdown label names (sorted), excluding __name__
	}
	// acm_rs* metrics carry a per-series `profile` label and are counted per
	// profile; kubevirt and filler stay flat with no profile.
	expect := map[string]want{
		"acm_rs:cluster:cpu_usage":        {count: 1 * p, labels: []string{"profile"}},
		"acm_rs_vm:cluster:cpu_usage":     {count: 1 * p, labels: []string{"profile"}},
		"acm_rs:namespace:cpu_usage":      {count: ns * p, labels: []string{"namespace", "profile"}},
		"acm_rs_vm:namespace:cpu_usage":   {count: nsTimesNames * p, labels: []string{"name", "namespace", "profile"}},
		"kubevirt_vm_running_status_last": {count: nsTimesNames, labels: []string{"name", "namespace"}},
		"extra_metric_1":                  {count: nsTimesNames, labels: []string{"name", "namespace"}},
	}

	counts := map[string]int{}
	sawLabels := map[string][]string{}
	for _, s := range blocks[0].Series {
		name := s.Labels.Get("__name__")
		counts[name]++
		if _, ok := sawLabels[name]; !ok {
			sawLabels[name] = breakdownNames(s.Labels)
		}
	}

	for name, w := range expect {
		if counts[name] != w.count {
			t.Errorf("%s: got %d series, want %d", name, counts[name], w.count)
		}
		if !equalStrings(sawLabels[name], w.labels) {
			t.Errorf("%s: got breakdown labels %v, want %v", name, sawLabels[name], w.labels)
		}
	}
}

// TestRecommendationDerivation checks that *_recommendation series are wired to
// derive from their *_usage sibling (SeedName + ValueScale) so they track usage,
// while other measures are left as independent draws. The actual value equality
// is proven in blockgen_test.go (TestSeedNameValueScaleCorrelate).
func TestRecommendationDerivation(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "1")
	t.Setenv("NUM_WORKLOADS", "1")
	t.Setenv("NUM_PODS", "1")
	t.Setenv("NUM_EXTRA_METRICS", "0")

	blocks := collectBlocks(t, rightSizingLeveled([]time.Duration{2 * time.Hour}))

	var sawRec, sawUsage bool
	for _, s := range blocks[0].Series {
		name := s.Labels.Get("__name__")
		switch {
		case strings.HasSuffix(name, "_recommendation"):
			sawRec = true
			wantSeed := strings.TrimSuffix(name, "_recommendation") + "_usage"
			if s.SeedName != wantSeed {
				t.Errorf("%s: SeedName=%q, want %q", name, s.SeedName, wantSeed)
			}
			if s.ValueScale != recommendationRatio {
				t.Errorf("%s: ValueScale=%v, want %v", name, s.ValueScale, recommendationRatio)
			}
		case strings.HasSuffix(name, "_usage"), strings.HasSuffix(name, "_request"), strings.HasSuffix(name, "_request_hard"):
			sawUsage = sawUsage || strings.HasSuffix(name, "_usage")
			if s.SeedName != "" || s.ValueScale != 0 {
				t.Errorf("%s: unexpected derivation SeedName=%q ValueScale=%v", name, s.SeedName, s.ValueScale)
			}
		}
	}
	if !sawRec || !sawUsage {
		t.Fatalf("expected both usage and recommendation series (usage=%v recommendation=%v)", sawUsage, sawRec)
	}
}

// TestMemoryMetricByteScale verifies memory_* measures draw from the byte-scale
// range (MEM_MIN_GAUGE/MEM_MAX_GAUGE) while cpu_* measures keep the cores-scale
// MIN_GAUGE/MAX_GAUGE — so memory lands at MB/GB magnitudes, not a few bytes.
func TestMemoryMetricByteScale(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "1")
	t.Setenv("NUM_WORKLOADS", "1")
	t.Setenv("NUM_PODS", "1")
	t.Setenv("NUM_EXTRA_METRICS", "0")
	t.Setenv("MIN_GAUGE", "2")
	t.Setenv("MAX_GAUGE", "8")
	t.Setenv("MEM_MIN_GAUGE", "1000000000") // 1 GB
	t.Setenv("MEM_MAX_GAUGE", "8000000000") // 8 GB

	blocks := collectBlocks(t, rightSizingLeveled([]time.Duration{2 * time.Hour}))

	var sawMem, sawCPU bool
	for _, s := range blocks[0].Series {
		name := s.Labels.Get("__name__")
		if !strings.HasPrefix(name, "acm_rs:") {
			continue
		}
		switch {
		case strings.Contains(name, "memory"):
			sawMem = true
			if s.Characteristics.Min != 1e9 || s.Characteristics.Max != 8e9 {
				t.Errorf("%s: memory range = [%v,%v], want [1e9,8e9]", name, s.Characteristics.Min, s.Characteristics.Max)
			}
		case strings.Contains(name, "cpu"):
			sawCPU = true
			if s.Characteristics.Min != 2 || s.Characteristics.Max != 8 {
				t.Errorf("%s: cpu range = [%v,%v], want [2,8]", name, s.Characteristics.Min, s.Characteristics.Max)
			}
		}
	}
	if !sawMem || !sawCPU {
		t.Fatalf("expected both cpu and memory acm_rs series (cpu=%v mem=%v)", sawCPU, sawMem)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// breakdownNames returns the sorted label names of ls excluding __name__.
func breakdownNames(ls labels.Labels) []string {
	var out []string
	for _, l := range ls {
		if l.Name == "__name__" {
			continue
		}
		out = append(out, l.Name)
	}
	sort.Strings(out)
	return out
}

func labelsSorted(ls labels.Labels) bool {
	for i := 1; i < len(ls); i++ {
		if ls[i-1].Name >= ls[i].Name {
			return false
		}
	}
	return true
}
