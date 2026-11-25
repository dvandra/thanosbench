package blockgen

import (
	"context"
	"fmt"
	"sort"
	"strconv"
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

// TestGetEnvDuration mirrors TestGetEnvInt for the duration knob backing
// RS_SCRAPE_INTERVAL: unset uses the default, Go duration strings parse, and
// anything else (no unit, "1d", zero or negative) is an error.
func TestGetEnvDuration(t *testing.T) {
	const key = "THANOSBENCH_TEST_DURATION"
	for _, tc := range []struct {
		val     string
		want    time.Duration
		wantErr bool
	}{
		{"", 15 * time.Minute, false},
		{"15m", 15 * time.Minute, false},
		{"24h", 24 * time.Hour, false},
		{"1h30m", 90 * time.Minute, false},
		{"0", 0, true},
		{"0s", 0, true},
		{"-5m", 0, true},
		{"15", 0, true},
		{"1d", 0, true},
		{"soon", 0, true},
	} {
		t.Run(fmt.Sprintf("%q", tc.val), func(t *testing.T) {
			t.Setenv(key, tc.val)
			got, err := getEnvDuration(key, 15*time.Minute)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("getEnvDuration(%q) = %v, want error", tc.val, got)
				}
				if !strings.Contains(err.Error(), key) {
					t.Errorf("error %q does not name the variable", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("getEnvDuration(%q): unexpected error %v", tc.val, err)
			}
			if got != tc.want {
				t.Errorf("getEnvDuration(%q) = %v, want %v", tc.val, got, tc.want)
			}
		})
	}
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
	t.Setenv("NUM_POD_METRICS", "0")
	t.Setenv("NUM_CLUSTER_METRICS", "0")

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
	t.Setenv("NUM_POD_METRICS", "0")
	t.Setenv("NUM_CLUSTER_METRICS", "0")

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
	t.Setenv("NUM_POD_METRICS", "0")
	t.Setenv("NUM_CLUSTER_METRICS", "0")

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

// planWithCluster runs a PlanFn with a `cluster` block label, as the demo scripts do.
func planWithCluster(t *testing.T, fn PlanFn, cluster string) []BlockSpec {
	t.Helper()
	var blocks []BlockSpec
	err := fn(context.Background(), fixedMaxTime(t), labels.FromStrings("cluster", cluster), func(b BlockSpec) error {
		blocks = append(blocks, b)
		return nil
	})
	if err != nil {
		t.Fatalf("plan returned error: %v", err)
	}
	return blocks
}

// podPairs returns the (namespace, pod) pairs of the series named name.
func podPairs(b BlockSpec, name string) map[[2]string]bool {
	out := map[[2]string]bool{}
	for _, s := range b.Series {
		if s.Labels.Get("__name__") == name {
			out[[2]string{s.Labels.Get("namespace"), s.Labels.Get("pod")}] = true
		}
	}
	return out
}

func equalSets[K comparable](a, b map[K]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// cancelledCtx makes a plan stop with context.Canceled at its first block, so a
// broken cap check fails a test quickly instead of building millions of series.
func cancelledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// TestPodNameGolden pins podName's output (expected values computed separately
// with FNV-1a 64), so any change to the naming, such as a per-run salt, fails.
func TestPodNameGolden(t *testing.T) {
	for _, tc := range []struct {
		cluster, ns, wl string
		pi              int
		want            string
	}{
		{"ac-test-man-1", "namespace-0", "workload-0", 0, "workload-0-69399f19bc-0"},
		{"ac-test-man-2", "namespace-0", "workload-0", 0, "workload-0-3b1c769042-0"},
		{"", "namespace-3", "workload-7", 2, "workload-7-5df624374f-2"},
	} {
		if got := podName(tc.cluster, tc.ns, tc.wl, tc.pi); got != tc.want {
			t.Errorf("podName(%q, %q, %q, %d) = %q, want %q", tc.cluster, tc.ns, tc.wl, tc.pi, got, tc.want)
		}
	}
}

// TestRightSizingLeveledPodNamesUnique checks there is one distinct pod name per
// pod, that pods nest under their workload, that names are stable across blocks
// and across separate runs (generate_180day.sh runs one plan per week), and that
// another cluster gets different names.
func TestRightSizingLeveledPodNamesUnique(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "3")
	t.Setenv("NUM_WORKLOADS", "2")
	t.Setenv("NUM_PODS", "4")
	t.Setenv("NUM_EXTRA_METRICS", "0")
	t.Setenv("NUM_POD_METRICS", "0")
	t.Setenv("NUM_CLUSTER_METRICS", "0")
	const pods = 3 * 2 * 4

	plan := func(cluster string) []BlockSpec {
		return planWithCluster(t, rightSizingLeveled([]time.Duration{2 * time.Hour, 2 * time.Hour}), cluster)
	}
	run1, run2, other := plan("ac-test-man-1"), plan("ac-test-man-1"), plan("ac-test-man-2")

	names := func(b BlockSpec) map[string]bool {
		out := map[string]bool{}
		for pair := range podPairs(b, "acm_rs:pod:cpu_usage") {
			out[pair[1]] = true
		}
		return out
	}
	first := names(run1[0])
	if len(first) != pods {
		t.Fatalf("got %d distinct pod names, want %d (one per pod)", len(first), pods)
	}
	for _, s := range run1[0].Series {
		if s.Labels.Get("__name__") == "acm_rs:pod:cpu_usage" && !strings.HasPrefix(s.Labels.Get("pod"), s.Labels.Get("workload")+"-") {
			t.Errorf("pod %q does not nest under its workload %q", s.Labels.Get("pod"), s.Labels.Get("workload"))
		}
	}
	if !equalSets(names(run1[1]), first) {
		t.Error("pod names differ between blocks of one run")
	}
	if !equalSets(names(run2[0]), first) {
		t.Error("pod names differ between two runs for the same cluster")
	}
	for pod := range names(other[0]) {
		if first[pod] {
			t.Errorf("pod name %q repeats in another cluster", pod)
		}
	}
}

// TestRightSizingLeveledPodFiller verifies NUM_POD_METRICS emits one series per
// pod per filler metric, on exactly the (namespace, pod) pairs of acm_rs:pod,
// without a profile label.
func TestRightSizingLeveledPodFiller(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "2")
	t.Setenv("NUM_WORKLOADS", "2")
	t.Setenv("NUM_PODS", "3")
	t.Setenv("NUM_EXTRA_METRICS", "1")
	t.Setenv("NUM_POD_METRICS", "2")
	t.Setenv("NUM_CLUSTER_METRICS", "3")

	blocks := collectBlocks(t, rightSizingLeveled([]time.Duration{2 * time.Hour}))
	counts := countByName(blocks[0])

	for _, name := range []string{"extra_cluster_metric_1", "extra_cluster_metric_2", "extra_cluster_metric_3"} {
		if got := counts[name]; got != 1 {
			t.Errorf("%s: got %d series, want 1 (one per cluster)", name, got)
		}
	}
	if got := counts["extra_cluster_metric_4"]; got != 0 {
		t.Errorf("extra_cluster_metric_4 should not exist with NUM_CLUSTER_METRICS=3, got %d", got)
	}
	for _, s := range blocks[0].Series {
		if name := s.Labels.Get("__name__"); strings.HasPrefix(name, "extra_cluster_metric_") {
			if got := breakdownNames(s.Labels); len(got) != 0 {
				t.Errorf("%s: got breakdown labels %v, want none", name, got)
			}
			if s.Characteristics.ScrapeInterval != 5*time.Minute {
				t.Errorf("%s: scrape interval %v, want 5m", name, s.Characteristics.ScrapeInterval)
			}
		}
	}

	const pods = 2 * 2 * 3
	for _, name := range []string{"extra_pod_metric_1", "extra_pod_metric_2"} {
		if got := counts[name]; got != pods {
			t.Errorf("%s: got %d series, want %d (one per pod)", name, got, pods)
		}
	}
	if got := counts["extra_pod_metric_3"]; got != 0 {
		t.Errorf("extra_pod_metric_3 should not exist with NUM_POD_METRICS=2, got %d", got)
	}
	if got := counts["extra_metric_1"]; got != 2 {
		t.Errorf("extra_metric_1: got %d series, want 2 (one per namespace)", got)
	}

	rsPairs := podPairs(blocks[0], "acm_rs:pod:cpu_usage")
	if len(rsPairs) != pods {
		t.Fatalf("acm_rs:pod has %d (namespace, pod) pairs, want %d", len(rsPairs), pods)
	}
	for _, name := range []string{"extra_pod_metric_1", "extra_pod_metric_2"} {
		if !equalSets(podPairs(blocks[0], name), rsPairs) {
			t.Errorf("%s: (namespace, pod) pairs differ from acm_rs:pod", name)
		}
	}
	for _, s := range blocks[0].Series {
		name := s.Labels.Get("__name__")
		if !strings.HasPrefix(name, "extra_pod_metric_") {
			continue
		}
		if got, want := breakdownNames(s.Labels), []string{"container", "namespace", "pod"}; !equalStrings(got, want) {
			t.Errorf("%s: got breakdown labels %v, want %v", name, got, want)
		}
		if !labelsSorted(s.Labels) {
			t.Errorf("%s: labels not sorted: %v", name, s.Labels)
		}
		if s.Characteristics.ScrapeInterval != 5*time.Minute {
			t.Errorf("%s: scrape interval %v, want 5m", name, s.Characteristics.ScrapeInterval)
		}
	}
}

// TestRightSizingLeveledNodeFiller verifies NUM_NODE_METRICS emits one series per
// node per filler metric, with a node label and no right-sizing profile.
func TestRightSizingLeveledNodeFiller(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "1")
	t.Setenv("NUM_WORKLOADS", "1")
	t.Setenv("NUM_PODS", "1")
	t.Setenv("NUM_EXTRA_METRICS", "0")
	t.Setenv("NUM_POD_METRICS", "0")
	t.Setenv("NUM_CLUSTER_METRICS", "0")
	t.Setenv("NUM_NODES", "4")
	t.Setenv("NUM_NODE_METRICS", "2")

	blocks := collectBlocks(t, rightSizingLeveled([]time.Duration{2 * time.Hour}))
	counts := countByName(blocks[0])

	for _, name := range []string{"extra_node_metric_1", "extra_node_metric_2"} {
		if got := counts[name]; got != 4 {
			t.Errorf("%s: got %d series, want 4 (one per node)", name, got)
		}
	}
	if got := counts["extra_node_metric_3"]; got != 0 {
		t.Errorf("extra_node_metric_3 should not exist with NUM_NODE_METRICS=2, got %d", got)
	}
	// 1 namespace x 1 workload x 1 pod, 3 profiles, 8 measures, plus namespace
	// hard and pod info: 3*(8*4+2+1)=105, plus 2 node metrics x 4 nodes.
	if got, want := len(blocks[0].Series), 105+8; got != want {
		t.Errorf("block series = %d, want %d", got, want)
	}
	nodes := map[string]bool{}
	for _, s := range blocks[0].Series {
		name := s.Labels.Get("__name__")
		if !strings.HasPrefix(name, "extra_node_metric_") {
			continue
		}
		if s.Labels.Get("profile") != "" {
			t.Errorf("%s has a profile label", name)
		}
		if got, want := breakdownNames(s.Labels), []string{"node"}; !equalStrings(got, want) {
			t.Errorf("%s: got breakdown labels %v, want %v", name, got, want)
		}
		nodes[s.Labels.Get("node")] = true
	}
	if len(nodes) != 4 {
		t.Errorf("got %d distinct nodes, want 4", len(nodes))
	}
}

// TestRightSizingLeveledScrapeIntervalGuardUnsortedRanges checks the guard looks
// at every block length, not only the first one in the range list.
func TestRightSizingLeveledScrapeIntervalGuardUnsortedRanges(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "1")
	t.Setenv("NUM_WORKLOADS", "1")
	t.Setenv("NUM_PODS", "1")
	t.Setenv("NUM_EXTRA_METRICS", "0")
	t.Setenv("NUM_POD_METRICS", "0")
	t.Setenv("NUM_CLUSTER_METRICS", "0")
	t.Setenv("RS_SCRAPE_INTERVAL", "3h")
	err := rightSizingLeveled([]time.Duration{8 * time.Hour, 2 * time.Hour})(
		context.Background(), fixedMaxTime(t), labels.Labels{}, func(BlockSpec) error {
			t.Fatal("blockEncoder must not be called when the interval exceeds the shortest block")
			return nil
		})
	if err == nil || !strings.Contains(err.Error(), "RS_SCRAPE_INTERVAL") {
		t.Fatalf("want RS_SCRAPE_INTERVAL guard error, got %v", err)
	}
}

// TestRightSizingLeveledScrapeInterval checks RS_SCRAPE_INTERVAL: unset keeps
// the 15m default, a custom value applies to every acm_rs series and only to
// them (the extra_* filler stays at 5m), and an invalid or non-positive value
// fails fast before any block is planned.
func TestRightSizingLeveledScrapeInterval(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "2")
	t.Setenv("NUM_WORKLOADS", "1")
	t.Setenv("NUM_PODS", "2")
	t.Setenv("NUM_EXTRA_METRICS", "1")
	t.Setenv("NUM_POD_METRICS", "1")
	t.Setenv("NUM_CLUSTER_METRICS", "1")

	// intervals returns, for the first block of a plan, how many acm_rs series
	// and how many extra_* filler series use each scrape interval.
	intervals := func(t *testing.T) (rs, filler map[time.Duration]int) {
		t.Helper()
		rs, filler = map[time.Duration]int{}, map[time.Duration]int{}
		blocks := collectBlocks(t, rightSizingLeveled([]time.Duration{2 * time.Hour}))
		for _, s := range blocks[0].Series {
			name := s.Labels.Get("__name__")
			switch {
			case strings.HasPrefix(name, "acm_rs:"):
				rs[s.Characteristics.ScrapeInterval]++
			case strings.HasPrefix(name, "extra_"):
				filler[s.Characteristics.ScrapeInterval]++
			default:
				t.Errorf("unexpected series %s", name)
			}
		}
		if len(rs) == 0 || len(filler) == 0 {
			t.Fatalf("plan emitted %d acm_rs and %d filler intervals, want both non-empty", len(rs), len(filler))
		}
		return rs, filler
	}
	// only reports whether every series of a kind uses exactly want.
	only := func(m map[time.Duration]int, want time.Duration) bool {
		return len(m) == 1 && m[want] > 0
	}

	t.Run("default", func(t *testing.T) {
		t.Setenv("RS_SCRAPE_INTERVAL", "")
		rs, filler := intervals(t)
		if !only(rs, 15*time.Minute) {
			t.Errorf("acm_rs intervals = %v, want only 15m", rs)
		}
		if !only(filler, 5*time.Minute) {
			t.Errorf("filler intervals = %v, want only 5m", filler)
		}
	})

	for _, tc := range []struct {
		val  string
		want time.Duration
	}{
		{"90s", 90 * time.Second},
		{"1h", time.Hour},
		{"2h", 2 * time.Hour}, // the shortest block: the largest interval allowed
	} {
		t.Run(tc.val, func(t *testing.T) {
			t.Setenv("RS_SCRAPE_INTERVAL", tc.val)
			rs, filler := intervals(t)
			if !only(rs, tc.want) {
				t.Errorf("acm_rs intervals = %v, want only %v", rs, tc.want)
			}
			if !only(filler, 5*time.Minute) {
				t.Errorf("filler intervals = %v, want only 5m (filler must not follow RS_SCRAPE_INTERVAL)", filler)
			}
		})
	}

	// 45m and 1h30m are shorter than the 2h block but do not divide it: the last
	// sample of each block would land past its end (observed overlaps).
	for _, val := range []string{"0", "0s", "-1h", "15", "1d", "fifteen", "3h", "24h", "45m", "1h30m", "7m"} {
		t.Run("invalid "+val, func(t *testing.T) {
			t.Setenv("RS_SCRAPE_INTERVAL", val)
			err := rightSizingLeveled([]time.Duration{2 * time.Hour})(
				context.Background(), fixedMaxTime(t), labels.Labels{}, func(BlockSpec) error {
					t.Fatal("blockEncoder must not be called on invalid RS_SCRAPE_INTERVAL")
					return nil
				})
			if err == nil {
				t.Fatalf("RS_SCRAPE_INTERVAL=%q: expected error, got nil", val)
			}
			if !strings.Contains(err.Error(), "RS_SCRAPE_INTERVAL") {
				t.Errorf("error %q does not name RS_SCRAPE_INTERVAL", err)
			}
		})
	}
}

// TestLeveledSeriesPerBlockMatchesEmitted checks the projected count, which the
// cap and the demo scripts' sizing rely on, equals what a plan really emits.
func TestLeveledSeriesPerBlockMatchesEmitted(t *testing.T) {
	for _, c := range []struct{ ns, wl, pods, extra, podMetrics, clusterMetrics int }{
		{1, 1, 1, 0, 0, 0}, {3, 2, 4, 0, 0, 0}, {3, 2, 4, 2, 3, 5}, {2, 0, 5, 1, 1, 1}, {0, 3, 3, 4, 4, 7},
	} {
		t.Run(fmt.Sprintf("ns=%d,wl=%d,pods=%d,extra=%d,podMetrics=%d,clusterMetrics=%d", c.ns, c.wl, c.pods, c.extra, c.podMetrics, c.clusterMetrics), func(t *testing.T) {
			t.Setenv("NUM_NAMESPACES", strconv.Itoa(c.ns))
			t.Setenv("NUM_WORKLOADS", strconv.Itoa(c.wl))
			t.Setenv("NUM_PODS", strconv.Itoa(c.pods))
			t.Setenv("NUM_EXTRA_METRICS", strconv.Itoa(c.extra))
			t.Setenv("NUM_POD_METRICS", strconv.Itoa(c.podMetrics))
			t.Setenv("NUM_CLUSTER_METRICS", strconv.Itoa(c.clusterMetrics))

			want, fits := leveledSeriesPerBlock(c.ns, c.wl, c.pods, c.extra, c.podMetrics, c.clusterMetrics)
			if !fits {
				t.Fatal("small config reported as over the cap")
			}
			blocks := collectBlocks(t, rightSizingLeveled([]time.Duration{2 * time.Hour}))
			if got := len(blocks[0].Series); got != want {
				t.Errorf("emitted %d series, projected %d", got, want)
			}
		})
	}
}

// TestRightSizingLeveledPodFillerCountsTowardCap ensures the per-pod filler is
// counted in the series cap, including values whose product would overflow int.
func TestRightSizingLeveledPodFillerCountsTowardCap(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "10")
	t.Setenv("NUM_WORKLOADS", "10")
	t.Setenv("NUM_PODS", "30")
	t.Setenv("NUM_EXTRA_METRICS", "0")
	for _, tc := range []struct{ name, podMetrics string }{
		{"6M filler series", "2000"}, // 2000 x 3000 pods
		// x 3000 pods wraps int64 to 2384, which an unbounded product would accept.
		{"overflowing product", "6148914691236518"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NUM_POD_METRICS", tc.podMetrics)
			err := rightSizingLeveled([]time.Duration{2 * time.Hour})(
				cancelledCtx(), fixedMaxTime(t), labels.Labels{}, func(BlockSpec) error {
					t.Fatal("blockEncoder must not be called when the cap is exceeded")
					return nil
				})
			if err == nil || !strings.Contains(err.Error(), "exceeds cap") {
				t.Fatalf("want a series-cap error, got %v", err)
			}
		})
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
	t.Setenv("NUM_POD_METRICS", "0")
	t.Setenv("NUM_CLUSTER_METRICS", "0")

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
	t.Setenv("NUM_POD_METRICS", "0")
	t.Setenv("NUM_CLUSTER_METRICS", "0")
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
