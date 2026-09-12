package blockgen

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/model"
)

func TestRSDimensionSetsCardinality(t *testing.T) {
	numNamespaces, numWorkloads, numPods := 2, 2, 3

	tests := []struct {
		metric string
		want   int
	}{
		{"acm_rs:pod:cpu_request", numNamespaces * numWorkloads * numPods},
		{"acm_rs:workload:memory_usage", numNamespaces * numWorkloads},
		{"acm_rs:namespace:cpu_recommendation", numNamespaces},
		{"acm_rs:cluster:cpu_request", 1},
	}

	for _, tt := range tests {
		got := rsDimensionSets(tt.metric, numNamespaces, numWorkloads, numPods)
		if len(got) != tt.want {
			t.Errorf("%s: got %d series, want %d", tt.metric, len(got), tt.want)
		}
		if got[0].Get("__name__") != tt.metric {
			t.Errorf("%s: missing __name__ label", tt.metric)
		}
		if !sort.IsSorted(got[0]) {
			t.Errorf("%s: labels are not sorted: %v", tt.metric, got[0])
		}
	}
}

func TestRSDimensionSetsPodLabels(t *testing.T) {
	sets := rsDimensionSets("acm_rs:pod:cpu_usage", 1, 1, 1)
	if len(sets) != 1 {
		t.Fatalf("expected 1 series, got %d", len(sets))
	}
	got := sets[0]
	if got.Get("namespace") != "Namespace 0" {
		t.Errorf("namespace = %q", got.Get("namespace"))
	}
	if got.Get("workload") != "Workload 0" {
		t.Errorf("workload = %q", got.Get("workload"))
	}
	if got.Get("workload_type") != "deployment" {
		t.Errorf("workload_type = %q", got.Get("workload_type"))
	}
	if got.Get("pod") == "" {
		t.Error("pod label is empty")
	}
}

func TestWorkloadTypeFor(t *testing.T) {
	if workloadTypeFor(0) != "deployment" {
		t.Errorf("got %s", workloadTypeFor(0))
	}
	if workloadTypeFor(1) != "statefulset" {
		t.Errorf("got %s", workloadTypeFor(1))
	}
	if workloadTypeFor(2) != "daemonset" {
		t.Errorf("got %s", workloadTypeFor(2))
	}
	if workloadTypeFor(3) != "deployment" {
		t.Errorf("got %s", workloadTypeFor(3))
	}
}

func TestWorkloadPodProfileRegistered(t *testing.T) {
	if _, ok := Profiles["custom-continous-1-week-workload-pod"]; !ok {
		t.Fatal("custom-continous-1-week-workload-pod profile is missing")
	}
	if _, ok := Profiles["custom-continous-1-week-vm"]; !ok {
		t.Fatal("existing VM profile was removed")
	}
	if _, ok := Profiles["custom-continous-1-week"]; !ok {
		t.Fatal("existing namespace profile was removed")
	}
}

func TestWorkloadPodPlanSeriesCount(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "2")
	t.Setenv("NUM_WORKLOADS", "2")
	t.Setenv("NUM_PODS", "3")
	t.Setenv("MIN_GAUGE", "1")
	t.Setenv("MAX_GAUGE", "4")

	now := time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC)
	maxTime := model.TimeOrDurationValue{Time: &now}

	var specs []BlockSpec
	plan := custom_continuous_workload_pod([]time.Duration{2 * time.Hour}, 1, []string{
		"acm_rs:cluster:cpu_request",
		"acm_rs:namespace:cpu_request",
		"acm_rs:workload:cpu_request",
		"acm_rs:pod:cpu_request",
	})
	err := plan(context.Background(), maxTime, labels.FromStrings("cluster", "ac-test-man-1"), func(spec BlockSpec) error {
		specs = append(specs, spec)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 {
		t.Fatalf("got %d blocks, want 1", len(specs))
	}

	// 1 cluster + 2 namespaces + 2*2 workloads + 2*2*3 pods = 19
	want := 1 + 2 + 4 + 12
	if got := len(specs[0].Series); got != want {
		t.Fatalf("got %d series, want %d", got, want)
	}

	seen := map[string]struct{}{}
	var podSeries, workloadSeries int
	for _, s := range specs[0].Series {
		key := s.Labels.String()
		if _, ok := seen[key]; ok {
			t.Errorf("duplicate series %s", key)
		}
		seen[key] = struct{}{}
		switch s.Labels.Get("__name__") {
		case "acm_rs:pod:cpu_request":
			podSeries++
			if s.Labels.Get("pod") == "" || s.Labels.Get("workload") == "" || s.Labels.Get("namespace") == "" {
				t.Errorf("pod series missing labels: %s", key)
			}
		case "acm_rs:workload:cpu_request":
			workloadSeries++
			if s.Labels.Get("workload") == "" || s.Labels.Get("workload_type") == "" {
				t.Errorf("workload series missing labels: %s", key)
			}
		}
	}
	if podSeries != 12 {
		t.Errorf("pod series = %d, want 12", podSeries)
	}
	if workloadSeries != 4 {
		t.Errorf("workload series = %d, want 4", workloadSeries)
	}
}

func TestWorkloadPodGenerateBlock(t *testing.T) {
	t.Setenv("NUM_NAMESPACES", "1")
	t.Setenv("NUM_WORKLOADS", "1")
	t.Setenv("NUM_PODS", "2")
	t.Setenv("MIN_GAUGE", "1")
	t.Setenv("MAX_GAUGE", "4")

	now := time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC)
	maxTime := model.TimeOrDurationValue{Time: &now}

	var specs []BlockSpec
	plan := custom_continuous_workload_pod([]time.Duration{2 * time.Hour}, 1, []string{
		"acm_rs:cluster:cpu_usage",
		"acm_rs:namespace:cpu_usage",
		"acm_rs:workload:cpu_usage",
		"acm_rs:pod:cpu_usage",
	})
	if err := plan(context.Background(), maxTime, labels.FromStrings("cluster", "ac-test-man-1"), func(spec BlockSpec) error {
		specs = append(specs, spec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	id, err := Generate(context.Background(), log.NewNopLogger(), 2, dir, specs[0])
	if err != nil {
		t.Fatalf("generate block: %v", err)
	}
	blockDir := filepath.Join(dir, id.String())
	if _, err := os.Stat(filepath.Join(blockDir, "meta.json")); err != nil {
		t.Fatalf("missing meta.json: %v", err)
	}
}
