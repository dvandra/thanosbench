package blockgen

import (
	"math"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanosbench/pkg/seriesgen"
)

// collectSamples drives a blockSeriesSet to completion and returns the generated
// sample values keyed by each series' __name__.
func collectSamples(t *testing.T, series []SeriesSpec, extLset labels.Labels) map[string][]float64 {
	t.Helper()
	set := &blockSeriesSet{config: BlockSpec{Series: series}, extLset: extLset}
	got := map[string][]float64{}
	for set.Next() {
		s := set.At()
		name := s.Labels().Get(labels.MetricName)
		it := s.Iterator()
		for it.Next() {
			_, v := it.At()
			got[name] = append(got[name], v)
		}
		if err := it.Err(); err != nil {
			t.Fatalf("iterator error for %s: %v", name, err)
		}
	}
	if err := set.Err(); err != nil {
		t.Fatalf("series set error: %v", err)
	}
	return got
}

// TestSeedNameValueScaleCorrelate is the engine-level proof for the right-sizing
// recommendation gap: a series that borrows a sibling's seed (SeedName) and
// scales it (ValueScale) yields exactly factor * sibling at every sample, rather
// than an independent random draw.
func TestSeedNameValueScaleCorrelate(t *testing.T) {
	mint := int64(0)
	maxt := durToMilis(time.Hour)
	chars := seriesgen.Characteristics{
		Min:            2,
		Max:            8,
		Jitter:         3,
		ScrapeInterval: 5 * time.Minute,
		ChangeInterval: 10 * time.Minute,
	}

	usage := SeriesSpec{
		Labels:          labels.FromStrings(labels.MetricName, "acm_rs:namespace:cpu_usage", "namespace", "ns-0"),
		Targets:         1,
		Type:            Gauge,
		MinTime:         mint,
		MaxTime:         maxt,
		Characteristics: chars,
	}
	// Identical to usage except __name__, seeded from usage and scaled by 1.10.
	rec := usage
	rec.Labels = labels.FromStrings(labels.MetricName, "acm_rs:namespace:cpu_recommendation", "namespace", "ns-0")
	rec.SeedName = "acm_rs:namespace:cpu_usage"
	rec.ValueScale = 1.10

	got := collectSamples(t, []SeriesSpec{usage, rec}, labels.Labels{})
	u := got["acm_rs:namespace:cpu_usage"]
	r := got["acm_rs:namespace:cpu_recommendation"]

	if len(u) == 0 {
		t.Fatal("usage produced no samples")
	}
	if len(u) != len(r) {
		t.Fatalf("sample count mismatch: usage=%d recommendation=%d", len(u), len(r))
	}
	for i := range u {
		if want := u[i] * 1.10; math.Abs(r[i]-want) > 1e-9 {
			t.Fatalf("sample %d: recommendation=%v, want usage*1.10=%v (usage=%v)", i, r[i], want, u[i])
		}
	}
	// Sanity: the two series must not be trivially identical (scale actually applied).
	if math.Abs(r[0]-u[0]) < 1e-12 {
		t.Fatalf("recommendation equals usage; ValueScale not applied (both %v)", u[0])
	}
}

// TestValueScaleUnsetLeavesValuesUnscaled confirms the default (zero) ValueScale
// is a no-op, so existing profiles are unaffected.
func TestValueScaleUnsetLeavesValuesUnscaled(t *testing.T) {
	mint := int64(0)
	maxt := durToMilis(time.Hour)
	chars := seriesgen.Characteristics{Min: 2, Max: 8, ScrapeInterval: 5 * time.Minute, ChangeInterval: 10 * time.Minute}

	plain := SeriesSpec{
		Labels:          labels.FromStrings(labels.MetricName, "m", "namespace", "ns-0"),
		Targets:         1,
		Type:            Gauge,
		MinTime:         mint,
		MaxTime:         maxt,
		Characteristics: chars,
	}
	// Same series, but explicitly seeded from itself and scaled 1.0 — must match.
	scaledOne := plain
	scaledOne.Labels = labels.FromStrings(labels.MetricName, "m2", "namespace", "ns-0")
	scaledOne.SeedName = "m"
	scaledOne.ValueScale = 1.0

	got := collectSamples(t, []SeriesSpec{plain, scaledOne}, labels.Labels{})
	a, b := got["m"], got["m2"]
	if len(a) != len(b) || len(a) == 0 {
		t.Fatalf("sample count mismatch: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("sample %d: %v != %v (ValueScale=1 or unset should be a no-op)", i, a[i], b[i])
		}
	}
}
