package blockgen

import (
	"context"
	"fmt"
	"math/rand" // Import the rand package
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanosbench/pkg/seriesgen"
)

type PlanFn func(ctx context.Context, maxTime model.TimeOrDurationValue, extLset labels.Labels, blockEncoder func(BlockSpec) error) error
type ProfileMap map[string]PlanFn

func (p ProfileMap) Keys() (keys []string) {
	for k := range p {
		keys = append(keys, k)
	}
	return keys
}

// maxSeriesPerBlock caps the number of series a custom profile is allowed to plan
// for a single block. The block writer accumulates a whole block in memory before
// flushing, so an over-large NUM_* combination OOMs the process; we fail fast with
// a clear message on stderr instead. The value leaves generous headroom over the
// documented "50 namespaces x 200 names" ceiling of the existing profiles (~2.25M).
const maxSeriesPerBlock = 5_000_000

// cardScope is the aggregation scope of a metric emitted by custom_continuous. It
// determines which breakdown labels the series carries and therefore its
// cardinality, so cluster/namespace roll-ups are not inflated with dimensions
// they should not have.
type cardScope int

const (
	// scopeFlat emits one series per namespace x name. Used for the VM namespace
	// level (which legitimately carries a per-VM `name`), the kubevirt metric and
	// the extra_metric_* filler.
	scopeFlat cardScope = iota
	// scopeCluster emits a single series; the cluster/profile identity comes from
	// the block (external) labels.
	scopeCluster
	// scopeNamespace emits one series per namespace, carrying only `namespace`.
	scopeNamespace
)

// scopeFor classifies a metric name into its aggregation scope. Cluster-level
// metrics (acm_rs:cluster:* and acm_rs_vm:cluster:*) collapse to a single series;
// non-VM namespace metrics carry only `namespace`; everything else keeps the flat
// namespace x name shape.
func scopeFor(metric string) cardScope {
	switch {
	case strings.Contains(metric, ":cluster:"):
		return scopeCluster
	case strings.HasPrefix(metric, "acm_rs:namespace:"):
		return scopeNamespace
	default:
		return scopeFlat
	}
}

// rsProfiles are the recording-rule "profile" variants the ACM right-sizing
// dashboards expose in their $profile dropdown (label_values(...,profile)). In
// real data `profile` is a per-series label emitted by the recording rules, so
// each acm_rs*/acm_rs_vm* metric is generated once per profile with a `profile`
// breakdown label rather than relying on a single block-level --labels value.
var rsProfiles = []string{"Max OverAll", "P95", "P99"}

// recommendationRatio is the headroom the ACM right-sizing rules apply over
// observed usage (recommendation = usage * 110/100). A *_recommendation series
// is seeded from its *_usage sibling and scaled by this factor so the two track
// together instead of being independent random draws (see SeriesSpec.SeedName /
// ValueScale).
const recommendationRatio = 1.10

// defaultMemMin / defaultMemMax are the byte-scale gauge bounds used for memory_*
// measures when MEM_MIN_GAUGE / MEM_MAX_GAUGE are unset. ACM memory right-sizing
// metrics are reported in bytes, so a cores-scale range (MIN_GAUGE/MAX_GAUGE,
// single digits) would render them as a handful of bytes; these defaults place
// them at realistic MB/GB magnitudes (512 MiB to 32 GiB).
const (
	defaultMemMin = 512 * 1024 * 1024       // 512 MiB
	defaultMemMax = 32 * 1024 * 1024 * 1024 // 32 GiB
)

// memJitterFraction sizes the per-sample jitter of memory gauges as a fraction of
// their (max-min) byte range, so byte-scale series still show realistic movement
// (a fixed cores-scale jitter of a few units is invisible at gigabyte levels).
const memJitterFraction = 0.05

// isMemoryMetric reports whether a fully-qualified metric name is a memory gauge
// (…:memory_*), which is measured in bytes and therefore drawn from the
// byte-scale range rather than the cores-scale MIN_GAUGE/MAX_GAUGE.
func isMemoryMetric(metric string) bool {
	return strings.Contains(metric, "memory")
}

// isRightSizingMetric reports whether a metric belongs to the ACM right-sizing
// recording-rule families (acm_rs:* / acm_rs_vm:*) that carry a per-series
// `profile` label. kubevirt_* and extra_metric_* filler do not.
func isRightSizingMetric(metric string) bool {
	return strings.HasPrefix(metric, "acm_rs:") || strings.HasPrefix(metric, "acm_rs_vm:")
}

// usageSibling maps a *_recommendation metric to its *_usage counterpart (same
// family and aggregation level) so the recommendation series can share the
// usage series' value stream. It returns false for non-recommendation metrics.
func usageSibling(metric string) (string, bool) {
	const suffix = "_recommendation"
	if strings.HasSuffix(metric, suffix) {
		return strings.TrimSuffix(metric, suffix) + "_usage", true
	}
	return "", false
}

// measureSpec returns a copy of base configured for the given fully-qualified
// metric name:
//   - memory_* measures are rescaled to the byte-scale range [memMin, memMax]
//     (with proportional jitter) so they land at MB/GB magnitudes instead of a
//     handful of bytes;
//   - a *_recommendation metric is wired to derive from its *_usage sibling
//     (SeedName) scaled by recommendationRatio (ValueScale);
//   - every other metric keeps base unchanged.
//
// Because a memory recommendation and its usage sibling both receive the same
// (memory) characteristics and share an RNG seed, recommendation = usage * ratio
// still holds exactly.
func measureSpec(base SeriesSpec, metric string, memMin, memMax float64) SeriesSpec {
	if isMemoryMetric(metric) {
		base.Characteristics.Min = memMin
		base.Characteristics.Max = memMax
		base.Characteristics.Jitter = (memMax - memMin) * memJitterFraction
	}
	if sib, ok := usageSibling(metric); ok {
		base.SeedName = sib
		base.ValueScale = recommendationRatio
	}
	return base
}

// profilesFor returns the per-series profile values a metric should be emitted
// under: the three right-sizing profiles for acm_rs* families, or a single
// empty value (no profile label) for everything else.
func profilesFor(metric string) []string {
	if isRightSizingMetric(metric) {
		return rsProfiles
	}
	return []string{""}
}

var (
	Profiles = ProfileMap{
		// Let's say we have 100 applications, 50 metrics each. All rollout every 1h.
		// This makes 2h block to have 15k series, 8h block 45k, 2d block to have 245k series.
		"realistic-k8s-2d-small": realisticK8s([]time.Duration{
			// Two days, from newest to oldest, in the same way Thanos compactor would do.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			2 * time.Hour,
		}, 1*time.Hour, 100, 50),
		"realistic-k8s-1w-small": realisticK8s([]time.Duration{
			// One week, from newest to oldest, in the same way Thanos compactor would do.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			2 * time.Hour,
		}, 1*time.Hour, 100, 50),
		"realistic-k8s-30d-tiny": realisticK8s([]time.Duration{
			// 30 days, from newest to oldest.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			2 * time.Hour,
		}, 1*time.Hour, 1, 5),
		"realistic-k8s-365d-tiny": realisticK8s([]time.Duration{
			// 1y days, from newest to oldest.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			67 * 24 * time.Hour,
			67 * 24 * time.Hour,
			67 * 24 * time.Hour,
			67 * 24 * time.Hour,
			67 * 24 * time.Hour,
		}, 1*time.Hour, 1, 5),
		"continuous-1w-small": continuous([]time.Duration{
			// One week, from newest to oldest, in the same way Thanos compactor would do.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			2 * time.Hour,
			// 10,000 series per block.
		}, 100, 100),
		"continuous-30d-tiny": continuous([]time.Duration{
			// 30 days, from newest to oldest.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			2 * time.Hour,
		}, 1, 5),
		"continuous-365d-tiny": continuous([]time.Duration{
			// 1y days, from newest to oldest.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			176 * time.Hour,
			67 * 24 * time.Hour,
			67 * 24 * time.Hour,
			67 * 24 * time.Hour,
			67 * 24 * time.Hour,
			67 * 24 * time.Hour,
		}, 1, 5),
		"continuous-1w-1series-10000apps": continuous([]time.Duration{
			// One week, from newest to oldest, in the same way Thanos compactor would do.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			2 * time.Hour,
			// 10,000 series per block.
		}, 10000, 1),

		// custom-continous-1-week emits the ACM right-sizing namespace/cluster
		// metrics (non-VM) plus NUM_EXTRA_METRICS synthetic filler series. The
		// filler is generated by custom_continuous itself (default 200) so it no
		// longer has to be inlined here.
		"custom-continous-1-week": custom_continuous([]time.Duration{
			// One week, from newest to oldest, in the same way Thanos compactor would do.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			2 * time.Hour,
		}, 1, []string{
			"kubevirt_vm_running_status_last",
			// namespace level (request_hard is the ResourceQuota ceiling the
			// namespaces dashboard's "hard limit" panels query)
			"acm_rs:namespace:cpu_request", "acm_rs:namespace:cpu_usage",
			"acm_rs:namespace:memory_request", "acm_rs:namespace:memory_usage",
			"acm_rs:namespace:cpu_recommendation", "acm_rs:namespace:memory_recommendation",
			"acm_rs:namespace:cpu_request_hard", "acm_rs:namespace:memory_request_hard",
			// cluster level
			"acm_rs:cluster:cpu_request", "acm_rs:cluster:cpu_usage",
			"acm_rs:cluster:memory_request", "acm_rs:cluster:memory_usage",
			"acm_rs:cluster:cpu_recommendation", "acm_rs:cluster:memory_recommendation",
		}),

		// custom-continous-1-week-vm additionally emits the VM (acm_rs_vm:*)
		// namespace/cluster metrics. Filler is generated by custom_continuous
		// (NUM_EXTRA_METRICS, default 200).
		"custom-continous-1-week-vm": custom_continuous([]time.Duration{
			// One week, from newest to oldest, in the same way Thanos compactor would do.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			2 * time.Hour,
		}, 1, []string{
			"kubevirt_vm_running_status_last_transition_timestamp_seconds",
			// VM namespace level
			"acm_rs_vm:namespace:cpu_request", "acm_rs_vm:namespace:cpu_usage",
			"acm_rs_vm:namespace:memory_request", "acm_rs_vm:namespace:memory_usage",
			"acm_rs_vm:namespace:cpu_recommendation", "acm_rs_vm:namespace:memory_recommendation",
			// VM cluster level
			"acm_rs_vm:cluster:cpu_request", "acm_rs_vm:cluster:cpu_usage",
			"acm_rs_vm:cluster:memory_request", "acm_rs_vm:cluster:memory_usage",
			"acm_rs_vm:cluster:cpu_recommendation", "acm_rs_vm:cluster:memory_recommendation",
			// namespace level (request_hard is the ResourceQuota ceiling the
			// namespaces dashboard's "hard limit" panels query)
			"acm_rs:namespace:cpu_request", "acm_rs:namespace:cpu_usage",
			"acm_rs:namespace:memory_request", "acm_rs:namespace:memory_usage",
			"acm_rs:namespace:cpu_recommendation", "acm_rs:namespace:memory_recommendation",
			"acm_rs:namespace:cpu_request_hard", "acm_rs:namespace:memory_request_hard",
			// cluster level
			"acm_rs:cluster:cpu_request", "acm_rs:cluster:cpu_usage",
			"acm_rs:cluster:memory_request", "acm_rs:cluster:memory_usage",
			"acm_rs:cluster:cpu_recommendation", "acm_rs:cluster:memory_recommendation",
		}),

		// Retain the workload/pod profile introduced on workload-pos-rs for
		// compatibility with its scripts and callers.
		"custom-continous-1-week-workload-pod": custom_continuous_workload_pod([]time.Duration{
			// One week, from newest to oldest, in the same way Thanos compactor would do.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			2 * time.Hour,
		}, 1, []string{
			"acm_rs:cluster:cpu_request", "acm_rs:cluster:cpu_usage",
			"acm_rs:cluster:memory_request", "acm_rs:cluster:memory_usage",
			"acm_rs:cluster:cpu_recommendation", "acm_rs:cluster:memory_recommendation",
			"acm_rs:namespace:cpu_request", "acm_rs:namespace:cpu_usage",
			"acm_rs:namespace:memory_request", "acm_rs:namespace:memory_usage",
			"acm_rs:namespace:cpu_recommendation", "acm_rs:namespace:memory_recommendation",
			"acm_rs:workload:cpu_request", "acm_rs:workload:cpu_usage",
			"acm_rs:workload:memory_request", "acm_rs:workload:memory_usage",
			"acm_rs:workload:cpu_recommendation", "acm_rs:workload:memory_recommendation",
			"acm_rs:pod:cpu_request", "acm_rs:pod:cpu_usage",
			"acm_rs:pod:memory_request", "acm_rs:pod:memory_usage",
			"acm_rs:pod:cpu_recommendation", "acm_rs:pod:memory_recommendation",
		}),

		// custom-continous-1-week-full adds two aggregation levels (workload and
		// pod) on top of the namespace and cluster levels, using the hierarchical
		// label model the acm_rs recording rules / dashboards expect.
		"custom-continous-1-week-full": rightSizingLeveled([]time.Duration{
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			48 * time.Hour,
			2 * time.Hour,
		}),

		// custom-continous-3-day-full is the leveled right-sizing profile
		// (rightSizingLeveled) over a 3-day (72h) window instead of a week, laid
		// out newest-to-oldest the way the Thanos compactor would. Same label model
		// and cardinality knobs as custom-continous-1-week-full.
		"custom-continous-3-day-full": rightSizingLeveled([]time.Duration{
			// 72h total = 3 days.
			2 * time.Hour,
			2 * time.Hour,
			2 * time.Hour,
			8 * time.Hour,
			8 * time.Hour,
			24 * time.Hour,
			24 * time.Hour,
			2 * time.Hour,
		}),
	}
)

// func profileFactory(profileType string, blockDurationList []int, rolloutInterval int, apps int, metricsPerApp int, customMetrics []string) PlanFn {
// 	var ranges []time.Duration
// 	for _, item := range blockDurationList {
// 		ranges = append(ranges, time.Duration(item)*time.Hour)
// 	}
// 	switch profileType {
// 	case "realisticK8s":
// 		return realisticK8s(ranges, time.Duration(rolloutInterval), apps, metricsPerApp)
// 	case "continuous":
// 		return continuous(ranges, apps, metricsPerApp)
// 	case "custom":
// 		return custom_continuous(ranges, apps, customMetrics)
// 	}
// }

// getEnvInt returns the integer value of environment variable key. When the
// variable is unset or empty the provided default is returned. A value that is
// present but not a valid non-negative integer is a fatal configuration error:
// the block plan is written to stdout, so silently substituting a wrong
// cardinality would corrupt the generated data set. Fail loudly instead.
func getEnvInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: must be an integer: %w", key, v, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid %s=%d: must be >= 0", key, n)
	}
	return n, nil
}

// getEnvFloat mirrors getEnvInt for float-valued environment variables (e.g.
// MIN_GAUGE / MAX_GAUGE). Unset/empty yields the default; a non-empty but
// unparseable value is a fatal error rather than a silent fallback.
func getEnvFloat(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: must be a number: %w", key, v, err)
	}
	return f, nil
}

func realisticK8s(ranges []time.Duration, rolloutInterval time.Duration, apps int, metricsPerApp int) PlanFn {
	return func(ctx context.Context, maxTime model.TimeOrDurationValue, extLset labels.Labels, blockEncoder func(BlockSpec) error) error {

		// Align timestamps as Prometheus would do.
		maxt := rangeForTimestamp(maxTime.PrometheusTimestamp(), durToMilis(2*time.Hour))

		// Track "rollouts". In heavy used K8s we have rollouts e.g every hour if not more. Account for that.
		lastRollout := maxt - (durToMilis(rolloutInterval) / 2)

		// All our series are gauges.
		common := SeriesSpec{
			Targets: apps,
			Type:    Gauge,
			Characteristics: seriesgen.Characteristics{
				Max:            200000000,
				Min:            10000000,
				Jitter:         30000000,
				ScrapeInterval: 15 * time.Second,
				ChangeInterval: 1 * time.Hour,
			},
		}

		for _, r := range ranges {
			mint := maxt - durToMilis(r) + 1

			b := BlockSpec{
				Meta: metadata.Meta{
					BlockMeta: tsdb.BlockMeta{
						MaxTime:    maxt,
						MinTime:    mint,
						Compaction: tsdb.BlockMetaCompaction{Level: 1},
						Version:    1,
					},
					Thanos: metadata.Thanos{
						Labels:     extLset.Map(),
						Downsample: metadata.ThanosDownsample{Resolution: 0},
						Source:     "blockgen",
					},
				},
			}
			for {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				smaxt := lastRollout + durToMilis(rolloutInterval)
				if smaxt > maxt {
					smaxt = maxt
				}

				smint := lastRollout
				if smint < mint {
					smint = mint
				}

				for i := 0; i < metricsPerApp; i++ {
					s := common

					s.Labels = labels.Labels{
						// TODO(bwplotka): Use different label for metricPerApp cardinality and stable number.
						{Name: "__name__", Value: fmt.Sprintf("k8s_app_metric%d", i)},
						{Name: "next_rollout_time", Value: timestamp.Time(lastRollout).String()},
					}
					s.MinTime = smint
					s.MaxTime = smaxt
					b.Series = append(b.Series, s)
				}

				if lastRollout <= mint {
					break
				}

				lastRollout -= durToMilis(rolloutInterval)
			}

			if err := blockEncoder(b); err != nil {
				return err
			}
			maxt = mint
		}
		return nil
	}
}

func custom_continuous(ranges []time.Duration, apps int, metrics []string) PlanFn {
	return func(ctx context.Context, maxTime model.TimeOrDurationValue, extLset labels.Labels, blockEncoder func(BlockSpec) error) error {

		// Gauge value range and cardinality from the environment. Unset/empty
		// falls back to the default; a present-but-invalid value fails fast.
		minGauge, err := getEnvFloat("MIN_GAUGE", 2.0)
		if err != nil {
			return err
		}
		maxGauge, err := getEnvFloat("MAX_GAUGE", 8.0)
		if err != nil {
			return err
		}
		// Memory measures are byte-scale (MB/GB), so they use their own range
		// rather than the cores-scale MIN_GAUGE/MAX_GAUGE.
		memMin, err := getEnvFloat("MEM_MIN_GAUGE", defaultMemMin)
		if err != nil {
			return err
		}
		memMax, err := getEnvFloat("MEM_MAX_GAUGE", defaultMemMax)
		if err != nil {
			return err
		}
		numNamespaces, err := getEnvInt("NUM_NAMESPACES", 50)
		if err != nil {
			return err
		}
		numNames, err := getEnvInt("NUM_NAMES", 200)
		if err != nil {
			return err
		}
		// NUM_EXTRA_METRICS controls the "extra_metric_<i>" filler load that used
		// to be inlined per profile. Default 200 preserves the previous output.
		numExtra, err := getEnvInt("NUM_EXTRA_METRICS", 200)
		if err != nil {
			return err
		}

		// Full metric list: caller-provided core metrics followed by the synthetic
		// filler series (previously duplicated across every profile definition).
		allMetrics := make([]string, 0, len(metrics)+numExtra)
		allMetrics = append(allMetrics, metrics...)
		for i := 1; i <= numExtra; i++ {
			allMetrics = append(allMetrics, fmt.Sprintf("extra_metric_%d", i))
		}

		// Each metric is emitted according to its aggregation scope (see scopeFor):
		// cluster-level metrics collapse to a single series, non-VM namespace
		// metrics carry only `namespace`, and everything else keeps the flat
		// namespace x name shape. acm_rs*/acm_rs_vm* metrics are additionally
		// emitted once per right-sizing profile (Max OverAll/P95/P99), so their
		// per-scope count is multiplied by len(rsProfiles). Project the per-block
		// series count up front and refuse a run that would blow past the
		// in-memory block cap.
		projected := 0
		for _, m := range allMetrics {
			var base int
			switch scopeFor(m) {
			case scopeCluster:
				base = 1
			case scopeNamespace:
				base = numNamespaces
			default:
				base = numNamespaces * numNames
			}
			projected += base * len(profilesFor(m))
		}
		fmt.Fprintf(os.Stderr, "custom_continuous: %d core + %d filler metric(s), %d namespaces x %d names x %d profiles -> %d series/block\n",
			len(metrics), numExtra, numNamespaces, numNames, len(rsProfiles), projected)
		if projected > maxSeriesPerBlock {
			return fmt.Errorf("projected %d series/block exceeds cap %d: lower NUM_NAMESPACES/NUM_NAMES/NUM_EXTRA_METRICS", projected, maxSeriesPerBlock)
		}

		// Generate a random Jitter value between 1 and 10
		randomJitter := rand.Intn(10) + 1

		// Align timestamps as Prometheus would do
		maxt := rangeForTimestamp(maxTime.PrometheusTimestamp(), durToMilis(2*time.Hour))

		// SeriesSpec for "extra_metric_*" metrics
		extraMetricSpec := SeriesSpec{
			Targets: apps,
			Type:    Gauge,
			Characteristics: seriesgen.Characteristics{
				Max:            maxGauge,
				Min:            minGauge,
				Jitter:         float64(randomJitter),
				ScrapeInterval: 5 * time.Minute,
				ChangeInterval: 10 * time.Minute,
			},
		}

		// SeriesSpec for core metrics
		defaultMetricSpec := SeriesSpec{
			Targets: apps,
			Type:    Gauge,
			Characteristics: seriesgen.Characteristics{
				Max:            maxGauge,
				Min:            minGauge,
				Jitter:         float64(randomJitter),
				ScrapeInterval: 15 * time.Minute,
				ChangeInterval: 10 * time.Minute,
			},
		}

		for _, r := range ranges {
			mint := maxt - durToMilis(r) + 1

			if ctx.Err() != nil {
				return ctx.Err()
			}

			b := BlockSpec{
				Meta: metadata.Meta{
					BlockMeta: tsdb.BlockMeta{
						MaxTime:    maxt,
						MinTime:    mint,
						Compaction: tsdb.BlockMetaCompaction{Level: 1},
						Version:    1,
					},
					Thanos: metadata.Thanos{
						Labels:     extLset.Map(),
						Downsample: metadata.ThanosDownsample{Resolution: 0},
						Source:     "blockgen",
					},
				},
			}

			// Append each metric with the breakdown labels appropriate to its
			// aggregation scope. acm_rs*/acm_rs_vm* metrics are emitted once per
			// right-sizing profile (with a `profile` breakdown label); other
			// metrics are emitted once with no profile label. *_recommendation
			// metrics are seeded from their *_usage sibling and scaled so they
			// track usage (see measureSpec).
			for _, metric := range allMetrics {
				spec := measureSpec(defaultMetricSpec, metric, memMin, memMax)
				if strings.HasPrefix(metric, "extra_metric_") {
					spec = extraMetricSpec
				}

				// emit appends one series: __name__ + breakdown + optional profile,
				// sorted into canonical label order for a clean TSDB index.
				emit := func(breakdown labels.Labels, profile string) {
					s := spec
					ls := make(labels.Labels, 0, len(breakdown)+2)
					ls = append(ls, labels.Label{Name: "__name__", Value: metric})
					ls = append(ls, breakdown...)
					if profile != "" {
						ls = append(ls, labels.Label{Name: "profile", Value: profile})
					}
					sort.Sort(ls)
					s.Labels = ls
					s.MinTime = mint
					s.MaxTime = maxt
					b.Series = append(b.Series, s)
				}

				for _, profile := range profilesFor(metric) {
					switch scopeFor(metric) {
					case scopeCluster:
						// Single series; cluster identity comes from block labels.
						emit(nil, profile)
					case scopeNamespace:
						for i := 0; i < numNamespaces; i++ {
							emit(labels.Labels{
								{Name: "namespace", Value: fmt.Sprintf("Namespace %d", i)},
							}, profile)
						}
					default:
						// Flat namespace x name (VM namespace, kubevirt, filler).
						for i := 0; i < numNamespaces; i++ {
							namespace := fmt.Sprintf("Namespace %d", i)
							for j := 0; j < numNames; j++ {
								emit(labels.Labels{
									{Name: "namespace", Value: namespace},
									{Name: "name", Value: fmt.Sprintf("Name %d", j)},
								}, profile)
							}
						}
					}
				}
			}

			if err := blockEncoder(b); err != nil {
				return err
			}
			maxt = mint
		}
		return nil
	}
}

// rsWorkloadTypes are the workload kinds cycled across the synthetic workload and
// pod series, matching the PascalCase values Kubernetes uses (and the
// kubernetes-mixin `workload_type` convention).
var rsWorkloadTypes = []string{"Deployment", "StatefulSet", "DaemonSet", "ReplicaSet"}

// rsMeasures are the six right-sizing gauges emitted at every aggregation level.
var rsMeasures = []string{
	"cpu_request", "cpu_usage", "cpu_recommendation",
	"memory_request", "memory_usage", "memory_recommendation",
}

// rightSizingLeveled generates ACM right-sizing gauges across four aggregation
// levels — cluster, namespace, workload and pod — each carrying the per-level
// breakdown labels the acm_rs recording rules / dashboards expect:
//
//	acm_rs:cluster:*    profile
//	acm_rs:namespace:*  namespace, profile
//	acm_rs:workload:*   namespace, workload, workload_type, profile
//	acm_rs:pod:*        namespace, pod, workload, workload_type, profile  (pods nest under workloads)
//
// Every acm_rs metric is emitted once per right-sizing profile (Max OverAll/P95/
// P99) with `profile` as a per-series label, matching how the recording rules
// label the real data (so the dashboards' $profile dropdown is populated). The
// namespace level additionally carries cpu/memory request_hard (the ResourceQuota
// ceiling the namespaces dashboard queries). Each *_recommendation series is
// seeded from its *_usage sibling and scaled by recommendationRatio so it tracks
// usage instead of being an independent random draw.
//
// The cluster/aggregation identity is supplied as block (external) labels by the
// caller via `block plan --labels`; profile is per-series and must NOT also be
// passed via --labels.
//
// Per-measure series per block (before the profile multiplier):
//
//	cluster:   1
//	namespace: NUM_NAMESPACES
//	workload:  NUM_NAMESPACES * NUM_WORKLOADS
//	pod:       NUM_NAMESPACES * NUM_WORKLOADS * NUM_PODS
//
// An optional NUM_EXTRA_METRICS filler load (off by default for this profile) can
// be layered on for cardinality/load testing.
func rightSizingLeveled(ranges []time.Duration) PlanFn {
	return func(ctx context.Context, maxTime model.TimeOrDurationValue, extLset labels.Labels, blockEncoder func(BlockSpec) error) error {

		minGauge, err := getEnvFloat("MIN_GAUGE", 2.0)
		if err != nil {
			return err
		}
		maxGauge, err := getEnvFloat("MAX_GAUGE", 8.0)
		if err != nil {
			return err
		}
		// Memory measures are byte-scale (MB/GB), so they use their own range
		// rather than the cores-scale MIN_GAUGE/MAX_GAUGE.
		memMin, err := getEnvFloat("MEM_MIN_GAUGE", defaultMemMin)
		if err != nil {
			return err
		}
		memMax, err := getEnvFloat("MEM_MAX_GAUGE", defaultMemMax)
		if err != nil {
			return err
		}
		numNamespaces, err := getEnvInt("NUM_NAMESPACES", 50)
		if err != nil {
			return err
		}
		numWorkloads, err := getEnvInt("NUM_WORKLOADS", 10)
		if err != nil {
			return err
		}
		numPods, err := getEnvInt("NUM_PODS", 20)
		if err != nil {
			return err
		}
		numExtra, err := getEnvInt("NUM_EXTRA_METRICS", 0)
		if err != nil {
			return err
		}

		// namespace level additionally carries the ResourceQuota "hard limit"
		// gauges (cpu/memory request_hard) the namespaces dashboard queries.
		rsNamespaceMeasures := append(append([]string{}, rsMeasures...),
			"cpu_request_hard", "memory_request_hard")

		perMeasure := 1 + numNamespaces + numNamespaces*numWorkloads + numNamespaces*numWorkloads*numPods
		// request_hard exists at the namespace level only.
		namespaceHard := numNamespaces * (len(rsNamespaceMeasures) - len(rsMeasures))
		// acm_rs series are emitted once per profile; filler is not.
		projected := len(rsProfiles)*(len(rsMeasures)*perMeasure+namespaceHard) + numExtra*numNamespaces
		fmt.Fprintf(os.Stderr, "rightSizingLeveled: %d namespaces x %d workloads x %d pods x %d profiles (+%d filler) = %d series/block\n",
			numNamespaces, numWorkloads, numPods, len(rsProfiles), numExtra, projected)
		if projected > maxSeriesPerBlock {
			return fmt.Errorf("projected %d series/block exceeds cap %d: lower NUM_NAMESPACES/NUM_WORKLOADS/NUM_PODS/NUM_EXTRA_METRICS", projected, maxSeriesPerBlock)
		}

		randomJitter := rand.Intn(10) + 1
		maxt := rangeForTimestamp(maxTime.PrometheusTimestamp(), durToMilis(2*time.Hour))

		coreSpec := SeriesSpec{
			Targets: 1,
			Type:    Gauge,
			Characteristics: seriesgen.Characteristics{
				Max:            maxGauge,
				Min:            minGauge,
				Jitter:         float64(randomJitter),
				ScrapeInterval: 15 * time.Minute,
				ChangeInterval: 10 * time.Minute,
			},
		}
		extraSpec := coreSpec
		extraSpec.Characteristics.ScrapeInterval = 5 * time.Minute

		for _, r := range ranges {
			mint := maxt - durToMilis(r) + 1

			if ctx.Err() != nil {
				return ctx.Err()
			}

			b := BlockSpec{
				Meta: metadata.Meta{
					BlockMeta: tsdb.BlockMeta{
						MaxTime:    maxt,
						MinTime:    mint,
						Compaction: tsdb.BlockMetaCompaction{Level: 1},
						Version:    1,
					},
					Thanos: metadata.Thanos{
						Labels:     extLset.Map(),
						Downsample: metadata.ThanosDownsample{Resolution: 0},
						Source:     "blockgen",
					},
				},
			}

			// add appends one series with __name__=name plus the given breakdown
			// labels; the whole set is sorted so the TSDB index stays canonical.
			// spec carries any recommendation seeding/scaling (see measureSpec).
			add := func(name string, breakdown labels.Labels, spec SeriesSpec) {
				ls := make(labels.Labels, 0, len(breakdown)+1)
				ls = append(ls, labels.Label{Name: "__name__", Value: name})
				ls = append(ls, breakdown...)
				sort.Sort(ls)
				spec.Labels = ls
				spec.MinTime = mint
				spec.MaxTime = maxt
				b.Series = append(b.Series, spec)
			}
			// addRS emits an acm_rs measure at a level under one profile, wiring the
			// recommendation-from-usage derivation from the fully-qualified name.
			addRS := func(name string, breakdown labels.Labels, profile string) {
				add(name, append(breakdown, labels.Label{Name: "profile", Value: profile}), measureSpec(coreSpec, name, memMin, memMax))
			}

			for _, profile := range rsProfiles {
				for _, m := range rsMeasures {
					// cluster level: one series per profile; cluster identity comes
					// from the block labels.
					addRS("acm_rs:cluster:"+m, nil, profile)
				}

				for ni := 0; ni < numNamespaces; ni++ {
					ns := fmt.Sprintf("namespace-%d", ni)

					// namespace level (core measures + request_hard).
					for _, m := range rsNamespaceMeasures {
						addRS("acm_rs:namespace:"+m, labels.Labels{
							{Name: "namespace", Value: ns},
						}, profile)
					}

					for wi := 0; wi < numWorkloads; wi++ {
						wl := fmt.Sprintf("workload-%d", wi)
						wt := rsWorkloadTypes[wi%len(rsWorkloadTypes)]

						for _, m := range rsMeasures {
							// workload level
							addRS("acm_rs:workload:"+m, labels.Labels{
								{Name: "namespace", Value: ns},
								{Name: "workload", Value: wl},
								{Name: "workload_type", Value: wt},
							}, profile)
						}

						for pi := 0; pi < numPods; pi++ {
							pod := fmt.Sprintf("%s-%d", wl, pi)

							for _, m := range rsMeasures {
								// pod level (nested under its workload)
								addRS("acm_rs:pod:"+m, labels.Labels{
									{Name: "namespace", Value: ns},
									{Name: "pod", Value: pod},
									{Name: "workload", Value: wl},
									{Name: "workload_type", Value: wt},
								}, profile)
							}
						}
					}
				}
			}

			// Optional filler load (NUM_EXTRA_METRICS), one series per namespace.
			// Filler is not right-sizing data, so it carries no profile label.
			for e := 1; e <= numExtra; e++ {
				name := fmt.Sprintf("extra_metric_%d", e)
				for ni := 0; ni < numNamespaces; ni++ {
					add(name, labels.Labels{
						{Name: "namespace", Value: fmt.Sprintf("namespace-%d", ni)},
					}, extraSpec)
				}
			}

			if err := blockEncoder(b); err != nil {
				return err
			}
			maxt = mint
		}
		return nil
	}
}

func envInt(key string, defaultVal int) int {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return n
}

func envFloat(key string, defaultVal float64) float64 {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return defaultVal
	}
	return n
}

var workloadTypes = []string{"deployment", "statefulset", "daemonset"}

func workloadTypeFor(index int) string {
	return workloadTypes[index%len(workloadTypes)]
}

// rsDimensionSets returns one label set per series for a right-sizing metric.
// Cluster metrics are a single series (cluster is an external label).
// Namespace metrics fan out by namespace.
// Workload metrics fan out by namespace × workload.
// Pod metrics fan out by namespace × workload × pod.
func rsDimensionSets(metric string, numNamespaces, numWorkloads, numPods int) []labels.Labels {
	base := labels.Label{Name: "__name__", Value: metric}
	switch {
	case strings.Contains(metric, ":pod:"):
		sets := make([]labels.Labels, 0, numNamespaces*numWorkloads*numPods)
		for ns := 0; ns < numNamespaces; ns++ {
			namespace := fmt.Sprintf("Namespace %d", ns)
			for w := 0; w < numWorkloads; w++ {
				workload := fmt.Sprintf("Workload %d", w)
				wType := workloadTypeFor(w)
				for p := 0; p < numPods; p++ {
					pod := fmt.Sprintf("workload-%d-%s-%d", w, wType, p)
					sets = append(sets, labels.New(
						base,
						labels.Label{Name: "namespace", Value: namespace},
						labels.Label{Name: "pod", Value: pod},
						labels.Label{Name: "workload", Value: workload},
						labels.Label{Name: "workload_type", Value: wType},
					))
				}
			}
		}
		return sets
	case strings.Contains(metric, ":workload:"):
		sets := make([]labels.Labels, 0, numNamespaces*numWorkloads)
		for ns := 0; ns < numNamespaces; ns++ {
			namespace := fmt.Sprintf("Namespace %d", ns)
			for w := 0; w < numWorkloads; w++ {
				workload := fmt.Sprintf("Workload %d", w)
				wType := workloadTypeFor(w)
				sets = append(sets, labels.New(
					base,
					labels.Label{Name: "namespace", Value: namespace},
					labels.Label{Name: "workload", Value: workload},
					labels.Label{Name: "workload_type", Value: wType},
				))
			}
		}
		return sets
	case strings.Contains(metric, ":namespace:"):
		sets := make([]labels.Labels, 0, numNamespaces)
		for ns := 0; ns < numNamespaces; ns++ {
			sets = append(sets, labels.New(
				base,
				labels.Label{Name: "namespace", Value: fmt.Sprintf("Namespace %d", ns)},
			))
		}
		return sets
	default:
		return []labels.Labels{labels.New(base)}
	}
}

// custom_continuous_workload_pod generates cluster/namespace/workload/pod right-sizing
// series. Cardinality is controlled by environment variables:
//
//	NUM_NAMESPACES (default 100)  — namespaces per cluster
//	NUM_WORKLOADS  (default 10)   — workloads per namespace
//	NUM_PODS       (default 3)    — pods per workload
//	MIN_GAUGE / MAX_GAUGE         — gauge value range
func custom_continuous_workload_pod(ranges []time.Duration, apps int, metrics []string) PlanFn {
	return func(ctx context.Context, maxTime model.TimeOrDurationValue, extLset labels.Labels, blockEncoder func(BlockSpec) error) error {
		minGauge := envFloat("MIN_GAUGE", 2.0)
		maxGauge := envFloat("MAX_GAUGE", 8.0)
		numNamespaces := envInt("NUM_NAMESPACES", 100)
		numWorkloads := envInt("NUM_WORKLOADS", 10)
		numPods := envInt("NUM_PODS", 3)
		randomJitter := rand.Intn(10) + 1

		maxt := rangeForTimestamp(maxTime.PrometheusTimestamp(), durToMilis(2*time.Hour))

		common := SeriesSpec{
			Targets: apps,
			Type:    Gauge,
			Characteristics: seriesgen.Characteristics{
				Max:            maxGauge,
				Min:            minGauge,
				Jitter:         float64(randomJitter),
				ScrapeInterval: 15 * time.Minute,
				ChangeInterval: 10 * time.Minute,
			},
		}

		for _, r := range ranges {
			mint := maxt - durToMilis(r) + 1

			if ctx.Err() != nil {
				return ctx.Err()
			}

			b := BlockSpec{
				Meta: metadata.Meta{
					BlockMeta: tsdb.BlockMeta{
						MaxTime:    maxt,
						MinTime:    mint,
						Compaction: tsdb.BlockMetaCompaction{Level: 1},
						Version:    1,
					},
					Thanos: metadata.Thanos{
						Labels:     extLset.Map(),
						Downsample: metadata.ThanosDownsample{Resolution: 0},
						Source:     "blockgen",
					},
				},
			}

			for _, metric := range metrics {
				for _, lset := range rsDimensionSets(metric, numNamespaces, numWorkloads, numPods) {
					s := common
					s.Labels = lset
					s.MinTime = mint
					s.MaxTime = maxt
					b.Series = append(b.Series, s)
				}
			}

			if err := blockEncoder(b); err != nil {
				return err
			}
			maxt = mint
		}
		return nil
	}
}

func continuous(ranges []time.Duration, apps int, metricsPerApp int) PlanFn {
	return func(ctx context.Context, maxTime model.TimeOrDurationValue, extLset labels.Labels, blockEncoder func(BlockSpec) error) error {

		// Align timestamps as Prometheus would do.
		maxt := rangeForTimestamp(maxTime.PrometheusTimestamp(), durToMilis(2*time.Hour))

		// All our series are gauges.
		common := SeriesSpec{
			Targets: apps,
			Type:    Gauge,
			Characteristics: seriesgen.Characteristics{
				Max:            200000000,
				Min:            10000000,
				Jitter:         30000000,
				ScrapeInterval: 15 * time.Second,
				ChangeInterval: 1 * time.Hour,
			},
		}

		for _, r := range ranges {
			mint := maxt - durToMilis(r) + 1

			if ctx.Err() != nil {
				return ctx.Err()
			}

			b := BlockSpec{
				Meta: metadata.Meta{
					BlockMeta: tsdb.BlockMeta{
						MaxTime:    maxt,
						MinTime:    mint,
						Compaction: tsdb.BlockMetaCompaction{Level: 1},
						Version:    1,
					},
					Thanos: metadata.Thanos{
						Labels:     extLset.Map(),
						Downsample: metadata.ThanosDownsample{Resolution: 0},
						Source:     "blockgen",
					},
				},
			}
			for i := 0; i < metricsPerApp; i++ {
				s := common

				s.Labels = labels.Labels{
					{Name: "__name__", Value: fmt.Sprintf("continuous_app_metric%d", i)},
				}
				s.MinTime = mint
				s.MaxTime = maxt
				b.Series = append(b.Series, s)
			}

			if err := blockEncoder(b); err != nil {
				return err
			}
			maxt = mint
		}
		return nil
	}
}

func rangeForTimestamp(t int64, width int64) (maxt int64) {
	return (t/width)*width + width
}
