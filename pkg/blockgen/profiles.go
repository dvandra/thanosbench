package blockgen

import (
	"context"
	"fmt"
	"math/rand" // Import the rand package
	"os"
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
		}, 1, []string{"kubevirt_vm_running_status_last", "acm_rs:namespace:cpu_request", "acm_rs:namespace:cpu_usage",
			"acm_rs:namespace:memory_request", "acm_rs:namespace:memory_usage",
			"acm_rs:namespace:cpu_recommendation", "acm_rs:namespace:memory_recommendation",
			"acm_rs:cluster:cpu_request", "acm_rs:cluster:cpu_usage",
			"acm_rs:cluster:memory_request", "acm_rs:cluster:memory_usage",
			"acm_rs:cluster:cpu_recommendation", "acm_rs:cluster:memory_recommendation", "extra_metric_1",
			"extra_metric_2", "extra_metric_3", "extra_metric_4", "extra_metric_5", "extra_metric_6",
			"extra_metric_7", "extra_metric_8", "extra_metric_9", "extra_metric_10", "extra_metric_11",
			"extra_metric_12", "extra_metric_13", "extra_metric_14", "extra_metric_15", "extra_metric_16",
			"extra_metric_17", "extra_metric_18", "extra_metric_19", "extra_metric_20", "extra_metric_21",
			"extra_metric_22", "extra_metric_23", "extra_metric_24", "extra_metric_25", "extra_metric_26",
			"extra_metric_27", "extra_metric_28", "extra_metric_29", "extra_metric_30", "extra_metric_31",
			"extra_metric_32", "extra_metric_33", "extra_metric_34", "extra_metric_35", "extra_metric_36",
			"extra_metric_37", "extra_metric_38", "extra_metric_39", "extra_metric_40", "extra_metric_41",
			"extra_metric_42", "extra_metric_43", "extra_metric_44", "extra_metric_45", "extra_metric_46",
			"extra_metric_47", "extra_metric_48", "extra_metric_49", "extra_metric_50", "extra_metric_51",
			"extra_metric_52", "extra_metric_53", "extra_metric_54", "extra_metric_55", "extra_metric_56",
			"extra_metric_57", "extra_metric_58", "extra_metric_59", "extra_metric_60", "extra_metric_61",
			"extra_metric_62", "extra_metric_63", "extra_metric_64", "extra_metric_65", "extra_metric_66",
			"extra_metric_67", "extra_metric_68", "extra_metric_69", "extra_metric_70", "extra_metric_71",
			"extra_metric_72", "extra_metric_73", "extra_metric_74", "extra_metric_75", "extra_metric_76",
			"extra_metric_77", "extra_metric_78", "extra_metric_79", "extra_metric_80", "extra_metric_81",
			"extra_metric_82", "extra_metric_83", "extra_metric_84", "extra_metric_85", "extra_metric_86",
			"extra_metric_87", "extra_metric_88", "extra_metric_89", "extra_metric_90", "extra_metric_91",
			"extra_metric_92", "extra_metric_93", "extra_metric_94", "extra_metric_95", "extra_metric_96",
			"extra_metric_97", "extra_metric_98", "extra_metric_99", "extra_metric_100", "extra_metric_101",
			"extra_metric_102", "extra_metric_103", "extra_metric_104", "extra_metric_105", "extra_metric_106",
			"extra_metric_107", "extra_metric_108", "extra_metric_109", "extra_metric_110", "extra_metric_111",
			"extra_metric_112", "extra_metric_113", "extra_metric_114", "extra_metric_115", "extra_metric_116",
			"extra_metric_117", "extra_metric_118", "extra_metric_119", "extra_metric_120", "extra_metric_121",
			"extra_metric_122", "extra_metric_123", "extra_metric_124", "extra_metric_125", "extra_metric_126",
			"extra_metric_127", "extra_metric_128", "extra_metric_129", "extra_metric_130", "extra_metric_131",
			"extra_metric_132", "extra_metric_133", "extra_metric_134", "extra_metric_135", "extra_metric_136",
			"extra_metric_137", "extra_metric_138", "extra_metric_139", "extra_metric_140", "extra_metric_141",
			"extra_metric_142", "extra_metric_143", "extra_metric_144", "extra_metric_145", "extra_metric_146",
			"extra_metric_147", "extra_metric_148", "extra_metric_149", "extra_metric_150", "extra_metric_151",
			"extra_metric_152", "extra_metric_153", "extra_metric_154", "extra_metric_155", "extra_metric_156",
			"extra_metric_157", "extra_metric_158", "extra_metric_159", "extra_metric_160", "extra_metric_161",
			"extra_metric_162", "extra_metric_163", "extra_metric_164", "extra_metric_165", "extra_metric_166",
			"extra_metric_167", "extra_metric_168", "extra_metric_169", "extra_metric_170", "extra_metric_171",
			"extra_metric_172", "extra_metric_173", "extra_metric_174", "extra_metric_175", "extra_metric_176",
			"extra_metric_177", "extra_metric_178", "extra_metric_179", "extra_metric_180", "extra_metric_181",
			"extra_metric_182", "extra_metric_183", "extra_metric_184", "extra_metric_185", "extra_metric_186",
			"extra_metric_187", "extra_metric_188", "extra_metric_189", "extra_metric_190", "extra_metric_191",
			"extra_metric_192", "extra_metric_193", "extra_metric_194", "extra_metric_195", "extra_metric_196",
			"extra_metric_197", "extra_metric_198", "extra_metric_199", "extra_metric_200"}),
		
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
		}, 1, []string{"kubevirt_vm_running_status_last_transition_timestamp_seconds", "acm_rs_vm:namespace:cpu_request", "acm_rs_vm:namespace:cpu_usage",
			"acm_rs_vm:namespace:memory_request", "acm_rs_vm:namespace:memory_usage",
			"acm_rs_vm:namespace:cpu_recommendation", "acm_rs_vm:namespace:memory_recommendation",
			"acm_rs_vm:cluster:cpu_request", "acm_rs_vm:cluster:cpu_usage",
			"acm_rs_vm:cluster:memory_request", "acm_rs_vm:cluster:memory_usage",
			"acm_rs_vm:cluster:cpu_recommendation", "acm_rs_vm:cluster:memory_recommendation", 
			"acm_rs:namespace:cpu_request", "acm_rs:namespace:cpu_usage",
			"acm_rs:namespace:memory_request", "acm_rs:namespace:memory_usage",
			"acm_rs:namespace:cpu_recommendation", "acm_rs:namespace:memory_recommendation",
			"acm_rs:cluster:cpu_request", "acm_rs:cluster:cpu_usage",
			"acm_rs:cluster:memory_request", "acm_rs:cluster:memory_usage",
			"acm_rs:cluster:cpu_recommendation", "acm_rs:cluster:memory_recommendation",
			"extra_metric_1",
			"extra_metric_2", "extra_metric_3", "extra_metric_4", "extra_metric_5", "extra_metric_6",
			"extra_metric_7", "extra_metric_8", "extra_metric_9", "extra_metric_10", "extra_metric_11",
			"extra_metric_12", "extra_metric_13", "extra_metric_14", "extra_metric_15", "extra_metric_16",
			"extra_metric_17", "extra_metric_18", "extra_metric_19", "extra_metric_20", "extra_metric_21",
			"extra_metric_22", "extra_metric_23", "extra_metric_24", "extra_metric_25", "extra_metric_26",
			"extra_metric_27", "extra_metric_28", "extra_metric_29", "extra_metric_30", "extra_metric_31",
			"extra_metric_32", "extra_metric_33", "extra_metric_34", "extra_metric_35", "extra_metric_36",
			"extra_metric_37", "extra_metric_38", "extra_metric_39", "extra_metric_40", "extra_metric_41",
			"extra_metric_42", "extra_metric_43", "extra_metric_44", "extra_metric_45", "extra_metric_46",
			"extra_metric_47", "extra_metric_48", "extra_metric_49", "extra_metric_50", "extra_metric_51",
			"extra_metric_52", "extra_metric_53", "extra_metric_54", "extra_metric_55", "extra_metric_56",
			"extra_metric_57", "extra_metric_58", "extra_metric_59", "extra_metric_60", "extra_metric_61",
			"extra_metric_62", "extra_metric_63", "extra_metric_64", "extra_metric_65", "extra_metric_66",
			"extra_metric_67", "extra_metric_68", "extra_metric_69", "extra_metric_70", "extra_metric_71",
			"extra_metric_72", "extra_metric_73", "extra_metric_74", "extra_metric_75", "extra_metric_76",
			"extra_metric_77", "extra_metric_78", "extra_metric_79", "extra_metric_80", "extra_metric_81",
			"extra_metric_82", "extra_metric_83", "extra_metric_84", "extra_metric_85", "extra_metric_86",
			"extra_metric_87", "extra_metric_88", "extra_metric_89", "extra_metric_90", "extra_metric_91",
			"extra_metric_92", "extra_metric_93", "extra_metric_94", "extra_metric_95", "extra_metric_96",
			"extra_metric_97", "extra_metric_98", "extra_metric_99", "extra_metric_100", "extra_metric_101",
			"extra_metric_102", "extra_metric_103", "extra_metric_104", "extra_metric_105", "extra_metric_106",
			"extra_metric_107", "extra_metric_108", "extra_metric_109", "extra_metric_110", "extra_metric_111",
			"extra_metric_112", "extra_metric_113", "extra_metric_114", "extra_metric_115", "extra_metric_116",
			"extra_metric_117", "extra_metric_118", "extra_metric_119", "extra_metric_120", "extra_metric_121",
			"extra_metric_122", "extra_metric_123", "extra_metric_124", "extra_metric_125", "extra_metric_126",
			"extra_metric_127", "extra_metric_128", "extra_metric_129", "extra_metric_130", "extra_metric_131",
			"extra_metric_132", "extra_metric_133", "extra_metric_134", "extra_metric_135", "extra_metric_136",
			"extra_metric_137", "extra_metric_138", "extra_metric_139", "extra_metric_140", "extra_metric_141",
			"extra_metric_142", "extra_metric_143", "extra_metric_144", "extra_metric_145", "extra_metric_146",
			"extra_metric_147", "extra_metric_148", "extra_metric_149", "extra_metric_150", "extra_metric_151",
			"extra_metric_152", "extra_metric_153", "extra_metric_154", "extra_metric_155", "extra_metric_156",
			"extra_metric_157", "extra_metric_158", "extra_metric_159", "extra_metric_160", "extra_metric_161",
			"extra_metric_162", "extra_metric_163", "extra_metric_164", "extra_metric_165", "extra_metric_166",
			"extra_metric_167", "extra_metric_168", "extra_metric_169", "extra_metric_170", "extra_metric_171",
			"extra_metric_172", "extra_metric_173", "extra_metric_174", "extra_metric_175", "extra_metric_176",
			"extra_metric_177", "extra_metric_178", "extra_metric_179", "extra_metric_180", "extra_metric_181",
			"extra_metric_182", "extra_metric_183", "extra_metric_184", "extra_metric_185", "extra_metric_186",
			"extra_metric_187", "extra_metric_188", "extra_metric_189", "extra_metric_190", "extra_metric_191",
			"extra_metric_192", "extra_metric_193", "extra_metric_194", "extra_metric_195", "extra_metric_196",
			"extra_metric_197", "extra_metric_198", "extra_metric_199", "extra_metric_200"}),

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

		// minGauge and maxGauge from environment variables
		minGauge, err := strconv.ParseFloat(os.Getenv("MIN_GAUGE"), 64)
		if err != nil {
			minGauge = 2.0
		}

		maxGauge, err := strconv.ParseFloat(os.Getenv("MAX_GAUGE"), 64)
		if err != nil {
			maxGauge = 8.0
		}

		numNamespacesStr := os.Getenv("NUM_NAMESPACES")
		if numNamespacesStr == "" {
			numNamespacesStr = "50"
		}

		numNamespaces, err := strconv.Atoi(numNamespacesStr)
		if err != nil {
			numNamespaces = 0
		}

		numNamesStr := os.Getenv("NUM_NAMES")
		if numNamesStr == "" {
			numNamesStr = "200"
		}

		numNames, err := strconv.Atoi(numNamesStr)
		if err != nil {
			numNames = 0
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

			// Append specific metric names and namespaces
			for _, metric := range metrics {
				for i := 0; i < numNamespaces; i++ {

					namespace := fmt.Sprintf("Namespace %d", i)

					for j := 0; j < numNames; j++ {

						name := fmt.Sprintf("Name %d", j)

						// Check if the metric matches the "extra_metric_*" pattern
						var s SeriesSpec
						if strings.HasPrefix(metric, "extra_metric_") {
							s = extraMetricSpec
						} else {
							s = defaultMetricSpec
						}

						s.Labels = labels.Labels{
							{Name: "__name__", Value: metric},
							{Name: "namespace", Value: namespace},
							{Name: "name", Value: name},
						}

						s.MinTime = mint
						s.MaxTime = maxt
						b.Series = append(b.Series, s)

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
