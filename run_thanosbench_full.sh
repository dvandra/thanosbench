#!/usr/bin/env bash
set -euo pipefail

# Generates ACM right-sizing blocks with FOUR aggregation levels — cluster,
# namespace, workload and pod — using the custom-continous-1-week-full profile.
#
# Unlike run_thanosbench.sh (which uses the flat namespace/cluster VM profile),
# this profile emits hierarchical per-level labels, each metric replicated across
# the three right-sizing profiles (Max OverAll/P95/P99) via a per-series `profile`
# label:
#   acm_rs:cluster:*    -> profile
#   acm_rs:namespace:*  -> namespace, profile (+ cpu/memory request_hard)
#   acm_rs:workload:*   -> namespace, workload, workload_type, profile
#   acm_rs:pod:*        -> namespace, pod, workload, workload_type, profile
#
# Usage: ./run_thanosbench_full.sh "<start>,<end>"   # inclusive cluster range
#        NUM_NAMESPACES=100 NUM_WORKLOADS=10 NUM_PODS=10 \
#          ./run_thanosbench_full.sh "1,20"
# To generate one right-sizing profile only, set RS_PROFILE to Max OverAll,
# P95, or P99. OUTPUT_ROOT can be used to keep separate profile output trees:
#   RS_PROFILE=P95 OUTPUT_ROOT=./new-run-p95 ./run_thanosbench_full.sh "1,20"

# Variables. NUM_PODS is pods per workload, not pods per namespace.
range="${1:-1}"

# Parse the range (defaults to a single cluster when omitted)
IFS=',' read -r start end <<< "$range"
start=$(printf "%d" "${start:-1}")
end=$(printf "%d" "${end:-$start}")

# Cardinality knobs (see pkg/blockgen/profiles.go: rightSizingLeveled).
# Defaults preserve the original runner behavior; override these for a
# category-sized run, e.g. 20 x 100 x 10 x 10 = 200,000 pods total.
NUM_NAMESPACES="${NUM_NAMESPACES:-40}"   # namespaces per cluster
NUM_WORKLOADS="${NUM_WORKLOADS:-10}"    # workloads per namespace
NUM_PODS="${NUM_PODS:-20}"               # pods per workload
NUM_VMS="${NUM_VMS:-50}"                 # VMs per namespace (0 = skip VM metrics)
NUM_EXTRA_METRICS="${NUM_EXTRA_METRICS:-0}" # optional synthetic filler load
RS_SCRAPE_INTERVAL="${RS_SCRAPE_INTERVAL:-1h}" # acm_rs sample interval (1h = fast; 15m = default dense)
NUM_WEEKS="${NUM_WEEKS:-26}"             # weekly periods to generate (26 ≈ 180 days)
OUTPUT_ROOT="${OUTPUT_ROOT:-./new-run}"

if [[ -n "${RS_PROFILE:-}" && "${RS_PROFILE}" != "Max OverAll" && "${RS_PROFILE}" != "P95" && "${RS_PROFILE}" != "P99" ]]; then
  echo "RS_PROFILE must be one of: Max OverAll, P95, P99" >&2
  exit 2
fi
MIN_GAUGE_MIN="${MIN_GAUGE_MIN:-2.1}"
MIN_GAUGE_MAX="${MIN_GAUGE_MAX:-4.6}"
MAX_GAUGE_MIN="${MAX_GAUGE_MIN:-10.6}"
MAX_GAUGE_MAX="${MAX_GAUGE_MAX:-19.8}"

# Compute MAX_TIMES dynamically: NUM_WEEKS weekly dates ending near today.
# END_DATE defaults to today; override to pin the newest week boundary.
END_DATE="${END_DATE:-$(date +%Y-%m-%d)}"
MAX_TIMES=()
for ((w = NUM_WEEKS - 1; w >= 0; w--)); do
  days_back=$((w * 7))
  if date -v-1d >/dev/null 2>&1; then
    # macOS date
    MAX_TIMES+=("$(date -j -v-${days_back}d -f "%Y-%m-%d" "$END_DATE" "+%Y-%m-%dT00:00:00Z")")
  else
    # GNU/Linux date
    MAX_TIMES+=("$(date -d "$END_DATE - ${days_back} days" "+%Y-%m-%dT00:00:00Z")")
  fi
done

# Function to generate a random number in the given range
random_in_range() {
  local min=$1
  local max=$2
  echo $(awk -v min=$min -v max=$max 'BEGIN{srand(); print min + rand() * (max - min)}')
}

# Loop through clusters and weeks to generate blocks
for ((cluster = start; cluster <= end; cluster++)); do
    for i in $(seq 0 $((NUM_WEEKS - 1))); do
      PROFILE="custom-continous-1-week-full"
      MAX_TIME=${MAX_TIMES[$i]}

      MIN_GAUGE=$(random_in_range "$MIN_GAUGE_MIN" "$MIN_GAUGE_MAX")
      MAX_GAUGE=$(random_in_range "$MAX_GAUGE_MIN" "$MAX_GAUGE_MAX")

      # Output directory based on cluster
      OUTPUT_DIR="$OUTPUT_ROOT/cluster-${cluster}"

      mkdir -p "$OUTPUT_DIR"

      # Generate the block plan and blocks.
      # cluster / aggregation are applied as block (external) labels; `profile`
      # is emitted per-series by the profile (Max OverAll/P95/P99), so it must
      # NOT be passed via --labels here.
      NUM_NAMESPACES=$NUM_NAMESPACES NUM_WORKLOADS=$NUM_WORKLOADS NUM_PODS=$NUM_PODS \
        NUM_VMS=$NUM_VMS NUM_EXTRA_METRICS=$NUM_EXTRA_METRICS RS_SCRAPE_INTERVAL=$RS_SCRAPE_INTERVAL \
        MIN_GAUGE=$MIN_GAUGE MAX_GAUGE=$MAX_GAUGE \
        ./thanosbench block plan -p "$PROFILE" \
        --labels "cluster=\"ac-test-man-${cluster}\"" \
        --labels "aggregation=\"1d\"" \
        --max-time "$MAX_TIME" \
        | ./thanosbench block gen --output.dir "$OUTPUT_DIR" --workers 20
    done
done

echo "Block generation completed for clusters $start to $end (namespaces=$NUM_NAMESPACES, workloads=$NUM_WORKLOADS, pods=$NUM_PODS, VMs=$NUM_VMS)."
