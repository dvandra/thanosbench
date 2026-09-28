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
NUM_WORKLOADS="${NUM_WORKLOADS:-10}"     # workloads per namespace
NUM_PODS="${NUM_PODS:-20}"               # pods per workload
NUM_EXTRA_METRICS="${NUM_EXTRA_METRICS:-0}" # optional synthetic filler load; 0 keeps blocks lean
NUM_POD_METRICS="${NUM_POD_METRICS:-0}"  # optional per-pod filler
NUM_CLUSTER_METRICS="${NUM_CLUSTER_METRICS:-0}" # optional cluster-level filler
MIN_GAUGE_MIN="${MIN_GAUGE_MIN:-2.1}"
MIN_GAUGE_MAX="${MIN_GAUGE_MAX:-4.6}"
MAX_GAUGE_MIN="${MAX_GAUGE_MIN:-10.6}"
MAX_GAUGE_MAX="${MAX_GAUGE_MAX:-19.8}"

# Weekly --max-time values: the WEEKS weeks ending at END_EPOCH (default: today
# 00:00 UTC), oldest first. Keeping the data recent keeps it inside the hub's
# retention (MCO default 365d); older blocks are deleted by the compactor.
WEEKS="${WEEKS:-16}"
END_EPOCH="${END_EPOCH:-$(( ( $(date -u +%s) / 86400 ) * 86400 ))}"
iso() { date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ; }
MAX_TIMES=()
for (( w = WEEKS - 1; w >= 0; w-- )); do MAX_TIMES+=("$(iso $(( END_EPOCH - w * 7 * 86400 )))"); done

# Function to generate a random number in the given range
random_in_range() {
  local min=$1
  local max=$2
  echo $(awk -v min=$min -v max=$max 'BEGIN{srand(); print min + rand() * (max - min)}')
}

# Loop through clusters and weeks to generate blocks
for ((cluster = start; cluster <= end; cluster++)); do
    for i in "${!MAX_TIMES[@]}"; do
      PROFILE="custom-continous-1-week-full"
      MAX_TIME=${MAX_TIMES[$i]}

      MIN_GAUGE=$(random_in_range "$MIN_GAUGE_MIN" "$MIN_GAUGE_MAX")
      MAX_GAUGE=$(random_in_range "$MAX_GAUGE_MIN" "$MAX_GAUGE_MAX")

      # Output directory based on cluster
      OUTPUT_DIR="./new-run/cluster-${cluster}"

      mkdir -p "$OUTPUT_DIR"

      # Generate the block plan and blocks.
      # cluster / aggregation are applied as block (external) labels; `profile`
      # is emitted per-series by the profile (Max OverAll/P95/P99), so it must
      # NOT be passed via --labels here.
      NUM_NAMESPACES=$NUM_NAMESPACES NUM_WORKLOADS=$NUM_WORKLOADS NUM_PODS=$NUM_PODS \
        NUM_EXTRA_METRICS=$NUM_EXTRA_METRICS NUM_POD_METRICS=$NUM_POD_METRICS NUM_CLUSTER_METRICS=$NUM_CLUSTER_METRICS \
        MIN_GAUGE=$MIN_GAUGE MAX_GAUGE=$MAX_GAUGE \
        ./thanosbench block plan -p "$PROFILE" \
        --labels "cluster=\"ac-test-man-${cluster}\"" \
        --labels "aggregation=\"1d\"" \
        --max-time "$MAX_TIME" \
        | ./thanosbench block gen --output.dir "$OUTPUT_DIR" --workers 20
    done
done

echo "Block generation completed for clusters $start to $end (namespaces=$NUM_NAMESPACES, workloads=$NUM_WORKLOADS, pods=$NUM_PODS)."
