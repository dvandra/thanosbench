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
#        ./run_thanosbench_full.sh "1,3"

# Variables
range="${1:-1}"

# Parse the range (defaults to a single cluster when omitted)
IFS=',' read -r start end <<< "$range"
start=$(printf "%d" "${start:-1}")
end=$(printf "%d" "${end:-$start}")

# Cardinality knobs (see pkg/blockgen/profiles.go: rightSizingLeveled).
NUM_NAMESPACES=40   # namespaces per cluster
NUM_WORKLOADS=10    # workloads per namespace
NUM_PODS=20         # pods per workload (pods nest under their workload)
NUM_EXTRA_METRICS=0 # optional synthetic filler load; 0 keeps blocks lean

MAX_TIMES=("2024-12-07T00:00:00Z" "2024-12-14T00:00:00Z" "2024-12-21T00:00:00Z" "2024-12-28T00:00:00Z" "2025-01-04T00:00:00Z" "2025-01-11T00:00:00Z" "2025-01-18T00:00:00Z" "2025-01-25T00:00:00Z" "2025-02-01T00:00:00Z" "2025-02-08T00:00:00Z" "2025-02-15T00:00:00Z" "2025-02-22T00:00:00Z" "2025-03-01T00:00:00Z" "2025-03-08T00:00:00Z" "2025-03-15T00:00:00Z" "2025-03-22T00:00:00Z")

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

      MIN_GAUGE=$(random_in_range 2.1 4.6)
      MAX_GAUGE=$(random_in_range 10.6 19.8)

      # Output directory based on cluster
      OUTPUT_DIR="./new-run/cluster-${cluster}"

      mkdir -p "$OUTPUT_DIR"

      # Generate the block plan and blocks.
      # cluster / aggregation are applied as block (external) labels; `profile`
      # is emitted per-series by the profile (Max OverAll/P95/P99), so it must
      # NOT be passed via --labels here.
      NUM_NAMESPACES=$NUM_NAMESPACES NUM_WORKLOADS=$NUM_WORKLOADS NUM_PODS=$NUM_PODS \
        NUM_EXTRA_METRICS=$NUM_EXTRA_METRICS MIN_GAUGE=$MIN_GAUGE MAX_GAUGE=$MAX_GAUGE \
        ./thanosbench block plan -p "$PROFILE" \
        --labels "cluster=\"ac-test-man-${cluster}\"" \
        --labels "aggregation=\"1d\"" \
        --max-time "$MAX_TIME" \
        | ./thanosbench block gen --output.dir "$OUTPUT_DIR" --workers 20
    done
done

echo "Block generation completed for clusters $start to $end (namespaces=$NUM_NAMESPACES, workloads=$NUM_WORKLOADS, pods=$NUM_PODS)."
