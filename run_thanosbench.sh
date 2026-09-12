#!/bin/bash

# Right-sizing block generation. Override any of these with environment variables:
#   NUM_CLUSTERS=10 NUM_NAMESPACES=100 NUM_WORKLOADS=10 NUM_PODS=3 ./run_thanosbench.sh
#
# Optional: pass a cluster index range as the first argument (used by run_parallel.sh):
#   ./run_thanosbench.sh 1,5

# Scale (defaults: 10 clusters, 100 namespaces, 10 workloads, 3 pods per workload)
NUM_CLUSTERS="${NUM_CLUSTERS:-10}"
NUM_NAMESPACES="${NUM_NAMESPACES:-100}"
NUM_WORKLOADS="${NUM_WORKLOADS:-10}"
NUM_PODS="${NUM_PODS:-3}"
NUM_NAMES="${NUM_NAMES:-0}"
CLUSTER_START="${CLUSTER_START:-1}"
PROFILE="${PROFILE:-custom-continous-1-week-workload-pod}"

range="$1"
if [ -n "$range" ]; then
  IFS=',' read -r start end <<< "$range"
  start=$(printf "%d" "$start")
  end=$(printf "%d" "$end")
else
  start=$CLUSTER_START
  end=$NUM_CLUSTERS
fi

MAX_TIMES=("2024-12-07T00:00:00Z" "2024-12-14T00:00:00Z" "2024-12-21T00:00:00Z" "2024-12-28T00:00:00Z" "2025-01-04T00:00:00Z" "2025-01-11T00:00:00Z" "2025-01-18T00:00:00Z" "2025-01-25T00:00:00Z" "2025-02-01T00:00:00Z" "2025-02-08T00:00:00Z" "2025-02-15T00:00:00Z" "2025-02-22T00:00:00Z" "2025-03-01T00:00:00Z" "2025-03-08T00:00:00Z" "2025-03-15T00:00:00Z" "2025-03-22T00:00:00Z")

random_in_range() {
  local min=$1
  local max=$2
  echo $(awk -v min=$min -v max=$max 'BEGIN{srand(); print min + rand() * (max - min)}')
}

for ((cluster = start; cluster <= end; cluster++)); do
    for i in "${!MAX_TIMES[@]}"; do
      MAX_TIME=${MAX_TIMES[$i]}

      MIN_GAUGE=$(random_in_range 2.1 4.6)
      MAX_GAUGE=$(random_in_range 10.6 19.8)

      OUTPUT_DIR="./new-run/cluster-${cluster}"

      mkdir -p "$OUTPUT_DIR"

      NUM_NAMESPACES=$NUM_NAMESPACES \
      NUM_WORKLOADS=$NUM_WORKLOADS \
      NUM_PODS=$NUM_PODS \
      NUM_NAMES=$NUM_NAMES \
      MIN_GAUGE=$MIN_GAUGE \
      MAX_GAUGE=$MAX_GAUGE \
      ./thanosbench block plan -p "$PROFILE" \
        --labels "instance=\"bench\"" \
        --labels "cluster=\"ac-test-man-${cluster}\"" \
        --labels "container=\"bench\"" \
        --labels "resource=\"cpu\"" \
        --labels "clusterType=\"bench\"" \
        --labels "mode=\"idle\"" \
        --labels "profile=\"Max OverAll\"" \
        --max-time "$MAX_TIME" \
        | ./thanosbench block gen --output.dir "$OUTPUT_DIR" --workers 20
    done
done

echo "Block generation completed for clusters from $start to $end using profile $PROFILE ($NUM_NAMESPACES namespaces, $NUM_WORKLOADS workloads, $NUM_PODS pods)."
