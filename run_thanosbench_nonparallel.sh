#!/bin/bash

# Sequential right-sizing generation. Override scale with environment variables:
#   NUM_CLUSTERS=10 NUM_NAMESPACES=100 NUM_WORKLOADS=10 NUM_PODS=3 ./run_thanosbench_nonparallel.sh

NUM_CLUSTERS="${NUM_CLUSTERS:-10}"
NUM_NAMESPACES="${NUM_NAMESPACES:-100}"
NUM_WORKLOADS="${NUM_WORKLOADS:-10}"
NUM_PODS="${NUM_PODS:-3}"
PROFILE="${PROFILE:-custom-continous-1-week-workload-pod}"
MAX_TIMES=("2024-06-07T00:00:00Z")

random_in_range() {
  local min=$1
  local max=$2
  echo $(awk -v min=$min -v max=$max 'BEGIN{srand(); print min + rand() * (max - min)}')
}

for cluster in $(seq 1 $NUM_CLUSTERS); do
    for i in ${!MAX_TIMES[@]}; do
      MAX_TIME=${MAX_TIMES[$i]}

      MIN_GAUGE=$(random_in_range 2.1 4.6)
      MAX_GAUGE=$(random_in_range 10.6 19.8)

      OUTPUT_DIR="./new-run/week-$((i + 1))"

      mkdir -p $OUTPUT_DIR

      NUM_NAMESPACES=$NUM_NAMESPACES \
      NUM_WORKLOADS=$NUM_WORKLOADS \
      NUM_PODS=$NUM_PODS \
      MIN_GAUGE=$MIN_GAUGE \
      MAX_GAUGE=$MAX_GAUGE \
      ./thanosbench block plan -p $PROFILE \
        --labels "instance=\"bench\"" \
        --labels "cluster=\"ac-test-man-${cluster}\"" \
        --labels "container=\"bench\"" \
        --labels "resource=\"cpu\"" \
        --labels "clusterType=\"bench\"" \
        --labels "mode=\"idle\"" \
        --labels "profile=\"Max OverAll\"" \
        --max-time $MAX_TIME \
        | ./thanosbench block gen --output.dir $OUTPUT_DIR --workers 20
    done
done

echo "Block generation completed for $NUM_CLUSTERS clusters, $NUM_NAMESPACES namespaces, $NUM_WORKLOADS workloads, and $NUM_PODS pods."
