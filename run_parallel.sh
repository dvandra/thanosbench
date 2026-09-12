#!/bin/bash

# Parallel right-sizing generation. Override scale with environment variables:
#   NUM_CLUSTERS=10 NUM_NAMESPACES=100 NUM_WORKLOADS=10 NUM_PODS=3 ./run_parallel.sh
#
# CLUSTER_STEP controls how many clusters each worker handles (default 2).

NUM_CLUSTERS="${NUM_CLUSTERS:-10}"
CLUSTER_START="${CLUSTER_START:-1}"
CLUSTER_STEP="${CLUSTER_STEP:-2}"

generate_ranges() {
    local start=$CLUSTER_START
    local max=$NUM_CLUSTERS
    local step=$CLUSTER_STEP
    local end=$((start + step - 1))

    while [ $start -le $max ]; do
        if [ $end -gt $max ]; then
            end=$max
        fi
        echo "$start,$end"
        start=$((end + 1))
        end=$((start + step - 1))
    done
}

# Export scale vars so child run_thanosbench.sh processes inherit them.
export NUM_CLUSTERS CLUSTER_START
export NUM_NAMESPACES="${NUM_NAMESPACES:-100}"
export NUM_WORKLOADS="${NUM_WORKLOADS:-10}"
export NUM_PODS="${NUM_PODS:-3}"
export NUM_NAMES="${NUM_NAMES:-0}"
export PROFILE="${PROFILE:-custom-continous-1-week-workload-pod}"

generate_ranges | while read range; do
    ./run_thanosbench.sh "$range" &
done

wait
