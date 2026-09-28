#!/usr/bin/env bash
set -euo pipefail

# Edit only this section for future runs, then execute:
#   ./run_category1.sh

NUM_CLUSTERS=100
CLUSTER_START=5
NUM_NAMESPACES=100
NUM_WORKLOADS=10
NUM_PODS=3
NUM_EXTRA_METRICS=150
NUM_WEEKS=13

# Optional gauge ranges. Leave these unchanged unless you need different values.
MIN_GAUGE_MIN=2.1
MIN_GAUGE_MAX=4.6
MAX_GAUGE_MIN=10.6
MAX_GAUGE_MAX=19.8

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_END=$((CLUSTER_START + NUM_CLUSTERS - 1))

echo "Generating:"
echo "  clusters: ${CLUSTER_START}-${CLUSTER_END}"
echo "  namespaces/cluster: ${NUM_NAMESPACES}"
echo "  workloads/namespace: ${NUM_WORKLOADS}"
echo "  pods/workload: ${NUM_PODS}"
echo "  extra metrics: ${NUM_EXTRA_METRICS}"
echo "  weekly periods: ${NUM_WEEKS}"
echo "  total pod identities: $((NUM_CLUSTERS * NUM_NAMESPACES * NUM_WORKLOADS * NUM_PODS))"

cd "$SCRIPT_DIR"

# Always rebuild so the binary matches the current profile/source configuration.
make build

NUM_NAMESPACES="$NUM_NAMESPACES" \
NUM_WORKLOADS="$NUM_WORKLOADS" \
NUM_PODS="$NUM_PODS" \
NUM_EXTRA_METRICS="$NUM_EXTRA_METRICS" \
NUM_WEEKS="$NUM_WEEKS" \
MIN_GAUGE_MIN="$MIN_GAUGE_MIN" \
MIN_GAUGE_MAX="$MIN_GAUGE_MAX" \
MAX_GAUGE_MIN="$MAX_GAUGE_MIN" \
MAX_GAUGE_MAX="$MAX_GAUGE_MAX" \
./run_thanosbench_full.sh "${CLUSTER_START},${CLUSTER_END}"
