#!/usr/bin/env bash
# Generate and upload right-sizing data for any cluster range.
#
# Usage:
#   ./run_clusters.sh 1 10        # clusters ac-test-man-1 through ac-test-man-10
#   ./run_clusters.sh 11 20       # clusters ac-test-man-11 through ac-test-man-20
#   ./run_clusters.sh 5 5         # single cluster ac-test-man-5
#   ./run_clusters.sh 21 40       # clusters ac-test-man-21 through ac-test-man-40
#
# Resumable: re-run the same command if interrupted.
# Each range gets its own WORK dir (gen-work-<start>-<end>/) so multiple
# ranges can run in parallel on different machines.
set -euo pipefail

START="${1:-}"
END="${2:-}"

if [[ -z "$START" || -z "$END" ]]; then
  echo "Usage: $0 <start> <end>"
  echo "Example: $0 1 10"
  exit 2
fi

if ! [[ "$START" =~ ^[0-9]+$ && "$END" =~ ^[0-9]+$ ]]; then
  echo "Error: start and end must be integers." >&2
  exit 2
fi

if (( START > END )); then
  echo "Error: start ($START) must not be greater than end ($END)." >&2
  exit 2
fi

# Build the space-separated CLUSTERS list
CLUSTERS=""
for ((i = START; i <= END; i++)); do
  CLUSTERS="${CLUSTERS:+$CLUSTERS }ac-test-man-$i"
done

WORK_DIR="./gen-work-${START}-${END}"

export PATH="/usr/local/aws-cli:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

echo "Generating clusters $START to $END: $CLUSTERS"
echo "WORK dir: $WORK_DIR"

exec caffeinate -dimsu env -u HUB_PREFIX \
  PATH="$PATH" \
  CLUSTERS="$CLUSTERS" \
  WEEKS="${WEEKS:-26}" \
  NUM_NAMESPACES="${NUM_NAMESPACES:-100}" \
  NUM_WORKLOADS="${NUM_WORKLOADS:-10}" \
  NUM_PODS="${NUM_PODS:-3}" \
  NUM_EXTRA_METRICS="${NUM_EXTRA_METRICS:-0}" \
  NUM_POD_METRICS="${NUM_POD_METRICS:-20}" \
  NUM_CLUSTER_METRICS="${NUM_CLUSTER_METRICS:-154}" \
  RS_SCRAPE_INTERVAL="${RS_SCRAPE_INTERVAL:-15m}" \
  PROFILE="${PROFILE:-custom-continous-1-week-full}" \
  BUCKET="${BUCKET:-oia-rs}" \
  SHARED_PREFIX="${SHARED_PREFIX:-thanosbench/dvandra}" \
  PER_CLUSTER_PREFIX="${PER_CLUSTER_PREFIX:-1}" \
  AWS_PROFILE_NAME="${AWS_PROFILE_NAME:-default}" \
  MAX_INFLIGHT="${MAX_INFLIGHT:-3}" \
  WORK="$WORK_DIR" \
  MANIFEST="$WORK_DIR/manifest.final.json" \
  BASE_EPOCH_OVERRIDE="${BASE_EPOCH_OVERRIDE:-1792800000}" \
  bash demo-data/generate_and_publish.sh
