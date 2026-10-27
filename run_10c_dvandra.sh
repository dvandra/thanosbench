#!/usr/bin/env bash
# Generate and upload right-sizing data for 10 clusters to s3://oia-rs/thanosbench/dvandra/
# Usage: ./run_10c_dvandra.sh
# Resumable: re-run the same command if interrupted.
set -euo pipefail

export PATH="/usr/local/aws-cli:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

exec caffeinate -dimsu env -u HUB_PREFIX \
  PATH="$PATH" \
  CLUSTERS='ac-test-man-1 ac-test-man-2 ac-test-man-3 ac-test-man-4 ac-test-man-5 ac-test-man-6 ac-test-man-7 ac-test-man-8 ac-test-man-9 ac-test-man-10' \
  WEEKS=26 NUM_NAMESPACES=100 NUM_WORKLOADS=10 NUM_PODS=3 \
  NUM_EXTRA_METRICS=0 NUM_POD_METRICS=20 NUM_CLUSTER_METRICS=154 \
  RS_SCRAPE_INTERVAL=15m PROFILE=custom-continous-1-week-full \
  BUCKET=oia-rs SHARED_PREFIX=thanosbench/dvandra \
  PER_CLUSTER_PREFIX=1 \
  AWS_PROFILE_NAME=default MAX_INFLIGHT=3 \
  WORK=./gen-work-10c-dvandra \
  MANIFEST=./gen-work-10c-dvandra/manifest.final.json \
  BASE_EPOCH_OVERRIDE=1792800000 \
  bash demo-data/generate_and_publish.sh
