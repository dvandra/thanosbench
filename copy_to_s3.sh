#!/usr/bin/env bash
set -euo pipefail

# Upload generated cluster blocks to S3.
#
# Usage:
#   ./copy_to_s3.sh 1-3       # upload clusters 1, 2, and 3
#   ./copy_to_s3.sh 2         # upload cluster 2
#
# Configuration:
#   LOCAL_ROOT=./new-run
#   S3_BUCKET=s3://my-bucket
#   S3_PREFIX=thanosbench

LOCAL_ROOT="${LOCAL_ROOT:-./new-run}"
S3_BUCKET="${S3_BUCKET:?Set S3_BUCKET, for example: export S3_BUCKET=s3://my-bucket}"
S3_PREFIX="${S3_PREFIX:-thanosbench}"
CLUSTER_RANGE="${1:-}"

if [[ -z "$CLUSTER_RANGE" ]]; then
  echo "Usage: $0 <cluster|start-end>"
  echo "Example: $0 1-3"
  exit 2
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "Error: AWS CLI is not installed or not on PATH." >&2
  exit 1
fi

if [[ "$CLUSTER_RANGE" =~ ^([0-9]+)-([0-9]+)$ ]]; then
  start="${BASH_REMATCH[1]}"
  end="${BASH_REMATCH[2]}"
elif [[ "$CLUSTER_RANGE" =~ ^[0-9]+$ ]]; then
  start="$CLUSTER_RANGE"
  end="$CLUSTER_RANGE"
else
  echo "Error: cluster must be a number or range such as 1-3." >&2
  exit 2
fi

if (( start > end )); then
  echo "Error: start cluster must not be greater than end cluster." >&2
  exit 2
fi

for ((cluster = start; cluster <= end; cluster++)); do
  source_dir="$LOCAL_ROOT/cluster-$cluster"
  destination="$S3_BUCKET/${S3_PREFIX%/}/cluster-$cluster/"

  if [[ ! -d "$source_dir" ]]; then
    echo "Error: directory not found: $source_dir" >&2
    exit 1
  fi

  echo "Uploading $source_dir -> $destination"
  aws s3 sync "$source_dir/" "$destination" --dryrun
done

read -r -p "Proceed with the upload? [y/N] " answer
if [[ ! "$answer" =~ ^[Yy]$ ]]; then
  echo "Upload cancelled."
  exit 0
fi

for ((cluster = start; cluster <= end; cluster++)); do
  source_dir="$LOCAL_ROOT/cluster-$cluster"
  destination="$S3_BUCKET/${S3_PREFIX%/}/cluster-$cluster/"
  aws s3 sync "$source_dir/" "$destination"
done

echo "Upload completed for clusters $start-$end."
