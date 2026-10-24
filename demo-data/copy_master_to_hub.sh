#!/usr/bin/env bash
# Copy an immutable demo-data master into a private, mutable hub S3 prefix.
#
# The copy is two-phase: all block data first, then every meta.json. Thanos uses
# meta.json as the block discovery marker, so a hub never observes a partial block.
# The source manifest is intentionally not copied: it remains the master's
# completion contract and is supplied locally to the validation scripts.
#
# Required:
#   BUCKET=oia-rs
#   MASTER_PREFIX=rs-perf-shared/20c-180d-2026-10-24
#   HUB_PREFIX=my-hub-thanos/20c-180d-2026-10-24
# Optional:
#   DEST_BUCKET=$BUCKET AWS_PROFILE_NAME=oia-rs RESUME=0
#
# The selected AWS profile must be able to read the source and write the
# destination. For an interrupted copy, re-run the exact command with RESUME=1.
set -euo pipefail

BUCKET="${BUCKET:-}"
DEST_BUCKET="${DEST_BUCKET:-$BUCKET}"
MASTER_PREFIX="${MASTER_PREFIX:-}"
HUB_PREFIX="${HUB_PREFIX:-}"
AWS_PROFILE_NAME="${AWS_PROFILE_NAME:-oia-rs}"
RESUME="${RESUME:-0}"

for v in BUCKET DEST_BUCKET MASTER_PREFIX HUB_PREFIX; do
  [[ -n "${!v}" ]] || { echo "ABORT: $v is required." >&2; exit 1; }
done
for v in MASTER_PREFIX HUB_PREFIX; do
  value="${!v}"
  [[ "$value" =~ ^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$ ]] || {
    echo "ABORT: $v='$value' must be a plain prefix with no leading/trailing slash or spaces." >&2
    exit 1
  }
done
[[ "$RESUME" == "0" || "$RESUME" == "1" ]] || { echo "ABORT: RESUME must be 0 or 1." >&2; exit 1; }
[[ "$BUCKET/$MASTER_PREFIX" != "$DEST_BUCKET/$HUB_PREFIX" ]] || {
  echo "ABORT: source and destination are identical; a hub must never use the master prefix." >&2
  exit 1
}

AWS=(aws)
[[ -n "$AWS_PROFILE_NAME" ]] && AWS+=(--profile "$AWS_PROFILE_NAME")
aws_() { "${AWS[@]}" "$@"; }

TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

echo "== source completion contract =="
aws_ s3 cp "s3://$BUCKET/$MASTER_PREFIX/manifest.json" "$TMP_DIR/manifest.json" --only-show-errors || {
  echo "ABORT: master manifest is missing; the source is not a completed dataset." >&2
  exit 2
}
read -r EXPECTED_BLOCKS MANIFEST_BUCKET MANIFEST_PREFIX < <(python3 - "$TMP_DIR/manifest.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    m = json.load(f)
print(m.get("expected_blocks", ""), m.get("s3_bucket", ""), m.get("s3_prefix", ""))
PY
)
[[ "$EXPECTED_BLOCKS" =~ ^[1-9][0-9]*$ ]] || { echo "ABORT: manifest expected_blocks is invalid." >&2; exit 2; }
[[ "$MANIFEST_BUCKET" == "$BUCKET" && "$MANIFEST_PREFIX" == "$MASTER_PREFIX" ]] || {
  echo "ABORT: manifest identifies s3://$MANIFEST_BUCKET/$MANIFEST_PREFIX, not the requested source." >&2
  exit 2
}

source_ulids() {
  aws_ s3 ls "s3://$BUCKET/$MASTER_PREFIX/" --recursive \
    | awk '$4 ~ /\/[0-9A-Z]{26}\/meta\.json$/ {n=split($4,a,"/"); print a[n-1]}' \
    | sort -u
}
dest_ulids() {
  aws_ s3 ls "s3://$DEST_BUCKET/$HUB_PREFIX/" --recursive \
    | awk '$4 ~ /\/[0-9A-Z]{26}\/meta\.json$/ {n=split($4,a,"/"); print a[n-1]}' \
    | sort -u
}

source_ulids > "$TMP_DIR/source.ulids"
SOURCE_COUNT="$(wc -l < "$TMP_DIR/source.ulids" | tr -d ' ')"
[[ "$SOURCE_COUNT" -eq "$EXPECTED_BLOCKS" ]] || {
  echo "ABORT: master has $SOURCE_COUNT discoverable blocks; manifest requires $EXPECTED_BLOCKS." >&2
  exit 2
}
echo "certified source: s3://$BUCKET/$MASTER_PREFIX/ ($SOURCE_COUNT blocks)"

MARKER="{\"source_bucket\":\"$BUCKET\",\"source_prefix\":\"$MASTER_PREFIX\",\"expected_blocks\":$EXPECTED_BLOCKS}"
MARKER_KEY="$HUB_PREFIX/.copy-source.json"
if [[ "$RESUME" == "0" ]]; then
  FIRST_KEY="$(aws_ s3api list-objects-v2 --bucket "$DEST_BUCKET" --prefix "$HUB_PREFIX/" --max-keys 1 --query 'Contents[0].Key' --output text)"
  [[ "$FIRST_KEY" == "None" ]] || {
    echo "ABORT: destination is not empty ($FIRST_KEY). Use a new prefix, or RESUME=1 only for this exact interrupted copy." >&2
    exit 3
  }
  printf '%s\n' "$MARKER" | aws_ s3 cp - "s3://$DEST_BUCKET/$MARKER_KEY" --only-show-errors
else
  REMOTE_MARKER="$(aws_ s3 cp "s3://$DEST_BUCKET/$MARKER_KEY" - --only-show-errors 2>/dev/null || true)"
  [[ "$REMOTE_MARKER" == "$MARKER" ]] || {
    echo "ABORT: resume marker is missing or identifies a different master; refusing to mix datasets." >&2
    exit 3
  }
  echo "resume marker matches the requested master"
fi

echo "== phase 1/2: block data (meta.json excluded) =="
aws_ s3 cp "s3://$BUCKET/$MASTER_PREFIX/" "s3://$DEST_BUCKET/$HUB_PREFIX/" \
  --recursive --exclude '*/meta.json' --exclude 'manifest.json' --only-show-errors

echo "== phase 2/2: discovery metadata =="
aws_ s3 cp "s3://$BUCKET/$MASTER_PREFIX/" "s3://$DEST_BUCKET/$HUB_PREFIX/" \
  --recursive --exclude '*' --include '*/meta.json' --only-show-errors

dest_ulids > "$TMP_DIR/dest.ulids"
DEST_COUNT="$(wc -l < "$TMP_DIR/dest.ulids" | tr -d ' ')"
[[ "$DEST_COUNT" -eq "$EXPECTED_BLOCKS" ]] || {
  echo "ABORT: destination has $DEST_COUNT blocks; expected $EXPECTED_BLOCKS. Re-run with RESUME=1." >&2
  exit 4
}
if ! cmp -s "$TMP_DIR/source.ulids" "$TMP_DIR/dest.ulids"; then
  echo "ABORT: destination ULIDs do not exactly match the master; do not point a hub at it." >&2
  exit 4
fi

echo "== copy complete =="
echo "verified $DEST_COUNT exact blocks at s3://$DEST_BUCKET/$HUB_PREFIX/"
echo "The destination is now a private mutable hub copy; the master remains unchanged."
