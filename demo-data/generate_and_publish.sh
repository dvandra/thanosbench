#!/usr/bin/env bash
# Generate right-sizing blocks one cluster-week at a time and publish each to S3
# while the next one generates, so the whole run needs only a few GB of local
# disk and takes about half the wall time of generate-then-upload.
#
#   env CLUSTERS="..." WEEKS=26 NUM_NAMESPACES=100 NUM_WORKLOADS=10 NUM_PODS=3 \
#       NUM_POD_METRICS=20 NUM_CLUSTER_METRICS=154 \
#       BUCKET=oia-rs SHARED_PREFIX=rs-perf-shared/20c-180d-2026-09-28 HUB_PREFIX=jdj64-thanos \
#       bash generate_and_publish.sh            # DRY_RUN=1 prints the plan only
#
# Per cluster-week (9 blocks, ~0.94 GB, ~90 s to generate at the recommended
# cardinality): generate into WORK/tmp/<cluster>-w<week>/, upload every block
# with index/chunks first and meta.json last (so a reader never sees a partial
# block), verify all meta.json objects exist, optionally copy the blocks
# server-side into HUB_PREFIX, write WORK/state/<cluster>.w<week>.done, and
# delete the local copy. Generation pauses while MAX_INFLIGHT cluster-weeks are
# still uploading. manifest.json is written to SHARED_PREFIX last, as the
# completion marker the validators read.
#
# Resumable: re-running skips finished cluster-weeks. BASE_EPOCH is pinned in
# WORK/state/base_epoch on the first run and must not change afterwards (blocks
# of a cluster-week anchored differently would overlap and halt the compactor).
# A cluster-week whose blocks were generated (WORK/state/*.ulids) but not
# confirmed is checked in S3: complete -> marked done; partial -> its uploaded
# blocks are removed and it is regenerated.
set -euo pipefail

# ---- inputs ------------------------------------------------------------------
PROFILE="${PROFILE:-custom-continous-1-week-full}"   # 168h span
BLOCKS_PER_WEEK="${BLOCKS_PER_WEEK:-9}"              # len(duration list) of the -full profile
WEEKS="${WEEKS:-26}"                                 # 26*7 = 182 days
CLUSTERS="${CLUSTERS-ac-test-man-1 ac-test-man-2 ac-test-man-3}"   # unset -> defaults; empty -> abort below
NUM_NAMESPACES="${NUM_NAMESPACES:-100}"
NUM_WORKLOADS="${NUM_WORKLOADS:-10}"
NUM_PODS="${NUM_PODS:-3}"
NUM_EXTRA_METRICS="${NUM_EXTRA_METRICS:-0}"
NUM_POD_METRICS="${NUM_POD_METRICS:-20}"
NUM_CLUSTER_METRICS="${NUM_CLUSTER_METRICS:-154}"
WORKERS="${WORKERS:-8}"
MAX_INFLIGHT="${MAX_INFLIGHT:-3}"                    # cluster-weeks on local disk at once (~1 GB each)
WORK="${WORK:-./gen-work}"
TB="${TB:-$(cd "$(dirname "$0")/.." && pwd)/thanosbench}"
DRY_RUN="${DRY_RUN:-0}"

# generate_cw changes into the per-week directory before invoking thanosbench.
# Keep WORK absolute so --output.dir and every state/log path still refer to the
# same run directory from inside that subshell.
case "$WORK" in
  /*) ;;
  *) WORK="$(pwd)/${WORK#./}" ;;
esac

BUCKET="${BUCKET:-}"
SHARED_PREFIX="${SHARED_PREFIX:-}"                   # e.g. rs-perf-shared/20c-180d-2026-09-28
HUB_PREFIX="${HUB_PREFIX:-}"                         # optional: the hub's own Thanos prefix (server-side copy)
AWS_PROFILE_NAME="${AWS_PROFILE_NAME:-oia-rs}"       # empty -> ambient AWS credentials

BYTES_PER_SAMPLE="${BYTES_PER_SAMPLE:-9.9}"
FILLER_BYTES_PER_SAMPLE="${FILLER_BYTES_PER_SAMPLE:-4.6}"
GEN_SECONDS_PER_CLUSTER_WEEK="${GEN_SECONDS_PER_CLUSTER_WEEK:-90}"   # measured on a 10-core Mac
DS_5M_RATIO="${DS_5M_RATIO:-3.8}"; DS_1H_RATIO="${DS_1H_RATIO:-1.1}"

# ---- validation --------------------------------------------------------------
for v in WEEKS NUM_NAMESPACES NUM_WORKLOADS NUM_PODS NUM_EXTRA_METRICS NUM_POD_METRICS NUM_CLUSTER_METRICS MAX_INFLIGHT WORKERS; do
  [[ "${!v}" =~ ^(0|[1-9][0-9]{0,8})$ ]] || { echo "ABORT: ${v}='${!v}' must be a non-negative integer." >&2; exit 1; }
done
[[ "$WEEKS" -ge 1 && "$MAX_INFLIGHT" -ge 1 ]] || { echo "ABORT: WEEKS and MAX_INFLIGHT must be >= 1." >&2; exit 1; }
for p in BUCKET SHARED_PREFIX; do
  [[ -n "${!p}" ]] || { echo "ABORT: $p is required." >&2; exit 1; }
done
for p in SHARED_PREFIX HUB_PREFIX; do
  v="${!p}"; [[ -z "$v" ]] && continue
  [[ "$v" =~ ^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$ ]] || { echo "ABORT: $p='$v' must be a plain prefix like a/b (no leading or trailing slash, no spaces)." >&2; exit 1; }
done
[[ -z "$HUB_PREFIX" || "$HUB_PREFIX" != "$SHARED_PREFIX" ]] || { echo "ABORT: HUB_PREFIX must differ from SHARED_PREFIX." >&2; exit 1; }
[[ -n "${CLUSTERS// /}" ]] || { echo "ABORT: CLUSTERS is empty." >&2; exit 1; }
# shellcheck disable=SC2086  # split the space-separated cluster list on purpose
[[ "$(printf '%s\n' $CLUSTERS | sort | uniq -d | wc -l | tr -d ' ')" -eq 0 ]] || { echo "ABORT: CLUSTERS has duplicate names." >&2; exit 1; }
[[ -x "$TB" ]] || { echo "ABORT: thanosbench binary not found at $TB (build it; on recent macOS: GOTOOLCHAIN=go1.22.12 go build -ldflags=-linkmode=external -o thanosbench ./cmd/thanosbench)." >&2; exit 1; }

# a full run makes ~30k aws calls: let the CLI retry transient S3 errors harder than its default 3 attempts
export AWS_RETRY_MODE="${AWS_RETRY_MODE:-adaptive}" AWS_MAX_ATTEMPTS="${AWS_MAX_ATTEMPTS:-10}"
AWS=(aws); [[ -n "$AWS_PROFILE_NAME" ]] && AWS+=(--profile "$AWS_PROFILE_NAME")
aws_() { "${AWS[@]}" "$@"; }
s3cp() { aws_ s3 cp --only-show-errors "$@"; }   # --only-show-errors exists for `aws s3` only
iso() { date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ; }
log() { echo "$(date -u +%H:%M:%S) $*"; }

# ---- plan --------------------------------------------------------------------
# shellcheck disable=SC2086  # split the space-separated cluster list on purpose
NCL=$(printf '%s\n' $CLUSTERS | grep -c .)
RS_SPB=$(python3 -c "N=$NUM_NAMESPACES;W=$NUM_WORKLOADS;P=$NUM_PODS;print(3*(6*(1+N+N*W+N*W*P)+2*N))")
FILLER_SPB=$(python3 -c "N=$NUM_NAMESPACES;W=$NUM_WORKLOADS;P=$NUM_PODS;print($NUM_EXTRA_METRICS*N+$NUM_POD_METRICS*N*W*P+$NUM_CLUSTER_METRICS)")
SPB=$(python3 -c "print($RS_SPB+$FILLER_SPB)")
TOTAL_CW=$(( NCL * WEEKS ))
BLOCKS=$(( TOTAL_CW * BLOCKS_PER_WEEK ))
CW_BYTES=$(python3 -c "print(int(($RS_SPB*96*$BYTES_PER_SAMPLE+$FILLER_SPB*288*$FILLER_BYTES_PER_SAMPLE)*7))")
PROJ_GB=$(python3 -c "print('%.1f'%($CW_BYTES*$TOTAL_CW/1e9))")
HUB_GB=$(python3 -c "print('%.1f'%($CW_BYTES*$TOTAL_CW*(1+$DS_5M_RATIO+$DS_1H_RATIO)/1e9))")
LOCAL_GB=$(python3 -c "print('%.1f'%($CW_BYTES*($MAX_INFLIGHT+1)/1e9))")
GEN_H=$(python3 -c "print('%.1f'%($TOTAL_CW*$GEN_SECONDS_PER_CLUSTER_WEEK/3600))")

PARAMS="profile=$PROFILE blocks_per_week=$BLOCKS_PER_WEEK clusters=[$CLUSTERS] N=$NUM_NAMESPACES W=$NUM_WORKLOADS P=$NUM_PODS extra=$NUM_EXTRA_METRICS pod=$NUM_POD_METRICS cluster=$NUM_CLUSTER_METRICS rs_interval=${RS_SCRAPE_INTERVAL:-15m} bucket=$BUCKET shared=$SHARED_PREFIX hub=$HUB_PREFIX"
if [[ -f "$WORK/state/params" && "$(cat "$WORK/state/params")" != "$PARAMS" ]]; then
  echo "ABORT: $WORK was started with different parameters:" >&2; echo "  was: $(cat "$WORK/state/params")" >&2; echo "  now: $PARAMS" >&2
  echo "  Only WEEKS may change between runs; use a new WORK dir (and SHARED_PREFIX) for a different dataset." >&2; exit 1
fi
if [[ -f "$WORK/state/base_epoch" ]]; then
  BASE_EPOCH="$(cat "$WORK/state/base_epoch")"
  [[ -z "${BASE_EPOCH_OVERRIDE:-}" || "$BASE_EPOCH_OVERRIDE" == "$BASE_EPOCH" ]] || { echo "ABORT: $WORK is pinned to BASE_EPOCH=$BASE_EPOCH; use a new WORK dir for a different anchor." >&2; exit 1; }
else
  BASE_EPOCH="${BASE_EPOCH_OVERRIDE:-$(( ( $(date -u +%s) / 86400 ) * 86400 ))}"
fi

echo "== plan =="
echo "  profile:      $PROFILE ($BLOCKS_PER_WEEK blocks/week)"
echo "  clusters:     $NCL; weeks: $WEEKS (~$(( WEEKS*7 )) days); cluster-weeks: $TOTAL_CW; blocks: $BLOCKS"
echo "  cardinality:  N=$NUM_NAMESPACES W=$NUM_WORKLOADS P=$NUM_PODS -> $RS_SPB right-sizing + $FILLER_SPB filler = $SPB series/block"
echo "  window:       $(iso $(( BASE_EPOCH - WEEKS*7*86400 ))) .. $(iso "$BASE_EPOCH") (pinned)"
echo "  destination:  s3://$BUCKET/$SHARED_PREFIX/${HUB_PREFIX:+  + server-side copy -> s3://$BUCKET/$HUB_PREFIX/}"
echo "  projected:    ~$PROJ_GB GB to upload; ~$HUB_GB GB on the hub after downsampling; peak local disk ~$LOCAL_GB GB (MAX_INFLIGHT=$MAX_INFLIGHT)"
echo "  time:         ~$GEN_H h of generation at ${GEN_SECONDS_PER_CLUSTER_WEEK}s per cluster-week; uploads overlap with generation"
python3 -c "import sys; sys.exit(0 if $SPB > 5000000 else 1)" && { echo "ABORT: $SPB series/block > maxSeriesPerBlock=5,000,000." >&2; exit 1; }
[[ "$DRY_RUN" == "1" ]] && { echo "== DRY_RUN: nothing generated or uploaded. =="; exit 0; }
mkdir -p "$WORK/state" "$WORK/tmp" "$WORK/log"

# ---- S3 access ---------------------------------------------------------------
aws_ sts get-caller-identity >/dev/null || { echo "ABORT: AWS credentials not usable (profile '$AWS_PROFILE_NAME')." >&2; exit 1; }
aws_ s3api head-bucket --bucket "$BUCKET" >/dev/null || { echo "ABORT: cannot access bucket $BUCKET." >&2; exit 1; }
if [[ ! -f "$WORK/state/base_epoch" ]]; then
  # a fresh run must not land on top of an existing dataset
  first_key="$(aws_ s3api list-objects-v2 --bucket "$BUCKET" --prefix "$SHARED_PREFIX/" --max-keys 1 --query 'Contents[0].Key' --output text)" \
    || { echo "ABORT: cannot list s3://$BUCKET/$SHARED_PREFIX/ (permissions?)." >&2; exit 1; }
  [[ "$first_key" == "None" ]] || { echo "ABORT: s3://$BUCKET/$SHARED_PREFIX/ is not empty ($first_key) and $WORK has no state for it. Use a new SHARED_PREFIX or the WORK dir of the run that created it." >&2; exit 1; }
  echo "$BASE_EPOCH" > "$WORK/state/base_epoch"
  echo "$PARAMS" > "$WORK/state/params"
fi

# manifest (same schema as generate_180day.sh, plus the S3 locations); uploaded last
python3 - "$WORK/manifest.json" <<PY
import json,sys
json.dump({
  "base_epoch": $BASE_EPOCH, "weeks": $WEEKS, "blocks_per_week": $BLOCKS_PER_WEEK,
  "clusters": "$CLUSTERS".split(),
  "num_namespaces": $NUM_NAMESPACES, "num_workloads": $NUM_WORKLOADS, "num_pods": $NUM_PODS,
  "num_extra_metrics": $NUM_EXTRA_METRICS, "num_pod_metrics": $NUM_POD_METRICS, "num_cluster_metrics": $NUM_CLUSTER_METRICS,
  "rs_scrape_interval": "${RS_SCRAPE_INTERVAL:-15m}",
  "series_per_block": $SPB, "rs_series_per_block": $RS_SPB, "filler_series_per_block": $FILLER_SPB,
  "blocks_per_cluster": $(( WEEKS * BLOCKS_PER_WEEK )), "expected_blocks": $BLOCKS,
  "profile": "$PROFILE",
  "min_data_epoch": $BASE_EPOCH - $WEEKS*7*86400, "max_data_epoch": $BASE_EPOCH,
  "s3_bucket": "$BUCKET", "s3_prefix": "$SHARED_PREFIX", "hub_prefix": "$HUB_PREFIX",
  "expected_server_substr": "${EXPECTED_SERVER_SUBSTR:-}",
}, open(sys.argv[1],"w"), indent=2)
PY

# ---- helpers -----------------------------------------------------------------
ulids_in() { find "$1" -mindepth 1 -maxdepth 1 -type d -name '[0-9A-Z]*' -exec basename {} \; | sort; }
complete_blocks_in() { local n=0; for u in $(ulids_in "$1"); do [[ -f "$1/$u/meta.json" && -f "$1/$u/index" && -d "$1/$u/chunks" ]] && n=$((n+1)); done; echo "$n"; }
s3_has_meta() { aws_ s3api head-object --bucket "$BUCKET" --key "$1/$2/meta.json" >/dev/null 2>&1; }
s3_rm_block() { aws_ s3 rm --only-show-errors --recursive "s3://$BUCKET/$1/$2/" || { echo "ABORT: could not remove s3://$BUCKET/$1/$2/ (a leftover block would duplicate the regenerated one)." >&2; exit 1; }; }

generate_cw() { # $1=cluster $2=week $3=dir
  local cluster="$1" w="$2" dir="$3" maxt gauges
  maxt="$(iso $(( BASE_EPOCH - w*7*86400 )))"
  gauges="$(awk -v s="${RANDOM}${w}" 'BEGIN{srand(s);printf "%.3f %.3f",2.1+rand()*2.5,10.6+rand()*9.2}')"
  rm -rf "$dir"; mkdir -p "$dir"
  ( cd "$dir" && NUM_NAMESPACES=$NUM_NAMESPACES NUM_WORKLOADS=$NUM_WORKLOADS NUM_PODS=$NUM_PODS \
      NUM_EXTRA_METRICS=$NUM_EXTRA_METRICS NUM_POD_METRICS=$NUM_POD_METRICS NUM_CLUSTER_METRICS=$NUM_CLUSTER_METRICS \
      MIN_GAUGE="${gauges% *}" MAX_GAUGE="${gauges#* }" \
      "$TB" block plan -p "$PROFILE" --labels "cluster=\"$cluster\"" --labels "aggregation=\"1d\"" --max-time "$maxt" \
      | "$TB" block gen --output.dir "$dir" --workers "$WORKERS" ) > "$WORK/log/$cluster-w$w.gen.log" 2>&1
  rm -rf "$dir/chunks_head"
  [[ "$(complete_blocks_in "$dir")" -eq "$BLOCKS_PER_WEEK" ]] || { echo "ABORT: $cluster week $w produced $(complete_blocks_in "$dir") complete blocks, expected $BLOCKS_PER_WEEK (see $WORK/log/$cluster-w$w.gen.log)." >&2; return 1; }
  ulids_in "$dir" > "$WORK/state/$cluster.w$w.ulids"
}

# NOTE: bash ignores `set -e` inside the left side of `||`/`&&`, inside `if` and
# inside functions called from there, so every step below checks its own status.
upload_block() { # $1=dir $2=ulid $3=prefix : data first, meta.json last
  s3cp --recursive "$1/$2/" "s3://$BUCKET/$3/$2/" --exclude meta.json || { echo "upload of $2 data failed"; return 1; }
  s3cp "$1/$2/meta.json" "s3://$BUCKET/$3/$2/meta.json" || { echo "upload of $2 meta.json failed"; return 1; }
  s3_has_meta "$3" "$2" || { echo "upload of $2 not visible in S3"; return 1; }
}
copy_block() { # $1=ulid : shared -> hub, server-side, meta.json last
  s3cp --recursive "s3://$BUCKET/$SHARED_PREFIX/$1/" "s3://$BUCKET/$HUB_PREFIX/$1/" --exclude meta.json || { echo "copy of $1 data failed"; return 1; }
  s3cp "s3://$BUCKET/$SHARED_PREFIX/$1/meta.json" "s3://$BUCKET/$HUB_PREFIX/$1/meta.json" || { echo "copy of $1 meta.json failed"; return 1; }
  s3_has_meta "$HUB_PREFIX" "$1" || { echo "copy of $1 not visible in S3"; return 1; }
}
copy_cw_to_hub() { # $1=ulids file : copy the blocks not yet in HUB_PREFIX (same ULIDs, idempotent)
  local u
  while read -r u; do
    s3_has_meta "$HUB_PREFIX" "$u" || copy_block "$u" || return 1
  done < "$1"
}
publish_body() { # $1=cluster $2=week $3=dir
  local u
  # 1. the whole cluster-week into the shared prefix first ...
  while read -r u; do upload_block "$3" "$u" "$SHARED_PREFIX" || return 1; done < "$WORK/state/$1.w$2.ulids"
  # 2. ... then into the hub prefix. Nothing reaches the hub (and its compactor)
  #    before the shared copy is complete, so a resume never has to regenerate a
  #    cluster-week the hub has already seen (see the resume step).
  if [[ -n "$HUB_PREFIX" ]]; then copy_cw_to_hub "$WORK/state/$1.w$2.ulids" || return 1; fi
  mv "$WORK/state/$1.w$2.ulids" "$WORK/state/$1.w$2.done" || return 1
  rm -rf "$3"
  echo "done"
}
publish_cw() { # $1=cluster $2=week $3=dir ; runs in the background
  trap 'pkill -TERM -P $BASHPID 2>/dev/null; exit 143' TERM INT   # stop our aws children when the run is interrupted
  if publish_body "$1" "$2" "$3" > "$WORK/log/$1-w$2.upload.log" 2>&1; then :; else
    touch "$WORK/state/$1.w$2.failed"; echo "FAILED (see $WORK/log/$1-w$2.upload.log)" >> "$WORK/log/$1-w$2.upload.log"
    log "upload of $1 week $2 FAILED ($(tail -1 "$WORK/log/$1-w$2.upload.log" | cut -c1-120)); the run continues and will report INCOMPLETE" >&2
  fi
}

# cluster-weeks still uploading: tmp dirs whose upload has not failed (a failed one
# keeps its dir for the resume step but must not occupy a slot, or MAX_INFLIGHT
# failures would stall the run)
inflight() {
  local n=0 d b
  for d in "$WORK"/tmp/*/; do
    [[ -d "$d" ]] || continue; b="$(basename "$d")"
    [[ -f "$WORK/state/${b%-w*}.w${b##*-w}.failed" ]] || n=$((n+1))
  done
  echo "$n"
}
count_state() { find "$WORK/state" -maxdepth 1 -name "*.$1" | wc -l | tr -d ' '; }   # $1 = done | failed | ulids
wait_for_slot() { while [[ "$(inflight)" -ge "$MAX_INFLIGHT" ]]; do sleep 5; done; }

# resume: a tmp dir without a .ulids file is an interrupted generation; nothing of it is in S3
for d in "$WORK"/tmp/*/; do
  [[ -d "$d" ]] || continue
  b="$(basename "$d")"; [[ -f "$WORK/state/${b%-w*}.w${b##*-w}.ulids" ]] || { log "resume: removing half-generated $b"; rm -rf "$d"; }
done
# resume: settle cluster-weeks that were generated but not confirmed
for f in "$WORK"/state/*.ulids; do
  [[ -e "$f" ]] || continue
  b="$(basename "$f" .ulids)"; cluster="${b%.w*}"; w="${b##*.w}"; dir="$WORK/tmp/$cluster-w$w"
  present=0
  while read -r u; do s3_has_meta "$SHARED_PREFIX" "$u" && present=$((present+1)); done < "$f"
  if [[ "$present" -eq "$BLOCKS_PER_WEEK" ]]; then
    # shared copy complete: only the hub copy may be missing or partial. Finish it
    # with the SAME ULIDs (server-side); never regenerate what the hub may have seen.
    log "resume: $cluster week $w complete in the shared prefix, finishing the hub copy"
    if [[ -n "$HUB_PREFIX" ]]; then copy_cw_to_hub "$f" || { echo "ABORT: could not finish the hub copy of $cluster week $w" >&2; exit 1; }; fi
    mv "$f" "$WORK/state/$b.done"; rm -rf "$dir"
  elif [[ -d "$dir" && "$(complete_blocks_in "$dir")" -eq "$BLOCKS_PER_WEEK" ]]; then
    log "resume: $cluster week $w generated locally, re-uploading its $present partial block(s) and the rest"
    while read -r u; do s3_rm_block "$SHARED_PREFIX" "$u"; done < "$f"
    publish_cw "$cluster" "$w" "$dir" &
  else
    # shared copy incomplete and no local copy: nothing of it reached the hub
    # prefix (the hub copy starts only after the shared copy is complete)
    log "resume: $cluster week $w incomplete, removing its $present uploaded block(s) and regenerating"
    while read -r u; do s3_rm_block "$SHARED_PREFIX" "$u"; done < "$f"
    rm -f "$f"; rm -rf "$dir"
  fi
done
find "$WORK/state" -maxdepth 1 -name '*.failed' -delete

# ---- main loop ---------------------------------------------------------------
trap 'trap - INT TERM; echo; log "interrupted: stopping uploads (re-run the same command to resume)"; pkill -TERM -P $$ 2>/dev/null; wait; exit 130' INT TERM
START=$(date +%s); i=0
for cluster in $CLUSTERS; do
  for (( w = 0; w < WEEKS; w++ )); do
    i=$((i+1))
    [[ -f "$WORK/state/$cluster.w$w.done" ]] && continue
    [[ -f "$WORK/state/$cluster.w$w.ulids" ]] && continue   # being re-uploaded by the resume step
    wait_for_slot
    dir="$WORK/tmp/$cluster-w$w"; t0=$(date +%s)
    if ! generate_cw "$cluster" "$w" "$dir"; then
      echo "== generation of $cluster week $w failed; waiting for in-flight uploads, then stopping. Re-run the same command to resume. ==" >&2
      wait; exit 1
    fi
    publish_cw "$cluster" "$w" "$dir" &
    log "[$i/$TOTAL_CW] $cluster week $w generated in $(( $(date +%s)-t0 ))s; uploading (in flight: $(inflight)); done so far: $(count_state "done")"
  done
done
wait

# ---- finish ------------------------------------------------------------------
FAILED=$(count_state "failed")
DONE=$(count_state "done")
if [[ "$FAILED" -gt 0 || "$DONE" -ne "$TOTAL_CW" ]]; then
  echo "== INCOMPLETE: $DONE/$TOTAL_CW cluster-weeks done, $FAILED failed. Re-run the same command to resume. ==" >&2; exit 1
fi
REMOTE=$(aws_ s3 ls "s3://$BUCKET/$SHARED_PREFIX/" --recursive | awk '{print $4}' | grep -cE '/[0-9A-Z]{26}/meta\.json$' || true)
[[ "$REMOTE" -eq "$BLOCKS" ]] || { echo "ABORT: S3 has $REMOTE complete blocks under $SHARED_PREFIX, expected $BLOCKS." >&2; exit 1; }
# the exact block list (from the .done files) goes into the manifest for consumers
python3 - "$WORK/manifest.json" "$WORK/state" <<'PY'
import json, sys, glob, os
m = json.load(open(sys.argv[1])); ulids = []
for f in sorted(glob.glob(os.path.join(sys.argv[2], "*.done"))):
    ulids += [l.strip() for l in open(f) if l.strip()]
m["block_ulids"] = ulids; m["generated_at"] = int(__import__("time").time())
json.dump(m, open(sys.argv[1], "w"), indent=2)
PY
s3cp "$WORK/manifest.json" "s3://$BUCKET/$SHARED_PREFIX/manifest.json"
cp "$WORK/manifest.json" "${MANIFEST:-./gen-180day.manifest.json}"
echo "== done in $(( ($(date +%s)-START)/60 )) min: $BLOCKS blocks in s3://$BUCKET/$SHARED_PREFIX/ (manifest.json written last)${HUB_PREFIX:+; copied to s3://$BUCKET/$HUB_PREFIX/} =="
echo "   manifest also saved to ${MANIFEST:-./gen-180day.manifest.json} for the validators"
