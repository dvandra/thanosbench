#!/usr/bin/env bash
# Demo-data ops script (see demo-data/README.md). Run it yourself:
#   ! bash validate_compaction_180day.sh                 # report + fail on real problems
#   ! REQUIRE_SETTLED=1 bash validate_compaction_180day.sh  # also require compaction/5m/1h done
#
# Read-only. Proves the Thanos compactor's work by inspecting every block's
# meta.json in the hub's object store (ground truth). The store is whatever the
# thanos-object-storage Secret (thanos.yaml) points at: bucket, optional prefix,
# endpoint and creds are all read from it. An AWS S3 endpoint (*.amazonaws.com) is
# reached directly over https with the region taken from the endpoint (no
# port-forward); anything else is treated as in-cluster MinIO behind a port-forward
# to the endpoint's service. Certification-grade:
#   * RECONCILES listed-vs-fetched metas; refuses to draw conclusions if incomplete
#   * excludes deletion-marked blocks (they linger ~48h and would skew counts/overlaps)
#   * overlap check is per (cluster,resolution,LEVEL) with a running-max sweep
#     (catches contained & non-adjacent overlaps; does NOT false-positive on the
#      normal level-1-source + level-2-parent coexistence during compaction)
#   * GAP detection over each tier's union: raw gaps fail; downsample-tier gaps are
#     reported as normal finer-resolution fallback (Thanos does not downsample every
#     short edge block)
#   * settled checks use Thanos's age-eligible horizons (raw->5m after 40h,
#     5m->1h after 10d), not an impossible full-window requirement for fresh data
#   * PER-CLUSTER verdict, AND-reduced; real pass/fail EXIT CODE
# Exit: 0 = healthy; 1 = problem (overlaps / raw gaps / incomplete / --settled shortfall);
#       2 = environment error (wrong cluster / port-forward / endpoint / creds).
set -uo pipefail

NS="open-cluster-management-observability"
SECRET="thanos-object-storage"; KEY="thanos.yaml"   # bucket/prefix/endpoint/creds all come from here
MANIFEST="${MANIFEST:-./gen-180day.manifest.json}"
DEMO_CLUSTERS="${DEMO_CLUSTERS:-ac-test-man-1 ac-test-man-2 ac-test-man-3}"
EXPECTED_SERVER_SUBSTR="${EXPECTED_SERVER_SUBSTR:-}"
PARALLEL="${PARALLEL:-8}"
GAP_THRESH_H="${GAP_THRESH_H:-6}"
REQUIRE_SETTLED="${REQUIRE_SETTLED:-0}"
RAW_TO_5M_DELAY_H="${RAW_TO_5M_DELAY_H:-40}"
FIVEM_TO_1H_DELAY_H="${FIVEM_TO_1H_DELAY_H:-240}"
LOCAL_PORT="${LOCAL_PORT:-$(( 20000 + RANDOM % 20000 ))}"
MIN_DATA_EPOCH="${MIN_DATA_EPOCH:-0}"; MAX_DATA_EPOCH="${MAX_DATA_EPOCH:-0}"

trim() { sed -E 's/^[[:space:]]*//; s/[[:space:]]*$//; s/^"//; s/"$//; s/^'\''//; s/'\''$//'; }

if [[ -f "${MANIFEST}" ]]; then
  _mvars="$(mktemp)"
  MANIFEST_PATH="${MANIFEST}" python3 - >"${_mvars}" <<'PY'
import json,sys,os
m=json.load(open(os.environ["MANIFEST_PATH"]))
print('DEMO_CLUSTERS="' + " ".join(m.get("clusters",["ac-test-man-1","ac-test-man-2","ac-test-man-3"])) + '"')
print("MIN_DATA_EPOCH=" + str(m.get("min_data_epoch",0)))
print("MAX_DATA_EPOCH=" + str(m.get("max_data_epoch",0)))
if m.get("expected_server_substr"):
    print('EXPECTED_SERVER_SUBSTR="' + m["expected_server_substr"] + '"')
PY
  source "${_mvars}"; rm -f "${_mvars}"
fi

[[ -n "${EXPECTED_SERVER_SUBSTR}" ]] || { echo "ABORT: EXPECTED_SERVER_SUBSTR empty." >&2; exit 2; }
SERVER="$(oc whoami --show-server)"
[[ "${SERVER}" == *"${EXPECTED_SERVER_SUBSTR}"* ]] || { echo "ABORT: wrong cluster." >&2; exit 2; }

TMPD="$(mktemp -d)"
PF_PID=""   # set only in minio mode (port-forward); the trap runs at exit, so it sees the final value
trap 'if [[ -n "${PF_PID}" ]]; then kill "${PF_PID}" >/dev/null 2>&1; fi; rm -rf "${TMPD}"' EXIT
# throwaway AWS config: nothing from ~/.aws or the shell's own AWS session may leak in
export AWS_CONFIG_FILE="${TMPD}/awsconfig" AWS_SHARED_CREDENTIALS_FILE="${TMPD}/awscreds"
unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_SESSION_TOKEN

# --- object store: parse the Thanos objstore config (thanos.yaml) from the MCO secret.
# Creds go into env only and are never printed.
CFG="$(oc get secret "${SECRET}" -n "${NS}" -o jsonpath="{.data.${KEY//./\\.}}" | base64 -d)"
cfg_val() { printf '%s\n' "${CFG}" | grep -E "^[[:space:]]*$1:" | head -1 | sed -E "s/^[[:space:]]*$1:[[:space:]]*//" | trim; }
AWS_ACCESS_KEY_ID="$(cfg_val access_key)"; AWS_SECRET_ACCESS_KEY="$(cfg_val secret_key)"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_EC2_METADATA_DISABLED="true"
[[ -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]] || { echo "ABORT: no creds." >&2; exit 2; }
BUCKET="$(cfg_val bucket)"; ENDPOINT="$(cfg_val endpoint)"; PREFIX="$(cfg_val prefix)"; CFG_REGION="$(cfg_val region)"
[[ -n "${BUCKET}" && -n "${ENDPOINT}" ]] || { echo "ABORT: no bucket/endpoint in secret ${SECRET}." >&2; exit 2; }
ENDPOINT="${ENDPOINT#*://}"; ENDPOINT="${ENDPOINT%%/*}"                # host[:port]
HOST="$(printf '%s' "${ENDPOINT%%:*}" | tr '[:upper:]' '[:lower:]')"
PORT="${ENDPOINT##*:}"; [[ "${PORT}" == "${ENDPOINT}" ]] && PORT=""
PREFIX="${PREFIX#/}"; PREFIX="${PREFIX%/}"
OBJ_BASE="s3://${BUCKET}${PREFIX:+/${PREFIX}}"                      # blocks live at ${OBJ_BASE}/<ULID>/

# mode: an AWS S3 endpoint is reached directly (https, region from the endpoint, no
# port-forward); anything else is the in-cluster MinIO behind a port-forward to the
# service named by the endpoint's first DNS label. The endpoint is passed explicitly
# to every aws call (--endpoint-url) so no AWS_ENDPOINT_URL from the shell can interfere.
_aws_re='(^|\.)amazonaws\.com$'
_region_re='(^|[.-])([a-z]{2}(-gov)?-[a-z]+-[0-9]+)\.amazonaws\.com$'   # s3.<region>. / s3-<region>. / s3.dualstack.<region>.
if [[ "${HOST}" =~ ${_aws_re} ]]; then
  MODE="s3"
  if [[ "${HOST}" =~ ${_region_re} ]]; then REGION="${BASH_REMATCH[2]}"; else REGION="${CFG_REGION:-us-east-1}"; fi
  ENDPOINT_URL="https://${ENDPOINT}"
  echo "== object store: mode=s3 ${OBJ_BASE} endpoint=${ENDPOINT} region=${REGION} (no port-forward) =="
else
  MODE="minio"; SVC="${HOST%%.*}"; SVC="${SVC:-minio}"; REGION="${CFG_REGION:-us-east-1}"
  ENDPOINT_URL="http://localhost:${LOCAL_PORT}"
  echo "== object store: mode=minio ${OBJ_BASE} endpoint=${ENDPOINT} (port-forward svc/${SVC} ${LOCAL_PORT}:${PORT:-9000}) =="
  oc port-forward "svc/${SVC}" "${LOCAL_PORT}:${PORT:-9000}" -n "${NS}" >"${TMPD}/pf.log" 2>&1 &
  PF_PID=$!
  READY=0; for i in $(seq 1 30); do curl -s -o /dev/null "http://localhost:${LOCAL_PORT}/minio/health/live" && { READY=1; break; }; sleep 1; done
  [[ "${READY}" == "1" ]] || { echo "ABORT: MinIO port-forward not ready (env error, NOT a data verdict)." >&2; exit 2; }
fi
export AWS_REGION="${REGION}" AWS_DEFAULT_REGION="${REGION}"

echo "== listing blocks under ${OBJ_BASE}/ =="
# top-level "directories" under the prefix; keep only ULID-shaped names (the compactor
# also writes debug/metas/, and a shared S3 prefix may hold other objects)
aws s3 ls "${OBJ_BASE}/" --endpoint-url "${ENDPOINT_URL}" | awk '/PRE /{print $2}' | sed 's#/$##' \
  | grep -E '^[0-9A-HJKMNP-TV-Z]{26}$' > "${TMPD}/all.txt" || true
LISTED="$(grep -c . "${TMPD}/all.txt" || true)"
echo "  top-level blocks: ${LISTED}"
[[ "${LISTED}" -gt 0 ]] || { echo "ABORT: no blocks listed under ${OBJ_BASE}/ (${MODE}: endpoint/bucket/creds issue, not a data verdict)." >&2; exit 2; }

# deletion marks (one recursive listing) so we can exclude soft-deleted blocks; keys come
# back as <prefix>/<ULID>/deletion-mark.json, so strip the prefix before taking the ULID
aws s3 ls "${OBJ_BASE}/" --recursive --endpoint-url "${ENDPOINT_URL}" 2>/dev/null \
  | awk -v pfx="${PREFIX:+${PREFIX}/}" '/deletion-mark\.json$/{k=$4; if (pfx!="" && index(k,pfx)==1) k=substr(k,length(pfx)+1); split(k,a,"/"); print a[1]}' \
  | sort -u > "${TMPD}/deleted.txt" || true
echo "  deletion-marked blocks: $(grep -c . "${TMPD}/deleted.txt" || true)"

METAD="${TMPD}/meta"; mkdir -p "${METAD}"
echo "== fetching meta.json (parallel x${PARALLEL}, reconciled) =="
FETCHED=0
for attempt in 1 2 3; do
  # base/dir/endpoint go in as positional args ($1/$2/$4) of the child shell, only the ULID is
  # {}-substituted: BSD xargs caps a substituted argument at 255 bytes
  # shellcheck disable=SC2016
  while IFS= read -r u; do [[ -n "$u" && ! -f "${METAD}/${u}.json" ]] && echo "$u"; done < "${TMPD}/all.txt" \
    | xargs -P "${PARALLEL}" -I{} bash -c \
      'aws s3 cp "$1/$3/meta.json" "$2/$3.json" --only-show-errors --endpoint-url "$4" 2>/dev/null || true' \
      _ "${OBJ_BASE}" "${METAD}" {} "${ENDPOINT_URL}"
  FETCHED="$(ls -1 "${METAD}" 2>/dev/null | grep -c '\.json$' || true)"
  echo "  attempt ${attempt}: ${FETCHED}/${LISTED}"
  [[ "${FETCHED}" -ge "${LISTED}" ]] && break
done
if [[ "${FETCHED}" -lt "${LISTED}" ]]; then
  echo "== INCOMPLETE: fetched ${FETCHED}/${LISTED} metas — cannot certify (retry) ==" >&2
  exit 1
fi

echo
echo "== compaction / downsampling report =="
DEMO_CLUSTERS="${DEMO_CLUSTERS}" GAP_THRESH_H="${GAP_THRESH_H}" REQUIRE_SETTLED="${REQUIRE_SETTLED}" \
RAW_TO_5M_DELAY_H="${RAW_TO_5M_DELAY_H}" FIVEM_TO_1H_DELAY_H="${FIVEM_TO_1H_DELAY_H}" \
MIN_DATA_EPOCH="${MIN_DATA_EPOCH}" MAX_DATA_EPOCH="${MAX_DATA_EPOCH}" \
python3 - "${METAD}" "${TMPD}/deleted.txt" <<'PY'
import collections,json,os,sys,time
metad, delfile = sys.argv[1], sys.argv[2]
demo=set(os.environ["DEMO_CLUSTERS"].split())
gap_thresh_ms=float(os.environ["GAP_THRESH_H"])*3600000
require=os.environ["REQUIRE_SETTLED"]=="1"
min_e=int(os.environ["MIN_DATA_EPOCH"])*1000; max_e=int(os.environ["MAX_DATA_EPOCH"])*1000
now_ms=int(time.time()*1000)
settled_delay_ms={
    "5m": float(os.environ["RAW_TO_5M_DELAY_H"])*3600000,
    "1h": float(os.environ["FIVEM_TO_1H_DELAY_H"])*3600000,
}
deleted=set(x.strip() for x in open(delfile) if x.strip())
RES={0:"raw",300000:"5m",3600000:"1h"}
by=collections.defaultdict(lambda:{"n":0,"level":collections.Counter(),"res":collections.Counter(),
    "maxspan":0.0,"lvlivals":collections.defaultdict(list),"resivals":collections.defaultdict(list)})
skipped_del=0
for fn in os.listdir(metad):
    u=fn[:-5]
    if u in deleted: skipped_del+=1; continue
    try: m=json.load(open(os.path.join(metad,fn)))
    except Exception: continue
    cl=m.get("thanos",{}).get("labels",{}).get("cluster","")
    if cl not in demo: continue
    res=RES.get(m.get("thanos",{}).get("downsample",{}).get("resolution",0),"?")
    lvl=m.get("compaction",{}).get("level",0)
    a,b=m["minTime"],m["maxTime"]
    d=by[cl]; d["n"]+=1; d["level"][lvl]+=1; d["res"][res]+=1
    d["maxspan"]=max(d["maxspan"],(b-a)/3600000.0)
    d["lvlivals"][(res,lvl)].append((a,b))
    d["resivals"][res].append((a,b))

def overlaps(ivals):
    ivals=sorted(ivals); n=0; mx=None
    for a,b in ivals:
        if mx is not None and a<mx: n+=1     # strict: abutting (a==prev_b) is legal
        mx=b if mx is None else max(mx,b)
    return n

def coverage_gaps(ivals):
    ivals=sorted(ivals); merged=[]
    for a,b in ivals:
        if merged and a<=merged[-1][1]: merged[-1]=(merged[-1][0],max(merged[-1][1],b))
        else: merged.append((a,b))
    gaps=[(merged[i][1],merged[i+1][0]) for i in range(len(merged)-1) if merged[i+1][0]-merged[i][1]>gap_thresh_ms]
    span=(merged[-1][1]-merged[0][0])/86400000.0 if merged else 0
    return span,gaps,merged

FAIL=False
print(f"  (excluded {skipped_del} deletion-marked blocks)")
for cl in sorted(by):
    d=by[cl]; cl_ok=True
    print(f"\n  cluster {cl}: {d['n']} live blocks")
    print(f"    levels:      {dict(sorted(d['level'].items()))}")
    print(f"    resolutions: {dict(d['res'])}")
    print(f"    largest span: {d['maxspan']:.1f}h")
    # overlaps within (resolution, level)
    tot_ov=0
    for (res,lvl),iv in sorted(d["lvlivals"].items()):
        ov=overlaps(iv)
        if ov: tot_ov+=ov; print(f"    !! {ov} OVERLAP(s) within res={res} level={lvl}  (compactor halt risk)")
    if tot_ov: cl_ok=False; FAIL=True
    else: print(f"    overlaps: none (within each resolution+level)")
    # Coverage/gaps per resolution tier. Raw is the source of truth, so a raw gap
    # is data loss. Gaps in a downsampled tier are legal: Store Gateway falls back
    # to a finer tier for short edge blocks that Thanos never downsamples.
    coverage={}
    for res,iv in sorted(d["resivals"].items()):
        span,gaps,merged=coverage_gaps(iv); coverage[res]=(span,gaps,merged)
        if gaps and res == "raw": note=f"  !! {len(gaps)} gap(s) >{os.environ['GAP_THRESH_H']}h"
        elif gaps: note=f"  ({len(gaps)} gap(s); finer-resolution fallback)"
        else: note=""
        print(f"    [{res:>3}] {len(iv):>4} blocks, covers {span:5.1f}d{note}")
        if gaps:
            if res == "raw": cl_ok=False; FAIL=True
            for g0,g1 in gaps[:3]: print(f"         gap {(g1-g0)/3600000:.1f}h")
    if min_e and max_e:
        raw=coverage.get("raw",(0,[],[]))[2]
        if not raw or raw[0][0]>min_e+gap_thresh_ms or raw[-1][1]<max_e-gap_thresh_ms:
            print("    !! raw resolution does not span the full expected window")
            cl_ok=False; FAIL=True
    # per-cluster settled checks
    comp = any(l>1 for l in d["level"])
    has5m=d["res"].get("5m",0)>0; has1h=d["res"].get("1h",0)>0
    print(f"    compaction ran: {'YES' if comp else 'no (still level-1)'} | 5m: {'YES' if has5m else 'no'} | 1h: {'YES' if has1h else 'no'}")
    if require:
        if not comp: print(f"    !! REQUIRE_SETTLED: compaction not done"); cl_ok=False; FAIL=True
        if not has5m: print(f"    !! REQUIRE_SETTLED: 5m downsampling missing"); cl_ok=False; FAIL=True
        if not has1h: print(f"    !! REQUIRE_SETTLED: 1h downsampling missing"); cl_ok=False; FAIL=True
        if min_e and max_e:
            for res,delay_ms in settled_delay_ms.items():
                merged=coverage.get(res,(0,[],[]))[2]
                eligible_end=min(max_e,now_ms-delay_ms)
                if merged and merged[0][0]>min_e+gap_thresh_ms:
                    print(f"    !! REQUIRE_SETTLED: {res} does not reach the oldest expected data"); cl_ok=False; FAIL=True
                if merged and eligible_end>min_e and merged[-1][1]<eligible_end-gap_thresh_ms:
                    short_h=(eligible_end-merged[-1][1])/3600000
                    print(f"    !! REQUIRE_SETTLED: {res} is {short_h:.1f}h short of its age-eligible horizon"); cl_ok=False; FAIL=True
    print(f"    => cluster {cl}: {'OK' if cl_ok else 'PROBLEM'}")

missing=[c for c in demo if c not in by]
if missing: print(f"\n  !! clusters with NO blocks: {missing}"); FAIL=True

print("\n  == verdict ==")
print("    "+("FAIL — see !! lines above" if FAIL else "PASS — no overlaps, raw coverage intact"+(" , compaction+age-eligible 5m+1h settled" if require else "")))
sys.exit(1 if FAIL else 0)
PY
RC=$?
echo
[[ "${RC}" -eq 0 ]] && echo "== validate_compaction: PASS ==" || echo "== validate_compaction: FAIL (exit ${RC}) =="
exit "${RC}"
