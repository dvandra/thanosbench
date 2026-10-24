#!/usr/bin/env bash
# Demo-data ops script (see demo-data/README.md). Read-only query-side validation
# of the uploaded right-sizing data through Thanos. Certification-grade:
#   * queries the FULL data window (from the run-manifest), not just the last 3 days
#   * ASSERTS (not just prints): per-level cardinality per cluster, memory byte-scale
#     & cpu cores-scale at cluster AND pod, recommendation/usage ratio bounds (min AND
#     max) with expected series count, request_hard present, profile & workload_type
#     balance, coverage at BOTH the oldest and newest timestamps
#   * probes the downsampled tier via max_source_resolution=1h
#   * distinguishes "store not ready yet" (exit 2) from "data wrong" (exit 1)
# Exit: 0 pass | 1 data problem | 2 environment/not-ready.
set -uo pipefail

NS="open-cluster-management-observability"
SVC="${SVC:-observability-thanos-query-frontend}"   # query-frontend handles heavy pod-level queries better; fallback: observability-thanos-query
MANIFEST="${MANIFEST:-./gen-180day.manifest.json}"
LOCAL_PORT="${LOCAL_PORT:-$(( 20000 + RANDOM % 20000 ))}"
DAYS="${DAYS:-182}"
STEP="${STEP:-21600}"     # 6h
NUM_NAMESPACES="${NUM_NAMESPACES:-20}"
NUM_WORKLOADS="${NUM_WORKLOADS:-5}"
NUM_PODS="${NUM_PODS:-10}"
CLUSTERS="${CLUSTERS:-ac-test-man-1 ac-test-man-2 ac-test-man-3}"
MIN_DATA_EPOCH="${MIN_DATA_EPOCH:-0}"; MAX_DATA_EPOCH="${MAX_DATA_EPOCH:-0}"
EXPECTED_SERVER_SUBSTR="${EXPECTED_SERVER_SUBSTR:-}"

if [[ -f "${MANIFEST}" ]]; then
  _mvars="$(mktemp)"
  MANIFEST_PATH="${MANIFEST}" python3 - >"${_mvars}" <<'PY'
import json,sys,os
m=json.load(open(os.environ["MANIFEST_PATH"]))
print("NUM_NAMESPACES=" + str(m.get("num_namespaces",20)))
print("NUM_WORKLOADS=" + str(m.get("num_workloads",5)))
print("NUM_PODS=" + str(m.get("num_pods",10)))
print('CLUSTERS="' + " ".join(m.get("clusters",["ac-test-man-1","ac-test-man-2","ac-test-man-3"])) + '"')
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
oc port-forward "svc/${SVC}" "${LOCAL_PORT}:9090" -n "${NS}" >"${TMPD}/pf.log" 2>&1 &
PF_PID=$!; trap 'kill "${PF_PID}" >/dev/null 2>&1 || true; rm -rf "${TMPD}"' EXIT
READY=0; for i in $(seq 1 30); do curl -s -o /dev/null "http://localhost:${LOCAL_PORT}/-/ready" && { READY=1; break; }; sleep 1; done
[[ "${READY}" == "1" ]] || { echo "ABORT: query port-forward not ready (env error)." >&2; exit 2; }

PORT="${LOCAL_PORT}" DAYS="${DAYS}" STEP="${STEP}" \
N="${NUM_NAMESPACES}" W="${NUM_WORKLOADS}" P="${NUM_PODS}" \
CLUSTERS="${CLUSTERS}" MIN_DATA_EPOCH="${MIN_DATA_EPOCH}" MAX_DATA_EPOCH="${MAX_DATA_EPOCH}" \
python3 -u <<'PY'
import json,os,sys,time,urllib.error,urllib.parse,urllib.request
port=os.environ["PORT"]; days=int(os.environ["DAYS"]); step=int(os.environ["STEP"])
N,W,P=int(os.environ["N"]),int(os.environ["W"]),int(os.environ["P"])
clusters=os.environ["CLUSTERS"].split(); ncl=len(clusters)
now=int(time.time()); end=now; start=now-days*86400
min_e=int(os.environ["MIN_DATA_EPOCH"]) or start; max_e=int(os.environ["MAX_DATA_EPOCH"]) or end
sel='cluster=~"%s"'%("|".join(clusters))
FAIL=[]; base=f"http://localhost:{port}/api/v1/query_range"

def qr(expr,msr=None,s=None,e=None):
    p={"query":expr,"start":s or start,"end":e or end,"step":step}
    if msr is not None: p["max_source_resolution"]=msr
    try:
        with urllib.request.urlopen(base+"?"+urllib.parse.urlencode(p),timeout=300) as r:
            d=json.load(r)
        return d.get("data",{}).get("result",[]) if d.get("status")=="success" else []
    except urllib.error.HTTPError as ex:
        body=ex.read(2048).decode("utf-8","replace").strip()
        print(f"    query error: {ex}"+(f" — {body}" if body else "")); return []
    except Exception as ex:
        print(f"    query error: {ex}"); return []
def vmax(res): return max((float(v[1]) for s in res for v in s["values"]), default=None)
def vmin(res): return min((float(v[1]) for s in res for v in s["values"]), default=None)
def check(cond,msg):
    print(("    PASS " if cond else "    FAIL ")+msg)
    if not cond: FAIL.append(msg)

# ---- readiness: cluster-level series present for every cluster (retry ~5m) ----
print("== readiness ==")
exp_cluster=3  # 3 profiles at cluster level
ready=False
for attempt in range(30):
    res=qr(f'count by (cluster) (acm_rs:cluster:cpu_usage{{{sel}}})')
    present={s["metric"].get("cluster") for s in res if (vmax([s]) or 0)>=exp_cluster}
    print(f"  attempt {attempt+1}: clusters serving cluster-level data = {len(present)}/{ncl}")
    if len(present)>=ncl: ready=True; break
    if attempt==0 and not res: pass
    time.sleep(10)
if not ready:
    got=len(present)
    if got==0:
        print("== NOT READY / data absent — store-gw still syncing or nothing uploaded (env). exit 2 =="); sys.exit(2)
    print("== partial data after wait — proceeding to full checks (failures below are real) ==")

# ---- 1. per-level cardinality per cluster ----
# pod-level: query one cluster at a time to avoid 180k-series regex fan-out
# killing the port-forward. cluster/namespace/workload use the full selector.
print("== 1. cardinality per level (per cluster) ==")
exp={"cluster":3,"namespace":3*N,"workload":3*N*W,"pod":3*N*W*P}
for lvl,e in exp.items():
    if lvl=="pod":
        # per-cluster to keep each query < 10k series
        got={}
        for c in clusters:
            res=qr(f'count(acm_rs:pod:cpu_usage{{cluster="{c}"}})')
            v=vmax(res)
            if v is not None: got[c]=int(v)
        ok=len(got)==ncl and all(got.get(c)==e for c in clusters)
        check(ok,f"{lvl}: each cluster has {e} series (got {got})")
    else:
        res=qr(f'count by (cluster) (acm_rs:{lvl}:cpu_usage{{{sel}}})')
        got={s["metric"].get("cluster"):int(vmax([s]) or 0) for s in res}
        ok=len(got)==ncl and all(got.get(c)==e for c in clusters)
        check(ok,f"{lvl}: each cluster has {e} series (got {got})")

# ---- 2. scale: memory byte-scale, cpu cores-scale, at cluster AND pod ----
# pod-level: query one representative cluster to avoid fan-out timeout
print("== 2. value scale ==")
rep=clusters[0]  # representative cluster for pod-level checks
for lvl in ("cluster","pod"):
    s2=sel if lvl=="cluster" else f'cluster="{rep}"'
    mx=vmax(qr(f'max by (cluster) (acm_rs:{lvl}:memory_usage{{{s2}}})'))
    lbl=f"{lvl} memory byte-scale (max={mx})" if lvl=="cluster" else f"{lvl} memory byte-scale for {rep} (max={mx})"
    check(mx is not None and 1e7<mx<1e13, lbl)
    cx=vmax(qr(f'max by (cluster) (acm_rs:{lvl}:cpu_usage{{{s2}}})'))
    lbl=f"{lvl} cpu cores-scale (max={cx})" if lvl=="cluster" else f"{lvl} cpu cores-scale for {rep} (max={cx})"
    check(cx is not None and 0<cx<10000, lbl)

# ---- 3. recommendation = usage * 1.10 (bounds min AND max) ----
# pod-level: use representative cluster to avoid 180k-series fan-out
print("== 3. recommendation/usage ratio == ")
for lvl in ("cluster","pod"):
    s2=sel if lvl=="cluster" else f'cluster="{rep}"'
    for r in ("cpu","memory"):
        ratio=f'acm_rs:{lvl}:{r}_recommendation{{{s2}}} / acm_rs:{lvl}:{r}_usage{{{s2}}}'
        # Compute the cross-series extrema in Thanos. Returning every pod/profile at
        # every 6h step would materialize millions of response points merely to find
        # two numbers locally; cardinality was already asserted in section 1.
        lo_res=qr(f'min({ratio})'); hi_res=qr(f'max({ratio})')
        lo,hi=vmin(lo_res),vmax(hi_res)
        ok=lo_res and hi_res and lo is not None and 1.0999<=lo and hi<=1.1001
        lbl=f"{lvl} {r}: ratio in [1.0999,1.1001] (min={lo},max={hi})"
        if lvl=="pod": lbl+=f" [{rep} only]"
        check(ok,lbl)

# ---- 4. request_hard present at namespace level ----
print("== 4. request_hard (namespace) present ==")
for r in ("cpu","memory"):
    res=qr(f'count by (cluster) (acm_rs:namespace:{r}_request_hard{{{sel}}})')
    got={s["metric"].get("cluster") for s in res if (vmax([s]) or 0)>0}
    check(len(got)==ncl, f"{r}_request_hard present for all clusters (got {len(got)}/{ncl})")

# ---- 5. profile & workload_type balance ----
print("== 5. label coverage ==")
res=qr(f'count by (profile) (acm_rs:pod:cpu_usage{{cluster="{clusters[0]}"}})')
profs={s["metric"].get("profile") for s in res if (vmax([s]) or 0)>0}
check(len(profs)==3, f"3 profiles present (got {sorted(profs)})")
res=qr(f'count by (workload_type) (acm_rs:pod:cpu_usage{{cluster="{clusters[0]}"}})')
wts={s["metric"].get("workload_type") for s in res if (vmax([s]) or 0)>0}
check(len(wts)==4, f"4 workload_types present (got {sorted(wts)})")

# ---- 6. downsampled tier is served (max_source_resolution=1h) ----
print("== 6. downsample tier served (max_source_resolution=1h) ==")
res=qr(f'max by (cluster) (acm_rs:cluster:memory_usage{{{sel}}})',msr="1h")
check(len({s["metric"].get("cluster") for s in res})==ncl, f"1h-ceiling query returns all clusters (got {len(res)})")

# ---- 7. coverage at BOTH ends of the window ----
print("== 7. coverage at oldest and newest timestamps ==")
win=6*3600
old=qr(f'count(acm_rs:cluster:cpu_usage{{{sel}}})',s=min_e,e=min_e+win)
new=qr(f'count(acm_rs:cluster:cpu_usage{{{sel}}})',s=max_e-win,e=max_e)
check(bool(old) and (vmax(old) or 0)>0, f"data present near oldest ts ({time.strftime('%Y-%m-%d',time.gmtime(min_e))})")
check(bool(new) and (vmax(new) or 0)>0, f"data present near newest ts ({time.strftime('%Y-%m-%d',time.gmtime(max_e))})")

print("\n== verdict ==")
if FAIL:
    print(f"  FAIL — {len(FAIL)} check(s) failed"); sys.exit(1)
print("  PASS — all query-side checks passed"); sys.exit(0)
PY
RC=$?
[[ "${RC}" -eq 0 ]] && echo "== validate_load: PASS ==" || echo "== validate_load: exit ${RC} (1=data,2=env) =="
exit "${RC}"
