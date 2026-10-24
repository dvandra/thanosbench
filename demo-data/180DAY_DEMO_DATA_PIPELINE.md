# 180-Day Right-Sizing Demo Data Pipeline — Process & Review Findings

> **Status:** operational doc for the `demo-data/` scripts, committed with them. The
> scripts store no credentials: they read the object-store secret from the cluster at
> run time and refuse to run unless `EXPECTED_SERVER_SUBSTR` matches the logged-in hub.
>
> **Update 2026-09-25:** the profile now has realistic filler (`NUM_POD_METRICS=20
> NUM_CLUSTER_METRICS=154` → 134,572 series/block for N=100/W=10/P=3) and unique pod
> names; preflight/upload size capacity for the downsampled copies too. Numbers in
> §3/§4 marked "without filler" predate this; see the notes under each section.
>
> **Update 2026-09-28:** the hub's object store is S3, not in-cluster MinIO.
> `generate_and_publish.sh` replaces the generate-everything-then-upload flow (§1, §4,
> §5): one cluster-week at a time, uploaded while the next generates, ~3 GB of local
> disk, resumable. `configure_hub_s3.sh` points the hub at its S3 prefix; the
> validators read the object store from the hub's Secret. The MinIO-only scripts
> (`upload_180day_batched.sh`, `expand_minio_pvc.sh`) are legacy. See `README.md`.
>
> **Scope:** generate synthetic ACM right-sizing data (trial or full 180-day), load
> it into a shared ACM MultiClusterObservability (MCO) hub's Thanos/MinIO backstore,
> and validate that Thanos compaction + downsampling behave correctly.
>
> **Validated 2026-09-24:** 20-cluster × 2-week trial (N=100/W=10/P=3, 360 blocks,
> 74,418 series/block) on jdj64. Compaction confirmed: levels 4–7, raw+5m+1h tiers,
> largest block 331.7h, no overlaps, all 20 clusters OK.
>
> **Shared S3 distribution plan:** see `SHARED_S3_PLAN.md` (v3, reviewed × 2 rounds)

---

## 1. What this pipeline does

`thanosbench` generates synthetic Prometheus/Thanos TSDB blocks from a named
profile. For the leveled right-sizing model we use `custom-continous-1-week-full`
(cluster → namespace → workload → pod, with a `profile` label = Max OverAll / P95 /
P99, byte-scale memory, and `recommendation = usage × 1.10`).

The end-to-end flow is **four stages** — note that generation writes to **local
disk**, and a separate step uploads to object storage. You do not "generate into
S3" directly.

```
 generate_180day.sh          preflight_180day.sh        upload_180day_batched.sh       validate_*
 ┌────────────────┐          ┌────────────────┐         ┌────────────────────┐         ┌──────────────┐
 │ thanosbench →  │  local   │ READ-ONLY      │  GO     │ local blocks →     │ resync  │ compaction   │
 │ local disk     │ ───────▶ │ go/no-go gates │ ──────▶ │ MinIO `thanos`     │ ──────▶ │ + query      │
 │ (flat ULIDs)   │  blocks  │ (retention,    │         │ bucket (already    │         │ validation   │
 │ + run-manifest │          │  capacity,…)   │         │ wired to Thanos)   │         │              │
 └────────────────┘          └────────────────┘         └────────────────────┘         └──────────────┘
```

**Key architectural fact:** on the target hub the object store is **already wired
to Thanos** (the `thanos-object-storage` secret, set up by ACM MCO, points Thanos
at the in-cluster MinIO `thanos` bucket). Wiring Thanos → bucket is a **one-time**
setup; this pipeline does **not** reconfigure Thanos — it only puts blocks into the
existing bucket and lets the store-gateway sync them. (Configuring the bucket is
only needed when standing up / normalizing a fresh Thanos — e.g. the earlier
`reconfigure_thanos_minio.sh` on a different hub.)

---

## 2. How Thanos treats the data (the mental model)

Three **independent** mechanisms act on blocks; the "180 days / 2 weeks" intuition
maps to compaction + downsampling + retention differently:

| Mechanism | What it does | Trigger |
|---|---|---|
| **Compaction** | merges adjacent blocks into larger ones | block **time-span**, per external-label set; halts on overlap |
| **Downsampling** | adds lower-resolution **copies** (5m, then 1h) | block span ≥ **40h** → 5m; ≥ **10d** → 1h |
| **Retention** | **deletes** blocks per resolution | block **maxTime** older than `retention.resolution-{raw,5m,1h}` |

For a settled 180-day dataset per cluster you end up with the **full tier pyramid**:
raw + 5m + 1h, each covering the whole range, blocks merged up to the compactor's
max window (~2 weeks; configurable). Downsampling is **additive** (raw is retained).

Correctness properties we rely on (all confirmed by the Thanos-expert review):
- Compaction groups strictly by **external-label set** → each demo cluster compacts
  independently, never mixing with real data. Deletion by `cluster` label is therefore safe.
- Downsampling resolution values in `meta.json` (`thanos.downsample.resolution`, ms):
  **`0` raw / `300000` 5m / `3600000` 1h**.
- `recommendation = usage × 1.10` **survives downsampling** — constant scaling commutes
  with the min/max/sum/count aggregates, so the ratio holds at every tier.
- Our raw cadence is **15 min** (filler: 5 min), no finer than the 5m bucket, so 5m
  downsampling doesn't reduce the point count at all, and every point is stored as 5
  aggregates (count/sum/min/max/counter). The 5m copy is therefore **~3.8× larger** than
  raw (measured with Thanos v0.42.4: a 248 MB 48h block became 945 MB), and the 1h copy
  is about the raw size (~0.85–1.1×). Downsampling only saves space for data scraped
  faster than every 5 min; here the hub ends up holding **~5.9× the upload**.

### Two corrections to earlier assumptions (important)
1. **Consistency-delay keys off ULID mint time, not data age.** The compactor's
   `--consistency-delay` (default **30m**) ignores any block whose *ULID* is younger
   than 30m. Our ULIDs are minted at generation time = *now*, so **every** freshly
   uploaded block waits ~30m before the compactor touches it — regardless of how old
   its data timestamps are. Compaction/downsampling therefore begin **~30m after
   upload**, not ~5m. (Store-gateway does **not** apply this delay → raw data is still
   queryable within minutes.)
2. **Retention below 180d silently discards the run.** Retention deletes by
   **`maxTime`**. Because we backfill blocks with data timestamps up to ~180d old, if
   any tier's MCO retention is below ~182d the compactor deletion-marks and removes
   most blocks **within hours** of upload — the entire 78 GB run is wasted. This is the
   #1 pre-check, not a footnote.

---

## 3. Configs & measured economics

Measured on the validated 3-day run (small config): avg **7.24 MB/block**, index ≈
50% of each block (**cardinality-heavy, sample-sparse**), ~**9.9 bytes/sample**,
~**1.46M samples/sec** generation throughput.

Series/block = `3 profiles × (6 measures × (1 + N + N·W + N·W·P) + 2N)`.

| Config | N / W / P | series/block | total (702 blocks) | volume |
|---|---|---|---|---|
| **Small** (validated) | 20 / 5 / 10 | 20,298 | 3 clusters × 234 | ~10 GB |
| **Full** (default driver) | 40 / 10 / 20 | 152,178 | 3 clusters × 234 | ~79 GB |

The table is **without filler**. With the recommended filler (`NUM_POD_METRICS=20
NUM_CLUSTER_METRICS=154`, N=100/W=10/P=3) a block has 134,572 series and one cluster-week
measured ~0.92 GB, so 20 clusters × 26 weeks is ~0.5 TB to upload and ~3 TB on the hub
after downsampling.

Both produce **702 blocks** (9 blocks/week × 26 weeks × 3 clusters) over 182 days —
block *count* is time-driven, not cardinality-driven. `custom-continous-1-week-full`
spans exactly **168h**, so stepping `--max-time` by 7 days tiles cleanly (only ~15-min
seam gaps from the first-sample shave; gaps are fine, overlaps would halt the compactor).

---

## 4. Timing expectations

For the S3 distribution workflow, keep master creation and hub loading as separate
phases. `generate_and_publish.sh` should publish only to the immutable master
(`HUB_PREFIX` unset). After certification, copy the entire master into an empty
hub-owned prefix with block data first and every `meta.json` last. This avoids serializing
a hub copy behind every generated cluster-week and keeps the master safe from compaction.

**Measured one-cluster S3 pilot (2026-10-24, recommended filler):** median generation
was 59 s per cluster-week, local-to-master upload was 157 s per cluster-week, and the
old inline per-week master-to-hub copy was another 142 s. Three uploads were concurrent.
The inline hub-copy stage is therefore intentionally omitted for future master builds;
the completed master is bulk-copied once when a hub test is requested.

After loading that pilot into the hub, the deployed Perses overview PromQL was measured
over the full 182-day range. Workload queries returned one aggregate in 7.09-7.49 s.
Pod queries returned 3,000 grouped pods in 77.26-87.07 s (Max OverAll 77.46 s,
P95 87.07 s, P99 77.26 s). These are the single-cluster baselines for the 20/100/300
scale tests; record cold and warm runs separately because query-frontend caching can
materially change subsequent latency.

During four-way 5m downsampling of the same pilot, the compactor reached about 2.94 CPU
cores and 17.5 GiB memory. Its deployed request was only 500m/1 GiB (with no limits),
while the single node had 15.5 cores and about 61 GiB allocatable memory (about 37 GiB
in use at the sample). Treat node headroom, peak compactor RSS, queue depth, and time to
settle as required measurements for every 20/100/300-cluster run. Do not raise
`--downsample.concurrency` without first proving the resulting memory peak fits the node.

The settled pilot had 29 live raw blocks covering 182.0 days, 26 5m blocks covering
181.1 days, and 13 1h blocks spanning the age-eligible 179.1 days. The 1h tier contained
twelve 48.1-hour gaps that Thanos serves from the finer 5m/raw tiers; these are not data
loss. `validate_compaction_180day.sh` therefore requires gap-free full raw coverage and
age-eligible 5m/1h boundaries (40 hours and 10 days), rather than incorrectly requiring
each downsample tier to cover fresh data or short edge blocks.

Extrapolated from measured throughput; the two big unknowns are **port-forward
throughput** (~10–40 MB/s) and the **shared compactor's** speed.

| Phase | Trial (20 clusters × 2w, N=100/W=10/P=3) | Full (20 clusters × 26w) | Bottleneck |
|---|---|---|---|
| Generate (local) | **~17 min measured** (995s for 360 blks) | ~3.5 h (×13) | CPU / index build; ~9 GB RAM peak |
| Upload → MinIO (port-forward) | **~2.5h measured** (360 blocks with 60s stage pause) | ~30h (×13 blocks + pauses) | port-forward ~10-40 MB/s; use `STAGE_PAUSE_SEC=60` |
| Raw queryable (store-gw sync) | ~2–5 min | ~5–15 min | index-header load |
| Compaction + 5m + 1h settle | **~1h measured** (all 20 clusters, all tiers) | ~2–6 h | single compactor, 3× rewrite |

The rows above are **without filler**. With the recommended filler, generation measured
~90 s per cluster-week (~13 h for 20 clusters × 26 weeks, ~8.2 GB RAM peak), and the
compactor has ~89B samples to downsample for the full run (estimated 5–8 h at the
3–5M samples/s measured locally).

**Measured on jdj64 (2026-09-24):** 20-cluster trial, 74,418 series/block, ~44 MB/block.
Compaction completed aggressively: 452/568 blocks deletion-marked within ~1h of upload,
all clusters reached levels 4–7, 331.7h largest span, raw+5m+1h confirmed.

**When can you consume?** Raw data: ~5–15 min after store-gw resync (you do *not*
wait for the compactor). Fast 180-day dashboards (served from the 1h tier): after the
compactor settles. Compaction itself starts ~30 min post-upload (consistency-delay).

---

## 5. The scripts

All are operator-run ops scripts. Credentials are always read in-place from
the `thanos-object-storage` secret and **never printed**. A **run-manifest**
(`gen-180day.manifest.json`) written by the generator is the single source of truth
(base epoch, weeks, clusters, cardinality, expected counts) that the later stages read.

| Script | Role | Key env vars |
|---|---|---|
| `generate_180day.sh` | blocks → local disk + manifest | `NUM_NAMESPACES/WORKLOADS/PODS`, `WEEKS`, `FORCE`, `DRY_RUN`, `OUT` |
| `preflight_180day.sh` | READ-ONLY go/no-go gates | `MIN_RETENTION_DAYS` (182), `CAP_MARGIN` |
| `expand_minio_pvc.sh` | replace emptyDir → 500Gi PVC on MinIO | `PVC_SIZE` (500Gi), `EXPECTED_SERVER_SUBSTR` |
| `upload_180day_batched.sh` | local blocks → MinIO + resync | `RESUME`, `TEARDOWN`, `RESTART_STORE` (rolling\|all\|none), `STAGE_PAUSE_SEC`, `PARALLEL`, `SKIP_CAPACITY_CHECK` |
| `validate_compaction_180day.sh` | compaction/downsampling in MinIO | `REQUIRE_SETTLED`, `GAP_THRESH_H`, `MANIFEST` |
| `validate_load.sh` | query-side correctness (full range) | `DAYS` (182/14), `STEP`, `SVC`, `MANIFEST` |

**Known script gotchas fixed 2026-09-24:**
- Bash `eval "$(python3 - "${MANIFEST}" <<'PY'...)"` is broken when the Python contains
  `"` inside f-string `{...}` — bash mis-parses the heredoc and leaks Python source into
  the eval'd string. Fix: write Python output to `$(mktemp)` then `source` it. All scripts
  now use this pattern.
- `validate_load.sh` defaults to `observability-thanos-query-frontend` (port 9090) and a
  300s timeout. At 20-cluster scale, pod-level queries (180k series) time out against the
  raw query service in 60s. Use the query-frontend which has per-query caching.
- If MinIO is running with an emptyDir volume (common on fresh MCO deployments), run
  `expand_minio_pvc.sh` once to replace it with a 500Gi PVC before uploading large datasets.

### Runbook

```bash
# 1. GENERATE (full config). Refuses a non-empty OUT (use FORCE=1 to wipe & regen).
DRY_RUN=1 NUM_NAMESPACES=40 NUM_WORKLOADS=10 NUM_PODS=20 bash generate_180day.sh   # preview
NUM_NAMESPACES=40 NUM_WORKLOADS=10 NUM_PODS=20 bash generate_180day.sh

# 2. PREFLIGHT — must print "GO" before uploading (read-only; no changes).
!  bash preflight_180day.sh

# 3. UPLOAD. Resume a dropped run with RESUME=1; tear down later with TEARDOWN=1.
!  bash upload_180day_batched.sh
!  RESUME=1 bash upload_180day_batched.sh        # after any port-forward drop

# 4. VALIDATE (poll compaction over hours; then query-side).
!  bash validate_compaction_180day.sh            # re-run to watch tiers appear
!  REQUIRE_SETTLED=1 bash validate_compaction_180day.sh   # once you expect it done
   bash validate_load.sh                         # full-range query assertions

# Teardown (label-scoped delete of demo data only):
!  TEARDOWN=1 bash upload_180day_batched.sh
```

Exit codes: validators return **0 = pass, 1 = data problem, 2 = environment/not-ready**
(so they can gate scripts and CI, and "not ready yet" is distinct from "data wrong").

---

## 6. Multi-perspective review — findings & resolutions

Four independent reviews (Architecture, Platform/PE, QE, Thanos-internals) were run
before execution. Cross-reviewer agreement raised confidence. Below: the consolidated,
deduped findings and how each is now handled in the hardened scripts.

### 🔴 Data-correctness (apply regardless of cluster tenancy)
| # | Finding | Reviewers | Resolution |
|---|---|---|---|
| 1 | MCO retention < 182d silently deletes the run by `maxTime` | Thanos, PE | `preflight` check #2 parses `retentionConfig`, requires all tiers ≥182d, NO-GO otherwise |
| 2 | Validators never failed (always exit 0) → false green | QE, PE | both validators now assert and return real exit codes (0/1/2) |
| 3 | `validate_load.sh` checked only last 3 of 182 days | Arch, PE, QE, Thanos | full-range query window from manifest + coverage checks at oldest **and** newest ts + `max_source_resolution=1h` probe |
| 4 | Generation non-idempotent; re-run appends a mis-tiled batch → overlap → halt | Arch, QE, PE | refuses non-empty `OUT` unless `FORCE=1`; `BASE_EPOCH` pinned & floored to a UTC day; per-week + total block-count asserts |
| 5 | Silent `meta.json` undercount faked "no overlaps" / left demo blocks undeleted | QE | delete-scan and validator reconcile listed-vs-fetched metas, retry, and **abort** if incomplete |
| 6 | Overlap check false-positive (level-1 sources vs level-2 parent) **and** false-negative (adjacency-only) | Arch, Thanos, QE | overlap checked per `(cluster, resolution, level)` with a running-max sweep; deletion-marked blocks excluded |
| 7 | 1.10 ratio & verdicts printed, not asserted; global-OR hid a broken cluster | QE | assert ratio min **and** max ∈ [1.0999, 1.1001] with expected series count; per-cluster AND-reduced verdict |

### 🟠 Operational safety (blockers on a shared cluster)
| # | Finding | Reviewers | Resolution |
|---|---|---|---|
| 8 | No MinIO capacity precheck before ~79 GB to a shared bucket → ENOSPC on real ingestion | PE | `preflight` check #3 + inline guard in `upload` (free ≥ size × margin; `SKIP_CAPACITY_CHECK` override) |
| 9 | `sync`/`cp` doesn't upload `meta.json` last → store-gw reads partial blocks mid-run | PE, Thanos | per-block **two-phase**: data first (`--exclude meta.json`), then `meta.json` last |
| 10 | All store shards restarted at once → full query outage; OOM risk from 702 large blocks | Arch, PE, Thanos | **rolling** restart (one shard, wait Ready) by default; `RESTART_STORE=none` to rely on auto-sync; preflight prints shard mem headroom |

### 🟡 Minors — all addressed
Temp `AWS_CONFIG_FILE` (no global `~/.aws` mutation) · random local port + `mktemp`
temp files (no collisions) · empty `EXPECTED_SERVER_SUBSTR` rejected · local `df`
precheck before ~79 GB gen · gap detection (a missing week can't hide behind "182d")
· deletion-marked blocks excluded from counts/coverage · decorrelated gauge jitter ·
`request_hard`, pod-level byte-scale, and profile/workload_type balance now asserted ·
staged-by-cluster upload with progress + optional `STAGE_PAUSE_SEC` · `TEARDOWN` mode ·
compactor scratch-PVC and downsampling-enabled checks in preflight · stale header
comments fixed.

### ✅ Confirmed correct (unchanged)
Per-external-label compaction grouping and delete-by-`cluster`-label safety · halt-on-overlap
(no vertical compaction by default) · downsampling thresholds 40h→5m, 10d→1h ·
resolution ms values 0/300000/3600000 and the `thanos.downsample.resolution` path ·
the 1.10 invariant surviving downsampling · additive downsampling · store-gw queryable
in minutes · 168h weekly tiling avoids overlaps · no replica/dedup needed · 5M series/block cap.

---

## 7. Safety model & standing constraints

- **Hub is treated as read-only** except for the deliberate demo-data mutations, which
  are delivered as **user-run** scripts (never executed by the assistant).
- **Credentials never printed** — read in-place from the secret; the permission
  classifier blocks direct reads of `thanos-object-storage`.
- **Blast radius contained** — the pipeline only ever deletes blocks whose external
  `cluster` label is one of the demo clusters in the run-manifest (`ac-test-man-*`),
  never real tenant data, and aborts if it cannot prove the demo set is clean.
- **No secrets in git** — the scripts and this doc are committed, but no credentials,
  kubeconfigs, generated blocks or run-manifests are (see `.gitignore`).

---

## 8. Known limitations / open items

- **Compactor churn is unpredictable** at full config (single `observability-thanos-compact-0`,
  hours). `validate_compaction_180day.sh` is re-runnable to poll; use `REQUIRE_SETTLED=1`
  only once you expect it done.
- **Port-forward at 79 GB** is the slow, drop-prone link. Resumable (`RESUME=1`), but the
  faster long-term path is an **in-cluster Job** (generate + push over the cluster network),
  which is designed but not yet built (needs a thanosbench image).
- **Data ages out** — timestamps are pinned to wall-clock at generation. The oldest week
  ends ~175 days before generation, so with MCO's default 365d retention it is deleted
  ~190 days after generation (only ~7 days at the 182d minimum). Regenerate to refresh.
- **No pod churn** — the same pods exist for all 180 days (real pods get new names on
  every rollout), so long-range pod-level queries come out somewhat optimistic.
- **Filler is thinner than real metrics** — it matches the series count and the
  pod/namespace fan-out, but real series carry more labels (uid, node, image, …).
- **`workload_type` casing** (PascalCase) still pending confirmation against the ACM
  dashboard's expected values (tracked as an open PR question, unrelated to this pipeline).
