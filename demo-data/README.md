# 180-Day Right-Sizing Demo Data Pipeline

Scripts for generating, publishing, and validating synthetic ACM right-sizing metrics
(180 days) for a MultiClusterObservability (MCO) hub whose Thanos uses an **S3 bucket**
as object store. Blocks are generated one cluster-week at a time and uploaded to an
immutable master S3 prefix while the next one generates (a few GB of local disk).
When a hub needs the dataset, the completed master is copied into a private, hub-owned
prefix. Nothing goes through a PVC or in-cluster MinIO.

See `180DAY_DEMO_DATA_PIPELINE.md` for full architecture, timing expectations, and
multi-perspective review findings. See `SHARED_S3_PLAN.md` for the multi-team S3
distribution design.

---

## Prerequisites

| Tool | Version |
|---|---|
| `oc` | logged in to the target hub cluster |
| `aws` CLI | configured (creds are read from the MCO secret; no `~/.aws` needed) |
| `python3` | 3.8+ |
| `thanosbench` binary | built from this repo (`make build`; on recent macOS see the build note in `RIGHT_SIZING.md`) |

---

## Required environment variable

Every script in this folder enforces a **cluster safety guard** — it reads the current
`oc whoami --show-server` URL and aborts if it does not contain `EXPECTED_SERVER_SUBSTR`.
This prevents accidentally running demo-data operations against the wrong cluster.

**You must set this before running any script:**

```bash
export EXPECTED_SERVER_SUBSTR="<substring of your hub's API server URL>"

# Example — copy the unique part of your cluster URL:
#   oc whoami --show-server
#   https://api.obsint-sno-4xlarge-5-myhub.llc.devcluster.openshift.com:6443
#                              ^^^^^^^^^^^^^^^^^
export EXPECTED_SERVER_SUBSTR="obsint-sno-4xlarge-5-myhub"
```

If unset, every script aborts immediately with:
```
ABORT: EXPECTED_SERVER_SUBSTR is empty
```

---

## Scripts

| Script | Role | Key env vars |
|---|---|---|
| `generate_and_publish.sh` | Generate one cluster-week at a time and upload to the immutable master while the next generates; resumable; writes `manifest.json` last. Leave `HUB_PREFIX` unset for master production. | `CLUSTERS`, `WEEKS`, `NUM_*`, `BUCKET`, `SHARED_PREFIX`, `PER_CLUSTER_PREFIX`, `AWS_PROFILE_NAME`, `MAX_INFLIGHT`, `UPLOAD_BLOCK_PARALLEL`, `UPLOAD_RETRY_*`, `WORK`, `DRY_RUN` |
| `copy_master_to_hub.sh` | Copy a completed master to an empty hub-owned S3 prefix, block data first and all `meta.json` files last; exact-ULID verification and safe resume | `BUCKET`, `MASTER_PREFIX`, `DEST_BUCKET`, `HUB_PREFIX`, `AWS_PROFILE_NAME`, `RESUME` |
| `configure_hub_s3.sh` | Point the hub's `thanos-object-storage` Secret at `s3://BUCKET/PREFIX` and restart the Thanos pods; `rollback` restores the backup | `EXPECTED_SERVER_SUBSTR`, `BUCKET`, `PREFIX`, `REGION`, `AWS_PROFILE_NAME` |
| `preflight_180day.sh` | READ-ONLY go/no-go checks (retention, compactor; capacity only for MinIO) | `MIN_RETENTION_DAYS` (182) |
| `validate_compaction_180day.sh` | Assert compaction/downsampling in the hub's object store (S3 or MinIO, read from the Secret) | `REQUIRE_SETTLED`, `GAP_THRESH_H`, `MANIFEST` |
| `validate_load.sh` | Query-side correctness checks via port-forward | `DAYS` (182 or 14 for trial), `STEP`, `SVC`, `MANIFEST` |
| `generate_180day.sh` | Legacy: generate everything to local disk first (needs the whole dataset's disk) | `CLUSTERS`, `NUM_*`, `WEEKS`, `OUT` |
| `upload_180day_batched.sh`, `expand_minio_pvc.sh` | Legacy, **in-cluster MinIO only**; not used in the S3 flow | |

Exit codes for validators: **0 = pass, 1 = data problem, 2 = environment/not-ready**.

---

## Runbook

```bash
# 0. Guard variable (a substring of the hub's API URL) and AWS profile for the bucket
export EXPECTED_SERVER_SUBSTR="<your-cluster-substring>"
export AWS_PROFILE_NAME=oia-rs

# 1. PREFLIGHT — read-only; must print "GO" (retention >= 182d, compactor healthy)
!  bash preflight_180day.sh

# 2. GENERATE + PUBLISH THE MASTER — one cluster-week at a time (~60-90 s, ~1.09 GB),
#    uploaded to the master prefix while the next one generates; ~3 GB of local disk,
#    resumable (re-run the same command). Needs ~8.2 GB RAM. Do NOT set HUB_PREFIX:
#    a master-production run never writes into a hub-owned prefix.
#    NUM_POD_METRICS and NUM_CLUSTER_METRICS add filler for the other metrics a real
#    cluster sends (see RIGHT_SIZING.md). 20 clusters = category 1; 100 / 300 = 2 / 3.
#    Use a new SHARED_PREFIX (and WORK dir) per dataset; the prefix must be empty. Write
#    the date into SHARED_PREFIX by hand: a resume must re-run the exact same command.
GEN_ENV=(CLUSTERS="$(seq -f 'ac-test-man-%g' 1 20 | tr '\n' ' ')"
         NUM_NAMESPACES=100 NUM_WORKLOADS=10 NUM_PODS=3 NUM_POD_METRICS=20 NUM_CLUSTER_METRICS=154
         BUCKET=oia-rs)

#    Plan only:
env "${GEN_ENV[@]}" WEEKS=26 SHARED_PREFIX=rs-perf-shared/20c-180d-2026-09-29 DRY_RUN=1 bash generate_and_publish.sh
#    Trial (20 clusters × 2 weeks; ~1h):
env "${GEN_ENV[@]}" WEEKS=2  SHARED_PREFIX=rs-perf-shared/20c-14d-2026-09-29 WORK=./gen-work-trial bash generate_and_publish.sh
#    Full 180 days (20 clusters × 26 weeks; ~14h, upload-bound; ~0.57 TB uploaded):
env "${GEN_ENV[@]}" WEEKS=26 SHARED_PREFIX=rs-perf-shared/20c-180d-2026-09-29 WORK=./gen-work-180d bash generate_and_publish.sh

# 3. COPY THE CERTIFIED MASTER INTO AN EMPTY, HUB-OWNED PREFIX. Copy every block's data
#    first and all meta.json files last, so Thanos can never discover a partial block.
#    Never point a hub at rs-perf-shared/...: its compactor mutates its object store.
MASTER=rs-perf-shared/20c-180d-2026-09-29
HUB_COPY=jdj64-thanos-20c-180d-2026-09-29
BUCKET=oia-rs MASTER_PREFIX="$MASTER" HUB_PREFIX="$HUB_COPY" \
  bash copy_master_to_hub.sh
# If interrupted, re-run the exact command with RESUME=1.

# 4. POINT THE HUB AT ITS PRIVATE COPY. This backs up the current Secret, writes the S3
#    config, and restarts Thanos. Receive and Rule also write only to this private prefix.
!  BUCKET=oia-rs PREFIX="$HUB_COPY" bash configure_hub_s3.sh

# 5. VALIDATE COMPACTION — reads the object store named in the hub's Secret; run it after
#    the copy has finished and poll until
#    all tiers settle (the compactor starts ~30 min after upload; hours for 180 days)
!  bash validate_compaction_180day.sh             # repeat to watch progress
!  REQUIRE_SETTLED=1 bash validate_compaction_180day.sh   # final assertion

# 6. VALIDATE LOAD — query-side checks (full range) through the query-frontend
   DAYS=14 bash validate_load.sh     # trial window
   bash validate_load.sh             # full 182-day window

# Teardown — remove the dataset from the hub prefix (the shared copy stays). This deletes
#    EVERYTHING under the hub prefix, including the blocks Receive and Rule uploaded since
#    the hub was pointed at it: fine for a private test hub, not for one with real tenants.
   aws s3 rm --recursive s3://oia-rs/jdj64-thanos/ --profile oia-rs
```

Other teams load the same dataset by copying `s3://oia-rs/<SHARED_PREFIX>/` blocks
server-side into their own empty hub prefix (the two-phase commands above) and
pointing their hub at it with `configure_hub_s3.sh`. Never point a hub at the shared
prefix itself: its compactor would rewrite the shared copy.

The master prefix is immutable after `manifest.json` appears. Do not resume generation
against it with different parameters, run compaction against it, or delete individual
blocks. A hub copy is disposable; delete and recreate the entire hub-owned prefix when
starting a clean test.

Commands prefixed with `!` mutate the cluster and should be run by the operator
(type `! <command>` in the Claude Code prompt to run them in-session).

### Categories 2 and 3 (100 / 300 clusters)

Same settings with `CLUSTERS` set to 100 or 300 names. Plan for (recommended filler,
26 weeks, one generating machine):

| | 20 clusters | 100 clusters | 300 clusters |
|---|---|---|---|
| Blocks | 4,680 | 23,400 | 70,200 |
| Upload (raw) | ~0.57 TB | ~2.8 TB | ~8.5 TB |
| On the hub after downsampling | ~3 TB | ~15 TB | ~45 TB |
| Generation alone (measured, 76 s/cluster-week) | ~11h | ~55h | ~165h |
| Upload, and so the whole run (laptop, ~11.6 MB/s) | ~14h | ~68h | ~203h |

- Local disk is no longer a constraint (~3 GB at any time), but upload bandwidth still is.
  A cluster-week's blocks upload in parallel, so the stream count is `MAX_INFLIGHT` ×
  `UPLOAD_BLOCK_PARALLEL`, 9 at the defaults, which sustained **11.6 MB/s** from a laptop to
  us-west-2 with generation running. That is roughly twice the serial-upload rate, but
  generation still finishes ~25% ahead of upload, so plan against the upload row above.
- Measure this on a long run, not a short one. A 1-cluster × 26-week run (28.4 GB) took 42 min
  against 33 min of pure generation — 27% overhead. The same code over 5 weeks showed 62%
  overhead and only 10.0 MB/s, because ramp-up (one stream until the first cluster-week exists)
  and the final drain are a much larger fraction of a short run. Short trials understate
  throughput.
- The run is upload-bound in a way the log makes visible: generation blocks in `wait_for_slot`
  waiting for an upload to free a slot. In that 26-week run it stalled 422 s across 9 of the 26
  cluster-weeks. If a run shows no such stalls (gaps equal to each `generated in Ns`), upload is
  keeping up and the bound has moved to generation.
- Tune the stream count, not a single transfer, and expect it to saturate early. One stream
  measured 3.6 MB/s and barely moved with `max_concurrent_requests=40`; four streams reached
  11.0 MB/s aggregate, and nine sustained 11.6 MB/s. The link, not the stream count, is the
  ceiling past roughly four or five streams, so raising `MAX_INFLIGHT` or
  `UPLOAD_BLOCK_PARALLEL` above the defaults mostly buys local disk pressure. For categories 2
  and 3 run the generator in the bucket's region (an EC2 instance in us-west-2), where that
  ceiling is far higher. Ctrl-C stops the run; re-run the same command to resume.
- A cluster-week whose upload fails is retried in bounded batches after generation finishes
  (`UPLOAD_RETRY_ROUNDS`, default 3). Retries re-upload from the local copy and never
  regenerate, so a transient S3 error no longer costs a second invocation.
- One dataset must be produced by one `generate_and_publish.sh` run (one machine, one
  `WORK` dir): the script owns its `SHARED_PREFIX` and checks the final block count
  against its own plan. Splitting a dataset across machines is not supported.

---

## What NOT to commit

The following are generated at runtime and must not be added to git:

```
gen-work*/                # generate_and_publish.sh state, logs and in-flight blocks
gen-180day-flat/          # legacy generate_180day.sh output (~40 GB for the 2-week trial with filler)
gen-3day-flat/
gen-180day.manifest.json  # run manifest (cluster-specific)
*.backup.*.yaml           # secret backups
```

Add these to `.gitignore` if you work from this directory directly.
