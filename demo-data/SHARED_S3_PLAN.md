# Shared S3 Demo Data Distribution Plan — v2

> Revised after 3-lens review (Architecture / PE / QE). All BLOCKER and MAJOR
> findings addressed. See "Review findings" section at the bottom for traceability.
>
> **Status:** partly implemented. `generate_and_publish.sh` covers the generator side
> (per-block upload with `meta.json` last and `manifest.json` written last).
> Master-production runs leave `HUB_PREFIX` unset. A completed master is bulk-copied,
> data first and all `meta.json` files last, into a private hub prefix;
> `copy_master_to_hub.sh` performs and verifies that copy, and
> `configure_hub_s3.sh` then points that hub at only its private copy.
> `load_from_s3.sh` and `validate_s3_source.sh` (checksums, IAM model) are not built
> yet; today a consumer copies the blocks server-side as described in `README.md`.

## Goal
Generate right-sizing demo blocks once locally, store them in a shared AWS S3 bucket,
and let any ACM hub cluster team pull and load those blocks into their own Thanos
object store — without re-generating or requiring direct cluster access from the generator.

---

## Architecture overview

```
Generator (local machine)
  │
  ├─ bash generate_180day.sh   → gen-180day-flat/ (local TSDB blocks)
  └─ bash upload_to_s3.sh      → s3://SHARED_BUCKET/demo-right-sizing/
                                   manifest.json          ← written LAST (completion sentinel)
                                   <ULID>/index
                                   <ULID>/chunks/000001
                                   <ULID>/integrity.sha256  ← SHA-256 of index+chunks
                                   <ULID>/meta.json          ← written last per block

Shared S3 bucket (single source of truth, read-only for consumers via IAM)
  │
  ├─► Team A  →  bash load_from_s3.sh  →  their thanos bucket (MinIO or S3)
  ├─► Team B  →  bash load_from_s3.sh  →  their thanos bucket
  └─► Team C  →  bash load_from_s3.sh  →  their thanos bucket
```

Copy-on-consume is the correct model. For S3 consumers this is an S3-to-S3 copy into
their own bucket/prefix; MinIO consumers download and re-upload. In both cases:
- Direct store-gateway pointing at shared S3 creates an unsolvable compactor isolation
  problem (multiple compactors racing on the same bucket → HALT within hours).
- Pull-and-push makes each team's copy independent and disposable; their own compactor
  produces the full raw+5m+1h tier pyramid.
- The shared S3 is unreachable by any Thanos component (never wired to any
  thanos-object-storage secret); IAM prevents consumer writes.
- Each team's copy survives the shared S3 becoming unavailable after initial load.

### Lifecycle boundary: master versus hub copy

- A master prefix is immutable once `manifest.json` is present. No Thanos component is
  ever configured to use it.
- A hub copy is private and mutable. Thanos compaction, downsampling, deletion marks,
  Receive, and Rule output are expected to change it and expand its storage footprint.
- Copy into an empty hub prefix in two phases: all files except `meta.json` first, then
  all `*/meta.json`. Point the hub at the prefix only after both phases succeed.
- Never reuse one mutable prefix across hubs and never copy a compacted hub prefix back
  over a master.
- The 20/100/300-cluster categories are separate masters. Consumers copy only the
  category they need into a separately named hub prefix.

---

## manifest.json schema (formal contract — all scripts depend on this)

Written by upload_to_s3.sh as the final object, read by load_from_s3.sh and
validate_s3_source.sh. All fields are required.

```json
{
  "schema_version": 1,
  "generated_at":   1790208000,        // Unix epoch (seconds) when generation completed
  "expires_at":     1806048000,        // generated_at + retention_days * 86400
  "retention_days": 182,               // minimum retention tier on the hub; warn if blocks will
                                       // expire within 30d of consumer's load time
  "base_epoch":     1790208000,        // anchor epoch used by generate_180day.sh
  "min_data_epoch": 1788998400,        // minTime of the oldest block (seconds)
  "max_data_epoch": 1790208000,        // maxTime of the newest block (seconds)
  "profile":        "custom-continous-1-week-full",
  "weeks":          26,
  "num_namespaces": 100,
  "num_workloads":  10,
  "num_pods":       3,
  "series_per_block": 74418,          // right-sizing + filler series per block
  "rs_series_per_block": 74418,
  "filler_series_per_block": 0,       // NUM_EXTRA_METRICS x N + NUM_POD_METRICS x N x W x P + NUM_CLUSTER_METRICS
  "num_extra_metrics": 0,             // recommended filler: 0 / 20 / 154 ->
  "num_pod_metrics": 0,               //   60,154 filler, 134,572 series/block
  "num_cluster_metrics": 0,
  "blocks_per_cluster": 234,
  "expected_blocks": 4680,
  "clusters": ["ac-test-man-1", ..., "ac-test-man-20"],  // exact list, 20 entries
  "block_ulids": ["01M3...", ...],   // EXACT list of all 4680 expected ULIDs — used by
                                     // validate_s3_source.sh for exact-match verification
                                     // (count-only checks can be fooled by orphaned old ULIDs)
  "s3_prefix":  "demo-right-sizing", // S3 prefix under which all blocks live
  "s3_bucket":  "SHARED_BUCKET",     // for self-referential validation
  "block_sizes": {                   // per-ULID expected byte sizes for existence spot-check
    "01M3...": {"index": 1572864, "chunks_000001": 47185920},
    ...
    // NOTE: meta_json_sha256 is NOT included — meta.json is verified structurally by the
    // Python linter (level=1, cluster label, minTime<maxTime, source=blockgen) which is
    // more specific than a content hash for detecting the relevant failure modes.
    // Full content integrity is covered by sha256sum on index+chunks (integrity.sha256).
  }
}
```

---

## IAM model

| Principal | Permissions | Notes |
|---|---|---|
| Generator | s3:PutObject, s3:GetObject, s3:ListBucket, s3:DeleteObject | on `demo-right-sizing/*` |
| Consumer teams | s3:GetObject, s3:ListBucket | read-only; explicit DENY on Put+Delete |
| No shared key | Generator and consumer principals are distinct IAM users/roles | Consumer DENY must be explicit (not just omission) to survive SCP overrides |

Consumer credentials: a dedicated read-only IAM user per team, or a cross-account
role with ExternalId. Credentials distributed out-of-band (never in script files or
this repo). load_from_s3.sh accepts `SHARED_AWS_ACCESS_KEY_ID` and
`SHARED_AWS_SECRET_ACCESS_KEY` env vars for the S3 read credential — completely
separate from the cluster MinIO credentials it reads from the thanos-object-storage
secret.

---

## S3 bucket provisioning checklist (generator responsibility — one-time)

```bash
# Create bucket (generator runs once)
aws s3api create-bucket --bucket SHARED_BUCKET --region us-east-1
aws s3api put-bucket-versioning --bucket SHARED_BUCKET \
  --versioning-configuration Status=Disabled
aws s3api put-bucket-lifecycle-configuration --bucket SHARED_BUCKET \
  --lifecycle-configuration '{"Rules":[{"ID":"expire-demo","Status":"Enabled",
    "Filter":{"Prefix":"demo-right-sizing/"},
    "Expiration":{"Days":365}}]}'

# Attach bucket policy (substitute CONSUMER_ACCOUNT_ID):
# generator: full access on demo-right-sizing/*
# consumer:  GetObject + ListBucket; explicit Deny on Put+Delete

# Create consumer IAM user per team; attach read-only policy; distribute access key
# (never stored in this repo; distributed via team's secrets manager)
```

---

## Content integrity: SHA-256 checksums

Every block carries a sidecar file `<ULID>/integrity.sha256` in **GNU sha256sum format**
(bare 64-char hex + two spaces + filename — no `sha256:` prefix; that prefix is an
OpenSSL convention rejected by both GNU sha256sum and BSD shasum):

```
<64-char-hex>  index
<64-char-hex>  chunks/000001
```

Generated by `upload_to_s3.sh`:
- Linux:  `sha256sum index chunks/000001 > integrity.sha256`
- macOS:  `shasum -a 256 index chunks/000001 > integrity.sha256`

Verified by `load_from_s3.sh` (consumer, after downloading, before cluster upload):
```bash
# platform-aware check (required; sha256sum is not on macOS stock installs):
if command -v sha256sum &>/dev/null; then
  sha256sum -c integrity.sha256
elif command -v shasum &>/dev/null; then
  shasum -a 256 -c integrity.sha256
else
  echo "ABORT: no sha256 tool found"; exit 1
fi
```

**Generator-side limitation (documented):** `validate_s3_source.sh` runs a 5-block
existence+size spot-check (not a full hash re-download) because downloading 208 GB
just to certify is impractical. Consumer-side sha256sum (step 4 of `load_from_s3.sh`)
is therefore the ONLY complete per-block hash integrity gate. This is acceptable — the
generator controls the upload; the consumer independently verifies every block on
download before trusting it.

---

## Scripts

### 1. `upload_to_s3.sh` — generator uploads blocks to shared S3

**Safe regeneration sequence (atomic swap — prevents orphaned old ULIDs):**
1. Upload all new-generation blocks (data first per block, `meta.json` last per block,
   `integrity.sha256` alongside data files). Uses per-block xargs pattern (NOT a top-level
   `aws s3 sync` which cannot guarantee meta.json ordering).
2. Verify all listed ULIDs are fully present in S3 (`aws s3api head-object` for index +
   chunks/000001 + meta.json per block).
3. Upload new `manifest.json` (written LAST — completion sentinel; consumers never start
   loading before this appears).
4. Delete all ULID prefixes in S3 that are NOT in the new manifest's `block_ulids` list
   (cleanup of any orphaned blocks from prior generations). Uses `--delete` on a filtered
   list, not `aws s3 sync --delete` on the whole prefix (safer scope).

**Additional guards:**
- Validates local blocks (same Python linter as upload_180day_batched.sh) before touching S3
- Generator disk precheck (`df`) before starting upload
- Local `gen-180day.manifest.json` must exist (generation must have completed)
- EXPECTED_SERVER_SUBSTR check is not applicable (no cluster) — replaced by S3 bucket name
  guard (abort if bucket does not match expected)

### 2. `load_from_s3.sh` — consumer pulls blocks from S3 to their cluster

**Hard gates at startup (all abort on failure before any download or cluster mutation):**
1. Cluster context: `EXPECTED_SERVER_SUBSTR` enforced as hard ABORT
2. MCO retention: checks compactor StatefulSet args (then MCO CR fallback) — all three
   tiers must be ≥ `MIN_RETENTION_DAYS` (default 182). Inline from preflight_180day.sh.
3. Consumer MinIO/S3 capacity: free space ≥ dataset size × (1 + 3.8 + 1.1) × 1.2 — the
   consumer's compactor keeps 5m (~3.8x) and 1h (~1.1x) downsampled copies next to the raw blocks
4. Consumer LOCAL disk: `df` check — free space ≥ dataset size × 1.2 (download temp dir)
5. Concurrency lock: `mkdir /tmp/load_from_s3_${EXPECTED_SERVER_SUBSTR}.lock 2>/dev/null`
   (POSIX atomic mkdir — works on macOS and Linux; `flock` is Linux-only). Abort if mkdir
   fails (another instance running on this machine). Release via EXIT trap `rmdir`.
   ⚠️ Per-machine only: two operators on DIFFERENT machines targeting the same cluster
   are NOT protected. Announce in team channel before running; one operator per cluster at a time.
6. Compactor halt precheck: scan compactor pod logs for halt lines

**Main flow:**
1. Download `manifest.json` from S3 → save to `./gen-180day.manifest.json` (both
   validators use this path; this is the manifest distribution mechanism)
2. Check `expires_at` — warn if blocks will age out within 30d of now
3. Download all blocks listed in `block_ulids` (manifest-guided, not a prefix glob —
   prevents loading orphaned blocks not in the current manifest)
4. Per-block: platform-aware checksum (sha256sum on Linux, shasum -a 256 on macOS) —
   abort if any block fails. This is the ONLY complete content-integrity gate in the chain.
5. Validate downloaded blocks with Python linter (ULID, meta.json, level=1, cluster label,
   time range, non-zero index + chunks)
6. DELETE_CAP enforced (default 5000) — label-scoped delete of existing demo blocks from
   consumer MinIO before upload; abort if count exceeds cap
7. Upload to consumer MinIO (per-block two-phase: data first, meta.json last)
8. Validate consumer MinIO (list ULIDs, verify exact match to manifest's block_ulids,
   verify meta.json + label coverage) BEFORE store-gateway restart
9. Rolling store-gateway restart (one shard at a time, 60s readiness wait)
10. Write summary: cluster, block count, data window, expires_at

**RESUME=1:** skips steps 6 (no delete), reuses existing temp dir. Prevents redundant
~208 GB re-download after an interrupted upload-to-MinIO phase (step 7).
⚠️ RESUME=1 is safe ONLY when resuming an interrupted load of the CURRENT generation's
ULIDs. Do NOT use RESUME=1 when loading a NEW generation over a completed prior load —
omit RESUME so step 6 (label-scoped delete) runs first to remove old-generation blocks.
A sentinel file `${TMPDIR}/.step6_complete` is written after step 6; if absent when
RESUME=1 is requested, the script warns and proceeds without RESUME (step 6 will run).

**TEARDOWN=1:** label-scoped delete of demo blocks from consumer MinIO; cleans local
temp dir; does NOT touch shared S3.

**Cleanup:** `trap cleanup EXIT` removes temp dir on success; on failure prints path for
RESUME=1 re-entry.

### 3. `validate_s3_source.sh` — READ-ONLY certification of shared S3

Run by generator after upload; run by consumer BEFORE load (as a hard gate inlined
into load_from_s3.sh, not just a separate advisory script).

Checks:
1. manifest.json is readable and parses cleanly against the schema
2. ULIDs listed in `block_ulids` exactly match the set of ULID prefixes in S3 (no more,
   no fewer). S3 listing MUST be fully paginated (`aws s3api list-objects-v2` with
   `NextContinuationToken` loop, or `aws s3 ls` which paginates internally). A single
   un-paginated call silently truncates at 1000 items and produces a false PASS.
3. For each listed ULID: `index`, `chunks/000001`, `integrity.sha256`, `meta.json` all
   exist with non-zero Content-Length (`aws s3api head-object`). Parallelized via
   `xargs -P 50`; target runtime < 2 min for 18,720 calls at 50ms/call avg.
   DO NOT implement as a per-ULID bash loop calling jq once per entry — that is O(n²)
   for the manifest parse and would take tens of minutes. Use a single `jq` pass to emit
   all (ulid, filename) pairs, then feed to xargs.
4. Per-block meta.json: cluster label in expected set, minTime < maxTime, source=blockgen
5. Overlap check per (cluster, compaction.level) — same running-max logic as existing
   validate_compaction_180day.sh
6. Time coverage: all clusters span [min_data_epoch, max_data_epoch] without gaps >6h
7. Existence spot-check (5 random blocks): download their `integrity.sha256` sidecars and
   verify that the file sizes from `aws s3api head-object` for `index` and `chunks/000001`
   match the values in `manifest.json`'s `block_sizes` field. This is a SIZE check, not a
   hash check — the generator cannot do a 208 GB re-download. Consumer sha256sum is the
   only complete hash gate. Labelled "existence+size spot-check" in output, not "SHA-256".
8. `generated_at` freshness: warn if manifest is older than 90d (data nearing expiry)

Exit 0 = certified; exit 1 = data problem; exit 2 = environment/access error.

---

## Consumer onboarding (per team)

```bash
# Prerequisites: aws CLI configured with SHARED read-only credentials;
#                oc CLI logged into your hub cluster.

# 1. Read-only source certification (no cluster contact needed)
SHARED_BUCKET=<bucket> SHARED_PREFIX=demo-right-sizing \
  bash validate_s3_source.sh        # must exit 0 before proceeding

# 2. Load (mutates your cluster — runs locally, you execute it)
SHARED_BUCKET=<bucket> SHARED_PREFIX=demo-right-sizing \
  EXPECTED_SERVER_SUBSTR=<your-cluster-api-substring> \
  bash load_from_s3.sh

# If interrupted mid-upload (NOT mid-download, NOT a new generation):
RESUME=1 SHARED_BUCKET=<bucket> EXPECTED_SERVER_SUBSTR=<...> bash load_from_s3.sh

# 3. Certify query-side (read-only)
MANIFEST=./gen-180day.manifest.json bash validate_compaction_180day.sh
MANIFEST=./gen-180day.manifest.json DAYS=182 bash validate_load.sh

# 4. Teardown when done
TEARDOWN=1 EXPECTED_SERVER_SUBSTR=<...> bash load_from_s3.sh
```

---

## Certification chain

```
GENERATOR:
  G1. generate_180day.sh exits 0 (360 or 4680 blocks + manifest)
  G2. upload_to_s3.sh: data + integrity.sha256 → meta.json per block → manifest.json last
      → atomic swap (old ULIDs deleted after new manifest is live)
  G3. validate_s3_source.sh exits 0 (exact ULID match, checksums, overlaps, coverage)
  → S3 source is certified at generated_at timestamp

CONSUMER (per team):
  C1. validate_s3_source.sh exits 0 (consumer re-confirms S3 state; read-only)
  C2. load_from_s3.sh: retention/capacity/lock gates → download → sha256 per block →
      linter → label-scoped delete → upload (meta.json last) → MinIO pre-restart verify
  C3. Rolling store-gw restart
  C4. validate_compaction_180day.sh exits 0 (MANIFEST=./gen-180day.manifest.json)
  C5. validate_load.sh exits 0 (MANIFEST=./gen-180day.manifest.json — 7 checks including
      correct min/max epoch from manifest)
  → Consumer cluster is certified
```

---

## Data lifecycle

- **Regeneration**: generator re-runs `generate_180day.sh` + `upload_to_s3.sh`. The atomic
  swap sequence ensures consumers never see a mixed old/new generation. Consumers re-run
  `load_from_s3.sh` to pull the fresh set.
- **Expiry**: blocks age against hub retention (365d confirmed on jdj64). `expires_at` in
  manifest.json; `load_from_s3.sh` warns at load time if < 30d remain. Regenerate before
  expiry; consumers re-load.
- **Teardown**: `TEARDOWN=1 bash load_from_s3.sh` removes demo blocks (by cluster label)
  from consumer MinIO only. Shared S3 unchanged.

---

## Cost estimate (full 26-week × 20-cluster run)

| Item | Estimate |
|---|---|
| S3 storage (208 GB × $0.023/GB/mo) | ~$4.80/month |
| Consumer download egress (208 GB × $0.09/GB) | ~$19/team per load |
| Generator upload | free (data-in to S3) |
| MinIO PVC on consumer cluster | already provisioned |

These figures are for data without filler. With the recommended filler
(`NUM_POD_METRICS=20 NUM_CLUSTER_METRICS=154`) the dataset is ~0.5 TB, and each consumer
hub holds ~5–6× that after its compactor downsamples it.

---

## Review findings addressed (traceability)

| Finding | Reviewer | Fix |
|---|---|---|
| manifest.json schema unspecified | Arch-B2 | Formal schema with all fields including block_ulids, block_sizes, generated_at, expires_at |
| MCO retention missing from consumer | PE-B1 | Hard ABORT gate #2 in load_from_s3.sh startup |
| DELETE_CAP absent | PE-B2 | Gate #6, default 5000, same as upload_180day_batched.sh |
| No content checksums | QE-B1, PE-M4 | integrity.sha256 per block; verified at both hops |
| manifest not distributed to consumers | QE-B2 | load_from_s3.sh step 1 downloads manifest.json to ./gen-180day.manifest.json |
| Generator retry leaves orphaned ULIDs | QE-B3, PE-M1 | Atomic swap: upload new → write manifest → delete non-manifest ULIDs |
| Consumer local disk precheck | Arch-B1 | Gate #4 (df check) at startup |
| Regeneration atomic swap | PE-M1, Arch-M4 | Explicit 4-step sequence in upload_to_s3.sh |
| manifest lacks ULID list | QE-M6 | block_ulids field in schema |
| Two-phase ambiguous (aws s3 sync) | Arch-M4 | Explicitly specified as per-block xargs (NOT top-level sync) |
| validate_s3_source insufficient (no file existence) | QE-M4 | head-object check per file per block (step 3) |
| Re-load idempotency (orphaned blocks) | QE-M5 | Unconditional label-scoped delete before upload (gate #6) |
| IAM provisioning missing | Arch-M3, PE-M2 | Bucket provisioning checklist + credential separation section |
| Temp dir no cleanup + RESUME | Arch-M1, PE-M3 | trap cleanup EXIT; RESUME=1 reuses dir |
| Concurrency lock | PE-B3 | flock gate #5 |
| Block aging invisible | Arch-M2 | expires_at field; expiry warning in load_from_s3.sh step 2 |
| validate_s3_source advisory only | QE-M8, Arch-N1 | Inlined as hard gate at top of load_from_s3.sh |
| Rolling restart pod-name pattern | Arch-N3 | Note: parameterize via label selector in implementation |
