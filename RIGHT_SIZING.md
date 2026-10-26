# Thanosbench for Producing Thanos Blocks Metrics Data

This guide provides step-by-step instructions for generating Thanos blocks metrics data using `thanosbench`. The scripts below produce 16 weeks (about four months) of data in weekly blocks, ending today. For the 180-day perf/scale dataset, use the `demo-data/` pipeline instead (see `demo-data/README.md`).

## Prerequisites

- **OpenShift Cluster** with `multicluster-observability-operator` installed.
- `oc` CLI tool installed and configured for logging into the cluster.
- **S3 Bucket** to store the generated data, which will be used as the `thanos-object-storage` endpoint.
- **ACM Right-Sizing dashboards** for namespace, workload, and pod recommendation views.

## Steps

### 1. Generate Thanos Blocks

1. Clone the `thanosbench` repository:

    ```bash
    git clone https://github.com/rajusem/thanosbench.git
    cd thanosbench
    git checkout fix/right-sizing-profile-refs   # until it is merged to master
    ```

    - The right-sizing profiles described below are not on the upstream `dvandra/thanosbench` `master`.

2. Build `thanosbench`:

    ```bash
    make build
    ```
    If you modify the profile (e.g., changing the time ranges), you must rebuild `thanosbench` by running `make build` again to apply those changes.

    On recent macOS, a binary built with Go 1.21 or 1.22 aborts with `dyld: missing LC_UUID load command`. Build with the system linker instead:

    ```bash
    GOTOOLCHAIN=go1.22.12 go build -ldflags=-linkmode=external -o thanosbench ./cmd/thanosbench
    ```

3. Run the following script to generate Thanos blocks:

    ```bash
    ./run_thanosbench.sh
    ```

    **Note:** Profiles are defined in `pkg/blockgen/profiles.go`. The default profile is `custom-continous-1-week-workload-pod`, which generates one week of cluster, namespace, workload, and pod right-sizing metrics per run; the script runs it for 16 weeks per cluster.

    The script generates the `WEEKS` (default 16) weeks ending today (UTC); set `END_EPOCH` to pin the end date. Keep the data recent: blocks older than the hub's retention (MCO default 365d) are deleted by the compactor.

    Scale is controlled by environment variables (edit the defaults in the scripts, or override at run time):

    | Variable | Default | Meaning |
    |---|---|---|
    | `NUM_CLUSTERS` | `10` | Clusters to generate |
    | `NUM_NAMESPACES` | `100` | Namespaces per cluster |
    | `NUM_WORKLOADS` | `10` | Workloads per namespace |
    | `NUM_PODS` | `3` | Pods per workload |
    | `PROFILE` | `custom-continous-1-week-workload-pod` | Blockgen profile |
    | `CLUSTER_STEP` | `2` | Clusters per parallel worker (`run_parallel.sh` only) |

    Examples:

    ```bash
    # Default: 10 clusters × 100 namespaces × 10 workloads × 3 pods
    ./run_thanosbench.sh

    # Smaller smoke run
    NUM_CLUSTERS=2 NUM_NAMESPACES=5 NUM_WORKLOADS=2 NUM_PODS=2 ./run_thanosbench.sh

    # Parallel across 10 clusters
    NUM_CLUSTERS=10 NUM_NAMESPACES=100 NUM_WORKLOADS=10 NUM_PODS=3 ./run_parallel.sh
    ```

    Workload series are labeled with `namespace`, `workload`, and `workload_type`. Pod series also include a `pod` label. To generate VM data instead:

    ```bash
    PROFILE=custom-continous-1-week-vm NUM_NAMES=100 ./run_parallel.sh
    ```

#### Available profiles

The profiles are defined in `pkg/blockgen/profiles.go`:

| Profile | Aggregation levels | Per-series breakdown labels |
| --- | --- | --- |
| `custom-continous-1-week` | namespace, cluster (`acm_rs:*`) | level-aware (see below) |
| `custom-continous-1-week-vm` | namespace, cluster for both `acm_rs:*` and `acm_rs_vm:*` | level-aware (see below) |
| `custom-continous-1-week-workload-pod` | cluster, namespace, workload, pod (`acm_rs:*`) | upstream-compatible hierarchical profile |
| `custom-continous-1-week-full` | cluster, namespace, **workload**, **pod** (`acm_rs:*`) | hierarchical (see below) |
| `custom-continous-3-day-full` | cluster, namespace, **workload**, **pod** (`acm_rs:*`) | three-day hierarchical profile |

Except for `custom-continous-1-week-workload-pod`, each `acm_rs*`/`acm_rs_vm*`
metric is emitted once per right-sizing **profile** (`Max OverAll`, `P95`, `P99`)
with `profile` as a per-series label — matching how the recording rules label real
data, so the dashboards' `$profile` dropdown is populated. Because `profile` is
per-series, it must **not** be passed via `--labels`: an external `profile` label
overwrites the series' own and folds P95/P99 into one profile.
`kubevirt_*`/`extra_metric_*` filler carry no `profile` label.

`custom-continous-1-week-workload-pod` emits a single copy of each metric, without
`profile` and without `request_hard`. `run_thanosbench.sh` gives its blocks
`profile="Max OverAll"` as an external label (and no other profile), so only that
profile appears in the dashboards.

| Metric | Breakdown labels (per series) | Series/block |
| --- | --- | --- |
| `acm_rs:cluster:*`, `acm_rs_vm:cluster:*` | `profile` | `len(profiles)` |
| `acm_rs:namespace:*` | `namespace`, `profile` | `NUM_NAMESPACES × len(profiles)` |
| `acm_rs_vm:namespace:*` | `namespace`, `name` (per-VM), `profile` | `NUM_NAMESPACES × NUM_NAMES × len(profiles)` |
| `kubevirt_*`, `extra_metric_*` | `namespace`, `name` | `NUM_NAMESPACES × NUM_NAMES` |

The non-VM namespace level additionally emits `acm_rs:namespace:cpu_request_hard`
and `acm_rs:namespace:memory_request_hard` (the ResourceQuota ceiling the
namespaces dashboard's "hard limit" panels query). Each `*_recommendation`
series is derived from its `*_usage` sibling and scaled by `110/100`, so
`recommendation = usage × 1.10` holds exactly per series (not an independent
random draw).

The `custom-continous-1-week-full` profile emits the label set the real ACM
right-sizing recording rules / dashboards expect, with `cluster` and
`aggregation` supplied as block (external) labels via `--labels`. `profile` is
per-series (one copy of each metric per `Max OverAll`/`P95`/`P99`):

| Metric | Breakdown labels (per series) |
| --- | --- |
| `acm_rs:cluster:*` | `profile` |
| `acm_rs:namespace:*` | `namespace`, `profile` |
| `acm_rs:workload:*` | `namespace`, `workload`, `workload_type`, `profile` |
| `acm_rs:pod:*` | `namespace`, `pod`, `workload`, `workload_type`, `profile` |

Each level carries the six measures `cpu_request`, `cpu_usage`,
`cpu_recommendation`, `memory_request`, `memory_usage`, `memory_recommendation`;
the namespace level adds `cpu_request_hard`/`memory_request_hard`. Every metric
is emitted once per profile, `*_recommendation` tracks `*_usage × 1.10`, and pods
nest under workloads (a pod's `workload`/`workload_type` labels match its
parent). Pod names are unique across namespaces and clusters and shaped like
Deployment pods (`workload-3-<10-hex hash>-1`), so the `pod` label has one value
per pod as in a real fleet. Pods don't churn: the same pods exist for the whole
generated range, so long-range pod queries come out somewhat optimistic.
Generate blocks for it with:

```bash
./run_thanosbench_full.sh "1,3"   # inclusive cluster range
```

For Category 1 (20 managed clusters, 100 namespaces per cluster, 10 workloads
per namespace, and 10 pods per workload), run:

```bash
NUM_NAMESPACES=100 NUM_WORKLOADS=10 NUM_PODS=10 NUM_EXTRA_METRICS=0 ./run_thanosbench_full.sh "1,20"
```

This creates `20 × 100 × 10 × 10 = 200,000` pod identities across the 20
clusters. The generated blocks contain multiple right-sizing metric series per
pod (six measures and three right-sizing profiles), so the TSDB series count is
larger than 200,000 by design.

Set NUM_WEEKS=13 for approximately 90 days (13 weekly periods, or about 91
days):

```bash
NUM_NAMESPACES=100 NUM_WORKLOADS=10 NUM_PODS=10 NUM_EXTRA_METRICS=200 NUM_WEEKS=13 ./run_thanosbench_full.sh "1,20"
```

For a reusable one-command run, edit the variables at the top of
`run_category1.sh` and execute:

```bash
./run_category1.sh
```

The wrapper rebuilds `thanosbench` automatically before generating the blocks.

#### Environment variables (cardinality & values)

All profiles read these (unset/empty uses the default; a non-empty but invalid
value fails fast with an error). These are the profiles' own defaults, used when you
call `thanosbench block plan` directly; the `run_*.sh` scripts pass their own values
(see the table in step 3):

| Variable | Default | Applies to | Meaning |
| --- | --- | --- | --- |
| `MIN_GAUGE` | `2.0` | all custom | minimum gauge value (CPU / non-memory measures, in cores) |
| `MAX_GAUGE` | `8.0` | all custom | maximum gauge value (CPU / non-memory measures, in cores) |
| `MEM_MIN_GAUGE` | `536870912` (512 MiB) | all custom | minimum gauge value for `*memory*` measures, in **bytes** |
| `MEM_MAX_GAUGE` | `34359738368` (32 GiB) | all custom | maximum gauge value for `*memory*` measures, in **bytes** |
| `NUM_NAMESPACES` | `50` | all custom | namespaces per cluster |
| `NUM_NAMES` | `200` | `custom-continous-1-week[-vm]` | `name` dimension per namespace |
| `NUM_WORKLOADS` | `10` | `-full` | workloads per namespace |
| `NUM_PODS` | `20` | `-full` | pods per workload |
| `NUM_EXTRA_METRICS` | `200` (`-full`: `0`) | `custom-continous-1-week[-vm]`, `*-full` | synthetic `extra_metric_*` filler: per metric, one series per namespace (`*-full`) or per namespace × `NUM_NAMES` (`custom-continous-1-week[-vm]`) |
| `NUM_POD_METRICS` | `0` | `-full` | synthetic `extra_pod_metric_*{container,namespace,pod}` filler, one series per pod per metric |
| `NUM_CLUSTER_METRICS` | `0` | `-full` | synthetic `extra_cluster_metric_*` filler, one series per cluster per metric |
| `RS_SCRAPE_INTERVAL` | `15m` | `-full` | sample interval of the `acm_rs:*` series, as a Go duration that divides every block length of the profile (2h, 8h, 48h): `15m`, `30m`, `1h` or `2h`; the filler stays at 5m. The real ACM right-sizing recording rules evaluate once a day, so the default is ~96× denser than production and long-range right-sizing queries scan more points than they would on a real hub (a pessimistic test). Other values are refused: the generator writes a block's samples at multiples of the interval from the block start, so an interval that does not divide the block length puts the last sample past the block's end, makes blocks overlap and halts the compactor (observed with 45m, 1h30m and 24h on Thanos v0.42.4). A true 1-sample/day cadence needs a generator change first. |

The three filler knobs stand in for the other metrics a managed cluster sends to
the hub (sampled every 5m, like the metrics collector). Of the ~260 names in the
ACM allowlist a cluster sends ~100–135 (only those that exist on it), and on a
live hub (3 clusters collected by MCOA) ~15 per-pod names make ~90% of those
series: about 18 series per pod, 3 per namespace, and one or a few per cluster or
node for the other ~80–110 names. So `NUM_POD_METRICS=20 NUM_CLUSTER_METRICS=154`
reproduces a VM-heavy cluster's series count, its pod fan-out and its metric-name
count (for 100 namespaces × 10 workloads × 3 pods: 60,154 filler series next to
74,418 right-sizing series, ~174 metric names). The few per-namespace-only series
of a real cluster (~3 per namespace) are folded into the cluster-level count to
keep one knob; `NUM_EXTRA_METRICS` still works for the `-full` profiles if that
shape is wanted. The per-pod value is slightly high on purpose: it covers the
node-level series (~300 per node) that the model does not scale with nodes. Real
series carry more labels (uid, node, image, …), so the index is still somewhat
smaller than on a real hub.

To keep the process from running out of memory (blocks are built in memory before
being flushed), each profile refuses to plan more than ~5,000,000 series per
block and prints the projected series count to stderr. Lower the `NUM_*` values
if you hit the cap.

The cap is not a memory guard: memory grows with samples, and a filler series
has 3× the samples of a right-sizing series. With the recommended filler
(~135k series/block) one cluster-week peaked at ~8.2 GB RSS (`block plan`
3.6 GB + `block gen` 4.6 GB, which run together) and took ~90 s and ~0.92 GB of
disk on a 10-core Mac. Memory grows roughly in proportion to `NUM_POD_METRICS`.

On the hub, the compactor keeps downsampled copies next to the raw blocks. With
samples every 5–15 minutes the 5m copy is ~3.8× the raw size (measured with
Thanos v0.42.4) and the 1h copy ~0.85–1.1×, so object storage ends up holding
about 5–6× the uploaded size.

### 2. Store Data Blocks in S3

1. Ensure the S3 bucket directory is cleared of old data:

    ```bash
    aws s3 rm s3://<your-bucket>/<your-sub-folder> --recursive
    ```

2. Copy the newly generated data into the S3 bucket using the copy_to_s3.sh script:

    ```
    ./copy_to_s3.sh
    ````
    **Note**: add the script inside same folder where data is stored.

### 3. Replace Thanos Store Instance with S3 Bucket

1. Save the following configuration as `thanos.yaml`:

    ```yaml
    type: s3
    config:
      bucket: <your-bucket>
      endpoint: s3.amazonaws.com
      access_key: <your-access-key>
      secret_key: <your-secret-key>
      region: us-west-2
      signature_version2: false
    prefix: <your-subfolder>
    ```

2. Base64 encode the `thanos.yaml` file:

    ```bash
    openssl base64 -in thanos.yaml -out encoded_thanos.txt
    ```

3. Copy the encoded content into the `thanos.yaml` field of the following Secret definition and save it as `thanos-object-storage-secret.yaml`:

    ```yaml
    apiVersion: v1
    data:
      thanos.yaml: <encoded-file-content>
    kind: Secret
    metadata:
      name: thanos-object-storage
      namespace: open-cluster-management-observability
    type: Opaque
    ```

4. Replace the existing `thanos-object-storage` Secret with the new one:

    ```bash
    oc delete secret thanos-object-storage -n open-cluster-management-observability
    oc apply -f thanos-object-storage-secret.yaml
    ```

5. Ensure that the MultiClusterObservability CR has the following values under the metricObjectStorage:
    ```
    metricObjectStorage:
      key: thanos.yaml
      name: thanos-object-storage
    ```

    Verify with:

    ```bash
    oc get multiclusterobservability observability \
      -o jsonpath='key: {.spec.storageConfig.metricObjectStorage.key}{"\n"}name: {.spec.storageConfig.metricObjectStorage.name}{"\n"}'
    ```

    > **Important:** The `key` value must match the data key inside the Secret.
    > The Secret above uses `thanos.yaml` as the data key, so set `key: thanos.yaml`
    > in the CR. If the CR uses `key: config`, rename the data key in the Secret to
    > `config` accordingly.

6. Restart the Thanos components to apply the changes:

    ```bash
    kubectl get pods -n open-cluster-management-observability | grep thanos | awk '{print $1}' | xargs kubectl delete pod -n open-cluster-management-observability
    ```

### 4. Verify the Thanos Stack

1. Verify that the Thanos Compactor is receiving the Thanos blocks:

    ```bash
    oc port-forward observability-thanos-compact-0 -n open-cluster-management-observability 8080:10902
    ```

    - Open [https://localhost:8080](https://localhost:8080) to view the Thanos Compactor UI. You should see blocks for the dates defined in `thanosbench`.

2. Verify the metrics in the Thanos Querier pod:

    ```bash
    oc port-forward -n open-cluster-management-observability \
      $(oc get pods -n open-cluster-management-observability -l app.kubernetes.io/name=thanos-query -o jsonpath='{.items[0].metadata.name}') \
      9090:9090
    ```

    - Open [http://localhost:9090](http://localhost:9090) and query a metric like `acm_rs:cluster:cpu_usage` to confirm data is present.

### 5. Visualize Data in Perses

The ACM observability operator already runs a Thanos Querier in
`open-cluster-management-observability`. Perses connects to it as a standard
Prometheus-compatible datasource.

#### 5a. Deploy Perses on the cluster

```bash
oc new-project perses
```

Create the Perses deployment:

```bash
cat <<'EOF' | oc apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: perses
  namespace: perses
  labels:
    app: perses
spec:
  replicas: 1
  selector:
    matchLabels:
      app: perses
  template:
    metadata:
      labels:
        app: perses
    spec:
      containers:
        - name: perses
          image: persesdev/perses:latest
          ports:
            - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: perses
  namespace: perses
spec:
  selector:
    app: perses
  ports:
    - port: 8080
      targetPort: 8080
EOF
```

Expose Perses via an OpenShift Route:

```bash
oc -n perses create route edge perses \
  --service=perses --port=8080 --insecure-policy=Redirect
```

Get the Perses URL:

```bash
PERSES_URL=$(oc get route perses -n perses -o jsonpath='{.spec.host}')
echo "Perses UI: https://$PERSES_URL"
```

#### 5b. Allow Perses to reach the Thanos Querier (cross-namespace networking)

Perses needs to reach the Thanos Querier service in `open-cluster-management-observability`.
On OpenShift, cross-namespace service DNS works out of the box. The internal URL is:

```
http://observability-thanos-query.open-cluster-management-observability.svc.cluster.local:9090
```

Verify the service name:

```bash
oc get svc -n open-cluster-management-observability | grep thanos-query
```

Use the actual service name from the output. It is typically
`observability-thanos-query` (port `9090` for HTTP).

#### 5c. Create a Project and Datasource in Perses

**Option A — Via the Perses UI:**

1. Open `https://<PERSES_URL>`
2. Click **+ Create** → name the project, e.g. `acm-rightsizing`
3. Go to the **Datasources** tab → **Add Datasource**
4. Fill in:
   - **Name:** `thanos-acm`
   - **Set as default:** toggle on
   - **Source:** `Prometheus Datasource`
   - **HTTP Settings:** select `Proxy`
   - **URL:** `http://observability-thanos-query.open-cluster-management-observability.svc.cluster.local:9090`
5. Click **Add** to save

**Option B — Via the API:**

```bash
PERSES_URL=$(oc get route perses -n perses -o jsonpath='{.spec.host}')

# Create project
curl -sk -X PUT "https://$PERSES_URL/api/v1/projects/acm-rightsizing" \
  -H 'Content-Type: application/json' \
  -d '{"kind":"Project","metadata":{"name":"acm-rightsizing"},"spec":{}}'

# Create datasource (Proxy mode — recommended)
curl -sk -X PUT "https://$PERSES_URL/api/v1/projects/acm-rightsizing/datasources/thanos-acm" \
  -H 'Content-Type: application/json' \
  -d '{
    "kind": "Datasource",
    "metadata": {
      "name": "thanos-acm",
      "project": "acm-rightsizing"
    },
    "spec": {
      "default": true,
      "plugin": {
        "kind": "PrometheusDatasource",
        "spec": {
          "proxy": {
            "kind": "HTTPProxy",
            "spec": {
              "url": "http://observability-thanos-query.open-cluster-management-observability.svc.cluster.local:9090"
            }
          }
        }
      }
    }
  }'
```

> **Tip:** Use Proxy mode so the Perses server forwards requests to Thanos
> on your behalf. This avoids CORS issues that occur with Direct access mode
> when the browser cannot reach the Querier directly.

#### 5d. Create a Dashboard

**Via the UI:**

1. Go to **Dashboards** tab → **Add Dashboard** → name it e.g. `ACM Right-Sizing`
2. Add variables (dropdowns) for filtering:
   - **`$cluster`** — Prometheus Label Values, label `cluster`, matcher `acm_rs:cluster:cpu_usage`
   - **`$profile`** — Prometheus Label Values, label `profile`, matcher `acm_rs:cluster:cpu_usage`
   - **`$namespace`** — Prometheus Label Values, label `namespace`, matcher `acm_rs:namespace:cpu_usage{cluster="$cluster"}`
3. Add panels (Time Series Chart + Prometheus Time Series Query):

| Panel name | PromQL expression |
|---|---|
| Cluster CPU Usage | `acm_rs:cluster:cpu_usage{profile="$profile", cluster="$cluster"}` |
| Cluster CPU Recommendation | `acm_rs:cluster:cpu_recommendation{profile="$profile", cluster="$cluster"}` |
| Cluster Memory Usage | `acm_rs:cluster:memory_usage{profile="$profile", cluster="$cluster"}` |
| Namespace CPU Usage | `acm_rs:namespace:cpu_usage{profile="$profile", cluster="$cluster", namespace="$namespace"}` |
| Namespace Memory Recommendation | `acm_rs:namespace:memory_recommendation{profile="$profile", cluster="$cluster", namespace="$namespace"}` |
| Namespace CPU Quota Headroom | `acm_rs:namespace:cpu_request_hard{profile="$profile", cluster="$cluster"} - acm_rs:namespace:cpu_usage{profile="$profile", cluster="$cluster"}` |
| Top 10 Pods by Memory | `topk(10, acm_rs:pod:memory_usage{profile="$profile", cluster="$cluster"})` |
| Workload CPU Request vs Usage | `acm_rs:workload:cpu_request{profile="$profile"} / acm_rs:workload:cpu_usage{profile="$profile"}` |

4. Set the dashboard **time range** to match the dates of the generated blocks
5. Click **Save**

### (Optional) Visualize Data in Grafana

The Grafana UI bundled with the ACM observability stack can also be used:

1. Access the Grafana route provided by the observability operator.
2. Select the appropriate dashboard and filter the date range to match the generated blocks.
