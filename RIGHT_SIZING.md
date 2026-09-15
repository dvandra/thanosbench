# Thanosbench for Producing Thanos Blocks Metrics Data

This guide provides step-by-step instructions for generating Thanos blocks metrics data using `thanosbench`. The example below demonstrates how to produce approximately one month of data in weekly increments.

## Prerequisites

- **OpenShift Cluster** with `multicluster-observability-operator` installed.
- `oc` CLI tool installed and configured for logging into the cluster.
- **S3 Bucket** to store the generated data, which will be used as the `thanos-object-storage` endpoint.
- **ACM Right-Sizing dashboards** for namespace, workload, and pod recommendation views.

## Steps

### 1. Generate Thanos Blocks

1. Clone the `thanosbench` repository:

    ```bash
    git clone https://github.com/dvandra/thanosbench/
    ```

    - Use the `master` branch (default).

2. Build `thanosbench`:

    ```bash
    make build
    ```
    If you modify the profile (e.g., changing the time ranges), you must rebuild `thanosbench` by running `make build` again to apply those changes.

3. Run the following script to generate Thanos blocks:

    ```bash
    ./run_thanosbench.sh
    ```

    **Note:** Profiles are defined in `pkg/blockgen/profiles.go`. The default profile is `custom-continous-1-week-workload-pod`, which generates one week of cluster, namespace, workload, and pod right-sizing metrics.

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

Each `acm_rs*`/`acm_rs_vm*` metric is emitted once per right-sizing **profile**
(`Max OverAll`, `P95`, `P99`) with `profile` as a per-series label — matching how
the recording rules label real data, so the dashboards' `$profile` dropdown is
populated. Because `profile` is now per-series, it must **not** be passed via
`--labels` (only `cluster`/`aggregation` are block-level external labels).
`kubevirt_*`/`extra_metric_*` filler carry no `profile` label.

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
parent). Generate blocks for it with:

```bash
./run_thanosbench_full.sh "1,3"   # inclusive cluster range
```

#### Environment variables (cardinality & values)

All profiles read these (unset/empty uses the default; a non-empty but invalid
value fails fast with an error):

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
| `NUM_EXTRA_METRICS` | `200` (`-full`: `0`) | all custom | synthetic `extra_metric_*` filler load |

To keep the process from running out of memory (blocks are built in memory before
being flushed), each profile refuses to plan more than ~5,000,000 series per
block and prints the projected series count to stderr. Lower the `NUM_*` values
if you hit the cap.

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
      key: config
      name: thanos-object-storage
    ```
6. Restart the Thanos components to apply the changes:

    ```bash
    kubectl get pods -n open-cluster-management-observability | grep thanos | awk '{print $1}' | xargs kubectl delete pod -n open-cluster-management-observability
    ```

### 4. Visualize Data in Grafana

1. Verify that the Thanos Compactor is receiving the Thanos blocks:

    ```bash
    kubectl port-forward observability-thanos-compact-0 -n open-cluster-management-observability 8080:10902
    ```

    - Open [https://localhost:8080](https://localhost:8080) to view the Thanos Compactor UI. You should see blocks for the dates defined in `thanosbench`.

2. Verify the metrics in the Thanos Querier pod:

    ```bash
    kubectl port-forward <observability-thanos-querier-pod> -n open-cluster-management-observability 9090:9090
    ```

    - You should see the metrics defined in the profile.

3. Access the Grafana UI:

    - Select the appropriate dashboard and filter the date range to match the generated blocks.
