# Thanosbench for Producing Thanos Blocks Metrics Data

This guide provides step-by-step instructions for generating Thanos blocks metrics data using `thanosbench`. The example below demonstrates how to produce approximately one month of data in weekly increments.

## Prerequisites

- **OpenShift Cluster** with `multicluster-observability-operator` installed.
- `oc` CLI tool installed and configured for logging into the cluster.
- **S3 Bucket** to store the generated data, which will be used as the `thanos-object-storage` endpoint.
- **ACM Right-Sizing Namespace Dashboard** defined in `thanos-metrics-analyzer` (Developer Preview).

## Steps

### 1. Generate Thanos Blocks

1. Clone the `thanosbench` repository:

    ```bash
    git clone https://github.com/Anxhela21/thanosbench/
    ```

    - Use the branch `anx/rs-data-gen`:

    ```bash
    git checkout anx/rs-data-gen
    ```

2. Build `thanosbench`:

    ```bash
    make build
    ```
    If you modify the profile (e.g., changing the time ranges), you must rebuild `thanosbench` by running `make build` again to apply those changes.

3. Run the following script to generate Thanos blocks:

    ```bash
    ./run_thanosbench.sh
    ```

    **Note:** The profile used for block generation is defined in `pkg/blockgen/profile.go`. By default, the profile `cc-1w-small-rs` is used, which generates one week of data. 


    You can adjust the following parameters directly in the `run_thanosbench.sh` script:
    - Number of clusters
    - Number of namespaces per cluster
    - Maximum time duration
    - `minGauge` and `maxGauge` values for simulating realistic metric data
    - Profile selection


    **Note**: For running in parallel, run `./run_parallel.sh` after adjusting the range settings as desired.

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
