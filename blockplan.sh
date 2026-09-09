#!/bin/bash

# Generate Thanos blocks for each namespace
  ./thanosbench block plan -p cc-1w-small-rs \
    --labels "namespace=\"bench-1\"" \
    --labels "instance=\"bench\"" \
    --labels "cluster=\"ac-test-man-1\"" \
    --labels "container=\"bench\"" \
    --labels "resource=\"cpu\"" \
    --labels "clusterType=\"bench\"" \
    --labels "mode=\"idle\"" \
    --labels "profile=\"Max OverAll\"" \
    --max-time 2024-06-14T00:00:00Z \
    > smcpu.yaml