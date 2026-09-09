#!/bin/bash

BUCKET_URL="s3://oia-rs/<bucket-suffix>"

for cluster in <path_to_folder>/cluster-*; do
  if [ -d "$cluster" ]; then
    echo "Copying contents of $cluster to $BUCKET_URL"
    aws s3 cp "$cluster/" "$BUCKET_URL" --recursive
  fi
done