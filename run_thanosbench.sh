#!/bin/bash

# Variables
range="$1"

# Parse the range
IFS=',' read -r start end <<< "$range"
start=$(printf "%d" "$start")
end=$(printf "%d" "$end")

# Constants
NUM_NAMESPACES=40  # has to be 50
NUM_NAMES=100  # has to be 200
MAX_TIMES=("2024-12-07T00:00:00Z" "2024-12-14T00:00:00Z" "2024-12-21T00:00:00Z" "2024-12-28T00:00:00Z" "2025-01-04T00:00:00Z" "2025-01-11T00:00:00Z" "2025-01-18T00:00:00Z" "2025-01-25T00:00:00Z" "2025-02-01T00:00:00Z" "2025-02-08T00:00:00Z" "2025-02-15T00:00:00Z" "2025-02-22T00:00:00Z" "2025-03-01T00:00:00Z" "2025-03-08T00:00:00Z" "2025-03-15T00:00:00Z" "2025-03-22T00:00:00Z")
# Function to generate a random number in the given range
random_in_range() {
  local min=$1
  local max=$2
  echo $(awk -v min=$min -v max=$max 'BEGIN{srand(); print min + rand() * (max - min)}')
}

# Loop through clusters and namespaces to generate blocks
for ((cluster = start; cluster <= end; cluster++)); do
    for i in "${!MAX_TIMES[@]}"; do
      PROFILE="custom-continous-1-week-vm"
      MAX_TIME=${MAX_TIMES[$i]}
      
      MIN_GAUGE=$(random_in_range 2.1 4.6)
      MAX_GAUGE=$(random_in_range 10.6 19.8)
      
      # Output directory based on weeks
      OUTPUT_DIR="./new-run/cluster-${cluster}"
      
      mkdir -p "$OUTPUT_DIR"
      
      # Generate the block plan and blocks
      NUM_NAMESPACES=$NUM_NAMESPACES NUM_NAMES=$NUM_NAMES MIN_GAUGE=$MIN_GAUGE MAX_GAUGE=$MAX_GAUGE ./thanosbench block plan -p "$PROFILE" \
        --labels "instance=\"bench\"" \
        --labels "cluster=\"ac-test-man-${cluster}\"" \
        --labels "container=\"bench\"" \
        --labels "resource=\"cpu\"" \
        --labels "clusterType=\"bench\"" \
        --labels "mode=\"idle\"" \
        --labels "profile=\"Max OverAll\"" \
        --max-time "$MAX_TIME" \
        | ./thanosbench block gen --output.dir "$OUTPUT_DIR" --workers 20
    done
done

echo "Block generation completed for clusters from $start to $end for $NUM_NAMESPACES namespaces and $NUM_NAMES VMs."
