#!/bin/bash

# generate ranges parameters 
# Below we define ranges to pass to run_thanobench.sh 
# ex: (1,10), (11,21), ..., (290, 300)
generate_ranges() {
    local start=1
    local end=300
    local step=10
    local max=300
    
    while [ $end -le $max ]; do
        echo "$start,$end"
        start=$((end + 1))
        end=$((start + step - 1))
    done
}

# run the script in parallel
generate_ranges | while read range; do
    ./run_thanosbench.sh "$range" &
done

wait
