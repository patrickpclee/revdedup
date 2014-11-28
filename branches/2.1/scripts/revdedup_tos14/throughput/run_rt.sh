#!/bin/bash

#
# Script to run throughput evaluations on RevDedup
# using various bucket size, segment size on data-
# sets Single1, Group, and VM.
#


for ((i=0;i<5;i++)); do
./variable_synthetic_baseline.sh '7' 1 78 1
./variable_synthetic_bucket.sh 'GP' 16 20 2
./variable_synthetic_segment.sh 'GP' 16 20 2
done
