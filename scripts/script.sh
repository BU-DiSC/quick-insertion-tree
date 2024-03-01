#!/bin/bash

workloads=$(ls -v ../workloads/5_*)
trees=$(ls ./trees/*)
for tree in $trees; do
  for input in $workloads; do
    echo "$tree" "$input"
  done
done
