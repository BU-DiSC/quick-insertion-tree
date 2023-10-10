#!/bin/bash

workloads=$(ls -v workloads/100_*)
trees=$(ls ./trees/*)
for tree in $trees; do
  for input in $workloads; do
    echo "$tree" "$input" --runs 5 --short 1000 --mid 1000 --long 1000 --reads 1
  done
done
