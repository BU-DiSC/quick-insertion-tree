#!/bin/bash

workloads=$(ls -v workloads/100_*)
for tree in simple lil lol lollipop; do
  for input in $workloads; do
    echo ./$tree "$input"
  done
done
