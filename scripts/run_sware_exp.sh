#!/bin/bash
N=$1
TRIALS=$2
# echo $TRIALS
DATE=$(date +"%Y-%m-%d")
RESULT_DIR="../results/$DATE"
mkdir -p $RESULT_DIR

TIME=$(date +"%H-%M")
RES_FILE="${RESULT_DIR}/N_${N}_${DATE}_${TIME}.csv"
echo "Writing results to: ${RES_FILE}"
workloads=$(ls -v workloads/${N}_*)
trees=("../build/sabtree")
name="sabtree"

for tree in ${trees[@]}; do
    echo "Running $tree"
    for input in $workloads; do
        for ((i = 1; i <= $TRIALS; i++)); do
            $tree $name $RES_FILE $input
        done
        #echo "Running $tree"
    done
done
