#!/bin/bash

# This workload is written for https://github.com/BU-DiSC/bods. To use this script, you should 
# first download and compile the "bods" project, and then put this script with the executable in the same
# directory. The seed of the generator will always be the time seed.
# This script accept 6 arguments:
# 1. The keys in each test file.
# 2. The value of K(0 <= K <= 50)
# 3. The value of L(0 <= L <= 50)
# 4. The output path(Where to store the output files)
# 5. The payload(integers that are greater than 0)
# 6. The number of files produced.

WORKLOAD_GENERATOR="sortedness_data_generator"
if [ ! -f "$WORKLOAD_GENERATOR" ]
then
	echo "Use \"make\" command before run the script, because the file \"sortedness_data_generator\" is needed"
	exit 1
fi

if [ $# -ne 7 ] 
then
	echo "The command should be: ./workload.sh {data size} {K value(less than 100)} {L value(less than 100) {output path} {payload(in bytes)} {number of output files} {random seed}"
	exit 1
fi

data_size=$1
k_val=$2
l_val=$3
if [[ $4 == */ ]]
then
    output_path="$4"
else
    output_path="${4}/"
fi
payload=$5
alpha=2.5
beta=2.9
test_file_num=$6
random_seed=$7

if [ ! -d "$output_path" ]
then
    $echo "mkdir $output_path"
	mkdir "$output_path"
fi

test_set_dir="${1}_test/"
if [ ! -d "$output_path$test_set_dir" ]
then
	mkdir "$output_path$test_set_dir"
fi

dst_prefix="$output_path$test_set_dir"
final_test_dir="${dst_prefix}k${k_val}_l${l_val}/"

if [ ! -d "$final_test_dir" ]
then
	mkdir "$final_test_dir"
fi

echo "Produce test datasets of $test_file_num files and each of them contains ${data_size} keys(k = ${k_val}, l = ${l_val}, payload = ${payload}) to ${output_path}"

for j in $( seq 1 $test_file_num )
do
		seed=$((random_seed-1+j))
        output_file="${final_test_dir}${data_size}_k${k_val}_l${l_val}_${seed}_p${payload}.txt"
        echo "Generating ${j}th file $output_file"
		./$WORKLOAD_GENERATOR -N $data_size -K $k_val -L $l_val -S $seed -o $output_file -a $alpha -b $beta -P $payload
        # sleep enough time to wait for workload_generator.o changing the random seed
		sleep 5s
        echo "-------------------------------------------------------------"
done


