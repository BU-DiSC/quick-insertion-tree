D=5
LS="1 3 5 10 25 50"
LS=100
./sortedness_data_generator -a 1 -b 1 -P 0 -S 1234 -N ${D}000000 -K 0 -L 0 -o workloads/${D}_0_0
for K in 1 3 5 10 25 50; do
  for L in $LS; do
    ./sortedness_data_generator -a 1 -b 1 -P 0 -S 1234 -N ${D}000000 -K $K -L $L -o workloads/${D}_${K}_${L}
  done
done
seq ${D}000000 | shuf | ./btio > workloads/${D}_100_100
