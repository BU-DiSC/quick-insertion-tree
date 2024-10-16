for tree in simple tail lil quit bptree; do
  for workload in 500_0_0 500_1_100 500_3_100 500_5_100 500_10_100 500_25_100 500_50_100 500_100_100; do
    ./trees/$tree ../workloads/$workload
  done
done
