for tree in optiql simple-v2 tail-v2 quit-v2; do
 for config in 1 2 4 8 16 32 48; do
   for workload in 500_0_0 500_5_100 500_25_100 500_100_100; do
     ./trees/$tree $config ../workloads/$workload
   done
 done
done
