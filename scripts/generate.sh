D=5
for K in 0 1 5 10 25 50 100; do
  for L in 0 1 5 10 25 50 100; do
    ../bods/sortedness_data_generator -a 1 -b 1 -P 0 -S 1234 -N ${D}000000 -K $K -L $L -o workloads/${D}_${K}_${L}
  done
done

