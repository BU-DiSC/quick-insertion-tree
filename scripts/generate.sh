EXEC=$1
D=$2
# $EXEC -a 1 -b 1 -P 0 -S 1234 -N ${D}000000 -K 0 -L 0 -o workloads/${D}_0_0
$EXEC -a 1 -b 1 -P 0 -S 1234 -N ${D}000000 -K 0 -L 0 --output_file workloads/${D}_0_0 --domain ${D}000000 --window 1 --start 0 --fixed --binary
for K in 1 3 5 10 25 50; do
    L=100
    # for L in 1 3 5 10 25 50; do
    # $EXEC -a 1 -b 1 -P 0 -S 1234 -N ${D}000000 -K $K -L $L -o workloads/${D}_${K}_${L}
    $EXEC -a 1 -b 1 -P 0 -S 1234 -N ${D}000000 -K $K -L $L --output_file workloads/${D}_${K}_${L} --domain ${D}000000 --window 1 --start 0 --fixed --binary
    # done
done
seq ${D}000000 | shuf | ./btio >workloads/${D}_100_100
