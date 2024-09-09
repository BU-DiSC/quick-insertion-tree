#for tree in simple tail quit; do
for tree in quit; do
    ./run_large.sh ./trees/$tree
    cp results.csv large_$tree.csv
    truncate -s0 results.csv
done
