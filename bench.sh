for tree in simple tail quit; do
    ./run.sh ./trees/$tree
    cp results.csv $tree.csv
    truncate -s0 results.csv
done
