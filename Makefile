all:
	mkdir -p trees
	g++ -O3 -std=c++20 -I BP-Tree/btree_tests/ParallelTools -I BP-Tree/tlx-leafds -DNDEBUG bench-bptree-v2.cpp -o trees/bptree-v2
	g++ -O3 -std=c++20 -DOMCS_LOCK -DBTREE_OMCS_LEAF_ONLY -DOMCS_OP_READ -DOMCS_OFFSET -DOMCS_OFFSET_NUMA_QNODE -DBTREE_PAGE_SIZE=4096 -I optiql/index-benchmarks bench-optiql.cpp -o trees/optiql
	git -C quick-insertion-tree/ checkout concurrency-v2
	g++ -O3 -std=c++20 -I quick-insertion-tree/src/bptree -DINMEMORY bench-v2.cpp -o trees/simple-v2
	g++ -O3 -std=c++20 -I quick-insertion-tree/src/bptree -DTAIL_FAT -DINMEMORY bench-v2.cpp -o trees/tail-v2
	g++ -O3 -std=c++20 -I quick-insertion-tree/src/bptree -DLOL_FAT -DVARIABLE_SPLIT -DLOL_RESET -DINMEMORY bench-v2.cpp -o trees/quit-v2
	g++ -O3 -std=c++20 -I BP-Tree/btree_tests/ParallelTools -I BP-Tree/tlx-leafds -DNDEBUG bench-bptree.cpp -o trees/bptree
	git -C quick-insertion-tree/ checkout main
	g++ -O3 -I quick-insertion-tree/src/bptree -std=c++20 bench.cpp -DINMEMORY -o trees/simple
	g++ -O3 -I quick-insertion-tree/src/bptree -std=c++20 bench.cpp -DTAIL_FAT -DINMEMORY -o trees/tail
	g++ -O3 -I quick-insertion-tree/src/bptree -std=c++20 bench.cpp -DLIL_FAT -DINMEMORY -o trees/lil
	g++ -O3 -I quick-insertion-tree/src/bptree -std=c++20 bench.cpp -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -DLOL_RESET -DINMEMORY -o trees/quit

run:
	./bench.sh > results.csv
	./bench-v2.sh > results-v2.csv

plot:
	python plot.py
	python plot-v2.py
