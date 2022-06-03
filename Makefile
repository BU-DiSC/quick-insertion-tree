
MKDIR_P = mkdir -p
all: bptree_basic_analysis bptree_mixed_analysis dual_basic_analysis dual_mixed_analysis
	$(MKDIR_P) tree_dat
	
bptree_basic_analysis: betree.h bptree_basic_analysis.cpp
	g++ -g -std=c++11 betree.h dual_tree.h bptree_basic_analysis.cpp -o bptree_basic_analysis -DTIMER -DBPLUS

bptree_mixed_analysis: betree.h bptree_mixed_analysis.cpp
	g++ -g -std=c++11 betree.h dual_tree.h bptree_mixed_analysis.cpp -o bptree_mixed_analysis -DTIMER -DBPLUS

dual_basic_analysis: betree.h dual_basic_analysis.cpp
	g++ -g -std=c++11 betree.h dual_tree.h dual_basic_analysis.cpp -o dual_basic_analysis -DTIMER -DBPLUS

dual_mixed_analysis: betree.h dual_mixed_analysis.cpp
	g++ -g -std=c++11 betree.h dual_tree.h dual_mixed_analysis.cpp -o dual_mixed_analysis -DTIMER -DBPLUS


clean: 
	$(RM) *.o
	$(RM) tree_dat/*
	rm bptree_basic_analysis
	rm bptree_mixed_analysis
	rm dual_basic_analysis
	rm dual_mixed_analysis
	rm -r tree_dat/