
MKDIR_P = mkdir -p
all: analysis
	$(MKDIR_P) tree_dat
	
analysis: betree.h basic_analysis.cpp
	g++ -g -std=c++11 betree.h dual_tree.h basic_analysis.cpp -o analysis.o -DTIMER -DBPLUS

clean: 
	$(RM) *.o
	$(RM) tree_dat/*
	rm -r tree_dat/