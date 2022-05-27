
MKDIR_P = mkdir -p
all: analysis
	$(MKDIR_P) tree_dat
	

simple_analysis: betree.h analysis.cpp -DBPLUS
	g++ -g -std=c++11 betree.h analysis.cpp -o analysis.o 

analysis: betree.h analysis.cpp
	g++ -g -std=c++11 betree.h analysis.cpp -o analysis.o -DTIMER -DBPLUS

clean: 
	$(RM) *.o
	$(RM) tree_dat/*
	rm -r tree_dat/