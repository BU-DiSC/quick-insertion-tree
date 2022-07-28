
CXX=g++
CFLAGS=-g -std=c++11
DFLAGS=-DTIME -DBPLUS
SOURCES=betree.h dual_tree_knob.h dual_tree.h
MKDIR_P=mkdir -p
MKDATA_DIR = $(MKDIR_P) tree_dat

all: bptree_basic_analysis bptree_mixed_analysis dual_basic_analysis dual_mixed_analysis	
	
bptree_basic_analysis: $(SOURCES) bptree_basic_analysis.cpp
	$(CXX) $(CFLAGS) $^ -o $@ $(DFLAGS)
	$(MKDATA_DIR)	

bptree_mixed_analysis: $(SOURCES) bptree_mixed_analysis.cpp
	$(CXX) $(CFLAGS) $^ -o $@ $(DFLAGS)
	$(MKDATA_DIR)

dual_basic_analysis: $(SOURCES) dual_basic_analysis.cpp
	$(CXX) $(CFLAGS) $^ -o $@ $(DFLAGS)
	$(MKDATA_DIR)

dual_mixed_analysis: $(SOURCES) dual_mixed_analysis.cpp
	$(CXX) $(CFLAGS) $^ -o $@ $(DFLAGS)
	$(MKDATA_DIR)

basic_analysis: $(SOURCES) basic_analysis.cpp
	$(CXX) $(CFLAGS) $^ -o $@ $(DFLAGS)
	$(MKDATA_DIR)

clean: 
	$(RM) bptree_basic_analysis
	$(RM) bptree_mixed_analysis
	$(RM) dual_basic_analysis
	$(RM) dual_mixed_analysis
	$(RM) basic_analysis
	$(RM) -r tree_dat/
