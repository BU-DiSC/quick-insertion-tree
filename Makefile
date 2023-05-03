CXXFLAGS=-Isrc/bptree -std=c++17
TARGET=src/tree_analysis.cpp
PROFILE=-O2 -DBENCHMARK

all: clean simple lil lol fast

simple:
	$(CXX) $(CXXFLAGS) -g $(TARGET) -o $@

lil:
	$(CXX) $(CXXFLAGS) -DLIL_FAT -g $(TARGET) -o $@

lol:
	$(CXX) $(CXXFLAGS) -DLOL_FAT -g $(TARGET) -o $@

fast:
	$(CXX) $(CXXFLAGS) -DLIL_FAT -DLOL_FAT -g $(TARGET) -o $@

clean:
	rm -rf simple lil lol fast
