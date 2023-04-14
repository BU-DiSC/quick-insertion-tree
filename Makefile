CXXFLAGS=-Isrc/bptree -std=c++17
TARGET=src/tree_analysis.cpp
PROFILE=-O2 -DBENCHMARK -fopenmp -shared-libgcc

all: clean vanilla profile

vanilla:
	$(CXX) $(CXXFLAGS) -g $(TARGET) -o $@

profile:
	$(CXX) $(CXXFLAGS) $(TARGET) $(PROFILE) -o $@

clean:
	rm -rf vanilla profile
