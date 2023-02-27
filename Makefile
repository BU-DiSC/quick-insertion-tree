CXXFLAGS=-Isrc/bptree -Isrc/bamboofilters -std=c++17 -g
AVX2FLAGS=-mavx2
TARGET=src/tree_analysis.cpp

all: clean vanilla outofplace inplace

vanilla:
	$(CXX) $(CXXFLAGS) $(TARGET) -o $@

outofplace:
	$(CXX) -DDUAL_FILTERS=1 $(CXXFLAGS) $(AVX2FLAGS) $(TARGET) -o $@

inplace:
	$(CXX) -DDUAL_FILTERS=2 $(CXXFLAGS) $(AVX2FLAGS) $(TARGET) -o $@

clean:
	rm -rf vanilla outofplace inplace
