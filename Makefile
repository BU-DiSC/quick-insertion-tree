CC=g++
CFLAGS=-g -std=c++17
BASE_INDEX_FILE=-Isrc/bptree
BASE_INDEX_TARGET=src/tree_analysis.cpp
FILTER_FLAGS=-Isrc/bamboofilters -mavx2

all: vanilla outofplace inplace

vanilla: 
	$(CC) $(CFLAGS) $(BASE_INDEX_TARGET) $(BASE_INDEX_FILE) -o vanilla 

outofplace: 
	$(CC) $(CFLAGS) $(BASE_INDEX_TARGET) $(BASE_INDEX_FILE) $(FILTER_FLAGS) -DDUAL_FILTERS=1 -o outofplace

inplace:
	$(CC) $(CFLAGS) $(BASE_INDEX_TARGET) $(BASE_INDEX_FILE) $(FILTER_FLAGS) -DDUAL_FILTERS=2 -o inplace

clean: 
	rm -rf vanilla outofplace inplace