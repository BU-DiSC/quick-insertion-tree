CXXFLAGS=-Isrc/bptree -std=c++17 -DINMEMORY
TARGET=src/tree_analysis.cpp

all: clean simple tail lil lol lollipop

simple:
	$(CXX) $(CXXFLAGS) -g $(TARGET) -o $@

tail:
	$(CXX) $(CXXFLAGS) -DTAIL_FAT -g $(TARGET) -o $@

lil:
	$(CXX) $(CXXFLAGS) -DLIL_FAT -g $(TARGET) -o $@

lol:
	$(CXX) $(CXXFLAGS) -DLOL_FAT -g $(TARGET) -o $@

lollipop:
	$(CXX) $(CXXFLAGS) -DLIL_FAT -DLOL_FAT -DTAIL_FAT -g $(TARGET) -o $@

clean:
	rm -rf simple tail lil lol lollipop
