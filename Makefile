CXXFLAGS=-Isrc/bptree -std=c++17 -DINMEMORY -g
TARGET=src/tree_analysis.cpp

all: clean simple tail lil lol lollipop

simple:
	$(CXX) $(CXXFLAGS) $(TARGET) -o $@

tail:
	$(CXX) $(CXXFLAGS) -DTAIL_FAT $(TARGET) -o $@

lil:
	$(CXX) $(CXXFLAGS) -DLIL_FAT $(TARGET) -o $@

lol:
	$(CXX) $(CXXFLAGS) -DLOL_FAT -DREDISTRIBUTE -DVARIABLE_SPLIT $(TARGET) -o $@

lollipop:
	$(CXX) $(CXXFLAGS) -DLIL_FAT -DLOL_FAT -DTAIL_FAT -DREDISTRIBUTE -DVARIABLE_SPLIT $(TARGET) -o $@

clean:
	rm -rf simple tail lil lol lollipop
