CXXFLAGS=-Isrc/bptree -std=c++20 -Wall -Wextra
FLAGS=-DINMEMORY
TARGET=src/tree_analysis.cpp
EXE_DIR=trees

all: clean simple tail lol lol_r lol_v lol_vr quit

simple:
	$(CXX) $(CXXFLAGS) $(TARGET) $(FLAGS) -o $(EXE_DIR)/simple
tail:
	$(CXX) $(CXXFLAGS) $(TARGET) $(FLAGS) -DTAIL_FAT -o $(EXE_DIR)/tail
lol:
	$(CXX) $(CXXFLAGS) $(TARGET) $(FLAGS) -DLOL_FAT -o $(EXE_DIR)/lol
lol_r:
	$(CXX) $(CXXFLAGS) $(TARGET) $(FLAGS) -DLOL_FAT -DLOL_RESET -o $(EXE_DIR)/lol_r
lol_v:
	$(CXX) $(CXXFLAGS) $(TARGET) $(FLAGS) -DLOL_FAT -DVARIABLE_SPLIT -o $(EXE_DIR)/lol_v
lol_vr:
	$(CXX) $(CXXFLAGS) $(TARGET) $(FLAGS) -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -o $(EXE_DIR)/lol_vr
quit:
	$(CXX) $(CXXFLAGS) $(TARGET) $(FLAGS) -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -DLOL_RESET -o $(EXE_DIR)/quit

all3: clean simple3 tail3 lol3 lol_r3 lol_v3 lol_vr3 quit3

simple3:
	$(CXX) -O3 $(CXXFLAGS) $(TARGET) $(FLAGS) -o $(EXE_DIR)/simple
tail3:
	$(CXX) -O3 $(CXXFLAGS) $(TARGET) $(FLAGS) -DTAIL_FAT -o $(EXE_DIR)/tail
lol3:
	$(CXX) -O3 $(CXXFLAGS) $(TARGET) $(FLAGS) -DLOL_FAT -o $(EXE_DIR)/lol
lol_r3:
	$(CXX) -O3 $(CXXFLAGS) $(TARGET) $(FLAGS) -DLOL_FAT -DLOL_RESET -o $(EXE_DIR)/lol_r
lol_v3:
	$(CXX) -O3 $(CXXFLAGS) $(TARGET) $(FLAGS) -DLOL_FAT -DVARIABLE_SPLIT -o $(EXE_DIR)/lol_v
lol_vr3:
	$(CXX) -O3 $(CXXFLAGS) $(TARGET) $(FLAGS) -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -o $(EXE_DIR)/lol_vr
quit3:
	$(CXX) -O3 $(CXXFLAGS) $(TARGET) $(FLAGS) -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -DLOL_RESET -o $(EXE_DIR)/quit

clean:
	mkdir -p $(EXE_DIR)
	rm -f $(EXE_DIR)/*
