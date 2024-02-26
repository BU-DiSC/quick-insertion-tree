CXXFLAGS=-Isrc/bptree -std=c++17
TARGET=src/tree_analysis.cpp
EXE_DIR=trees

all: trees

trees: clean
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -o $(EXE_DIR)/simple
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DTAIL_FAT -o $(EXE_DIR)/tail
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLIL_FAT -o $(EXE_DIR)/lil
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -o $(EXE_DIR)/lol
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DLOL_RESET -o $(EXE_DIR)/lol_r
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -o $(EXE_DIR)/lol_v
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -o $(EXE_DIR)/lol_vr
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -DLOL_RESET -o $(EXE_DIR)/quit

treesO3: clean
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -O3 -o $(EXE_DIR)/O3_simple
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DTAIL_FAT -O3 -o $(EXE_DIR)/O3_tail
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLIL_FAT -O3 -o $(EXE_DIR)/O3_lil
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -O3 -o $(EXE_DIR)/O3_lol
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DLOL_RESET -O3 -o $(EXE_DIR)/O3_lol_r
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -O3 -o $(EXE_DIR)/O3_lol_v
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -O3 -o $(EXE_DIR)/O3_lol_vr
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -DLOL_RESET -O3 -o $(EXE_DIR)/O3_quit

simple:
	$(CXX) $(CXXFLAGS) $(TARGET) -o $(EXE_DIR)/$@

tail:
	$(CXX) $(CXXFLAGS) -DTAIL_FAT $(TARGET) -o $(EXE_DIR)/$@

lil:
	$(CXX) $(CXXFLAGS) -DLIL_FAT $(TARGET) -o $(EXE_DIR)/$@

plainlol:
	$(CXX) $(CXXFLAGS) -DLOL_FAT $(TARGET) -o $(EXE_DIR)/$@

lol:
	$(CXX) $(CXXFLAGS) -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -DLOL_RESET $(TARGET) -o $(EXE_DIR)/$@

clean:
	mkdir -p $(EXE_DIR)
	rm -f $(EXE_DIR)/*
