CXXFLAGS=-Isrc/bptree -std=c++17
TARGET=src/tree_analysis.cpp
EXE_DIR=trees

all: trees

trees: clean
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -o $(EXE_DIR)/1_simple
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DTAIL_FAT -o $(EXE_DIR)/2_tail
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLIL_FAT -o $(EXE_DIR)/3_lil
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -o $(EXE_DIR)/4_lol
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -o $(EXE_DIR)/5_lol_v
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -o $(EXE_DIR)/6_lol_vir
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -DLOL_RESET -o $(EXE_DIR)/7_quit

treesO3: clean
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -O3 -o $(EXE_DIR)/O3_1_simple
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DTAIL_FAT -O3 -o $(EXE_DIR)/O3_2_tail
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLIL_FAT -O3 -o $(EXE_DIR)/O3_3_lil
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -O3 -o $(EXE_DIR)/O3_4_nlol
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -O3 -o $(EXE_DIR)/O3_5_lol_v
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -O3 -o $(EXE_DIR)/O3_6_lol_vir
	$(CXX) $(CXXFLAGS) $(TARGET) -DINMEMORY -DBENCHMARK -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -DLOL_RESET=0 -O3 -o $(EXE_DIR)/O3_7_quit

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
