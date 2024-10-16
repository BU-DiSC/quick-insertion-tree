/*
g++ -O3 -I quick-insertion-tree/src/bptree -std=c++20 bench.cpp -DINMEMORY -o trees/simple
g++ -O3 -I quick-insertion-tree/src/bptree -std=c++20 bench.cpp -DTAIL_FAT -DINMEMORY -o trees/tail
g++ -O3 -I quick-insertion-tree/src/bptree -std=c++20 bench.cpp -DLIL_FAT -DINMEMORY -o trees/lil
g++ -O3 -I quick-insertion-tree/src/bptree -std=c++20 bench.cpp -DLOL_FAT -DVARIABLE_SPLIT -DREDISTRIBUTE -DLOL_RESET -DINMEMORY -o trees/quit
*/
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iostream>
#include <random>

#include "bp_tree.h"

using key_type = uint64_t;
using value_type = uint64_t;
using tree_t = bp_tree<key_type, value_type>;

std::vector<key_type> read_bin(const char *filename) {
    std::ifstream inputFile(filename, std::ios::binary);
    assert(inputFile.is_open());
    inputFile.seekg(0, std::ios::end);
    const std::streampos fileSize = inputFile.tellg();
    inputFile.seekg(0, std::ios::beg);
    std::vector<uint32_t> data(fileSize / sizeof(uint32_t));
    inputFile.read(reinterpret_cast<char *>(data.data()), fileSize);
    return {data.begin(), data.end()};
}

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: ./tree_analysis <input_file>" << std::endl;
        return -1;
    }
    std::cerr << argv[0] << ' ' << argv[1] << std::endl;

    auto file = argv[1];

    std::filesystem::path fsPath(file);
    auto data = read_bin(file);

    BlockManager manager("tree.dat", 4000000);
    tree_t tree(manager);
const char *name =
#ifdef QUIT_FAT
"QUIT"
#else
#ifdef TAIL_FAT
"TAIL"
#else
#ifdef LIL_FAT
"LIL"
#else
"SIMPLE"
#endif
#endif
#endif
;
    std::cout << name << ',' << fsPath.filename().c_str();
{
    auto start = std::chrono::high_resolution_clock::now();
    for (const auto &key : data) {
        tree.insert(key + 1, 0);
    }
    auto duration = std::chrono::high_resolution_clock::now() - start;
    std::cout << ',' << duration.count();
}
{
    std::shuffle(data.begin(), data.end(), std::default_random_engine(1234));
    size_t num_queries = data.size() / 100;
    auto start = std::chrono::high_resolution_clock::now();
    size_t lost = 0;
    for (size_t i = 0; i < num_queries; i++)
    {
        const key_type &key = data[i] + 1;
        lost += !tree.contains(key);
    }
    auto duration = std::chrono::high_resolution_clock::now() - start;
    std::cout  << ',' << duration.count() << std::endl;
    if (lost) std::cerr << "Lost " << lost << " keys" << std::endl;
}
    return 0;
}
