/*
g++ -O3 -std=c++20 -I BP-Tree/btree_tests/ParallelTools -I BP-Tree/tlx-leafds -DNDEBUG bench-bptree.cpp -o trees/bptree
*/
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iostream>
#include <random>

#include <container/btree_map.hpp>

using key_type = uint64_t;
using value_type = uint64_t;
using tree_t = tlx::btree_map<key_type, value_type>;

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

    tree_t tree;
const char *name =
        "BPTREE";
    std::cout << name << ',' << fsPath.filename().c_str();
{
    auto start = std::chrono::high_resolution_clock::now();
    for (const auto &key : data) {
        tree.insert({key + 1, 0});
    }
    auto duration = std::chrono::high_resolution_clock::now() - start;
    std::cout << ',' << duration.count();
}
{
    std::shuffle(data.begin(), data.end(), std::default_random_engine(1234));
    size_t num_queries = data.size() / 100;
    size_t lost = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < num_queries; i++)
    {
        const key_type &key = data[i] + 1;
        lost += !tree.exists(key);
    }
    auto duration = std::chrono::high_resolution_clock::now() - start;
    std::cout  << ',' << duration.count() << std::endl;
    if (lost) std::cerr << "Lost " << lost << " keys" << std::endl;
}
    return 0;
}
