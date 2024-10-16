/*
g++ -O3 -std=c++20 -I quick-insertion-tree/src/bptree -DINMEMORY bench-v2.cpp -o trees/simple-v2
g++ -O3 -std=c++20 -I quick-insertion-tree/src/bptree -DTAIL_FAT -DINMEMORY bench-v2.cpp -o trees/tail-v2
g++ -O3 -std=c++20 -I quick-insertion-tree/src/bptree -DLOL_FAT -DVARIABLE_SPLIT -DLOL_RESET -DINMEMORY bench-v2.cpp -o trees/quit-v2
*/
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
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

struct Ticket {
    std::atomic<size_t> _idx;
    const size_t _size;

    unsigned get() {
        unsigned idx = _idx++;
        return idx < _size ? idx : _size;
    }

    Ticket(size_t first, size_t size) : _idx(first), _size(size) {}

    explicit Ticket(size_t size) : Ticket(0, size) {}
};

unsigned hash(unsigned num) { return (num >> 1) + (num & 1) * 48; }

void insert_worker(tree_t &tree, const std::vector<key_type> &data, Ticket &line) {
    auto idx = line.get();
    const auto &size = line._size;
    while (idx < size) {
        const key_type &key = data[idx] + 1;
        tree.insert(key, 0);
        idx = line.get();
    }
}

void query_worker(tree_t &tree, const std::vector <key_type> &data, Ticket &line, std::atomic<unsigned> &counter) {
    auto idx = line.get();
    const auto &size = line._size;
    while (idx < size) {
        const key_type &key = data[idx] + 1;
        bool found = tree.contains(key);
        counter.fetch_add(!found, std::memory_order_relaxed);
        idx = line.get();
    }
}

int main(int argc, char **argv) {
    if (argc != 3) {
        std::cerr << "Usage: ./tree_analysis <num_threads> <input_file>" << std::endl;
        return -1;
    }
    std::cerr << argv[0] << ' ' << argv[1] << ' ' << argv[2] << std::endl;

    unsigned num_threads = atoi(argv[1]);
    auto file = argv[2];

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
"SIMPLE"
#endif
#endif
;
    std::cout << name << ',' << num_threads << ',' << fsPath.filename().c_str();
{
    Ticket line(data.size());
    auto start = std::chrono::high_resolution_clock::now();
    {
        std::vector<std::jthread> threads;

        for (unsigned i = 0; i < num_threads; ++i) {
            threads.emplace_back(insert_worker, std::ref(tree), std::ref(data), std::ref(line));

            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(hash(i), &cpuset);
            pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
        }
    }
    auto duration = std::chrono::high_resolution_clock::now() - start;
    std::cout << ',' << duration.count();
}
    std::shuffle(data.begin(), data.end(), std::default_random_engine(1234));
    size_t num_queries = data.size() / 100;
    std::atomic<unsigned> lost{};
{
    Ticket line(data.size());
    auto start = std::chrono::high_resolution_clock::now();
    {
        std::vector <std::jthread> threads;

        for (unsigned i = 0; i < num_threads; ++i) {
            threads.emplace_back(query_worker, std::ref(tree), std::ref(data), std::ref(line), std::ref(lost));

            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(hash(i), &cpuset);
            pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
        }
    }
    auto duration = std::chrono::high_resolution_clock::now() - start;
    std::cout << ',' << duration.count() << std::endl;
    if (lost) std::cerr << "Lost " << lost << " keys" << std::endl;
}
    return 0;
}
