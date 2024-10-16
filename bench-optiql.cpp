/*
g++ -O3 -std=c++20 -DOMCS_LOCK -DBTREE_OMCS_LEAF_ONLY -DOMCS_OP_READ -DOMCS_OFFSET -DOMCS_OFFSET_NUMA_QNODE -DBTREE_PAGE_SIZE=4096 -I optiql/index-benchmarks bench-optiql.cpp -o trees/optiql
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

#include "indexes/BTreeOLC/BTreeOLC.h"

using key_type = uint64_t;
using value_type = uint64_t;
using tree_t = btreeolc::BTreeOLC<uint64_t, uint64_t>;

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
    std::atomic <size_t> _idx;
    const size_t _size;

    unsigned get() {
        unsigned idx = _idx++;
        return idx < _size ? idx : _size;
    }

    Ticket(size_t first, size_t size) : _idx(first), _size(size) {}

    explicit Ticket(size_t size) : Ticket(0, size) {}
};

unsigned hash(unsigned num) { return (num >> 1) + (num & 1) * 48; }

void insert_worker(tree_t &tree, const std::vector <key_type> &data, Ticket &line) {
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
        value_type value;
        bool found = tree.lookup(key, value);
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

    tree_t tree;
    const char *name =
"OPTIQL";
    std::cout << name << ',' << num_threads << ',' << fsPath.filename().c_str();
{
    Ticket line(data.size());
    auto start = std::chrono::high_resolution_clock::now();
    {
        std::vector <std::jthread> threads;

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
    std::atomic<unsigned> lost{};
    std::shuffle(data.begin(), data.end(), std::default_random_engine(1234));
    size_t num_queries = data.size() / 100;
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
