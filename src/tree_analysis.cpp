#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>

// #include "bptree/bp_tree.h"
#include "bench_bptree.h"
#include "bptree/config.h"
#include "index_bench.h"

using key_type = unsigned;
using value_type = unsigned;

std::vector<key_type> read_file(const char *filename) {
    std::vector<key_type> data;
    std::string line;
    std::ifstream ifs(filename);
    while (std::getline(ifs, line)) {
        key_type key = std::stoul(line);
        data.push_back(key);
    }
    return data;
}

std::vector<key_type> read_bin(const char *filename) {
    std::ifstream inputFile(filename, std::ios::binary);
    inputFile.seekg(0, std::ios::end);
    const std::streampos fileSize = inputFile.tellg();
    inputFile.seekg(0, std::ios::beg);
    std::vector<key_type> data(fileSize / sizeof(key_type));
    inputFile.read(reinterpret_cast<char *>(data.data()), fileSize);
    return data;
}

// bp_tree<key_type, value_type> &tree,
void workload(index_bench::BPTreeIndex<key_type, value_type> &tree,
              const std::vector<key_type> &data, const Config &conf,
              std::ofstream &results, const key_type &offset) {
    const unsigned num_inserts = data.size();
    const unsigned raw_queries = conf.raw_read_perc / 100.0 * num_inserts;
    const unsigned raw_writes = conf.raw_write_perc / 100.0 * num_inserts;
    const unsigned mixed_size = conf.mix_load_perc / 100.0 * num_inserts;
    // mixed reads is a percentage of the mixed size. E.g., if we are going to
    // do 20% of writes (from the total workload) in the mixed r-w phase for say
    // 100M entries, then mixed_size = 20M = 20% of 100M = total writes in mixed
    // phase. Now, if we define a read-write ratio of 90:10, then 10% writes =
    // 20M, so 90% = 20M/10 * 90 = 180M reads
    // = mixed_size/(100-mixed_reads_perc) * mixed_reads_perc .
    const unsigned mixed_reads =
        mixed_size / (100 - conf.mixed_reads_perc) * conf.mixed_reads_perc;
    const unsigned updates = conf.updates_perc / 100.0 * num_inserts;
    const unsigned num_load = num_inserts - raw_writes - mixed_size;

    std::mt19937 generator(conf.seed);
    std::uniform_int_distribution distribution(0, 1);

    unsigned mix_inserts = 0;
    unsigned mix_queries = 0;
    uint32_t ctr_empty = 0;

    value_type idx = 0;
    auto it = data.cbegin();
    std::cerr << "Preloading (" << num_load << "/" << num_inserts << ")\n";
    auto start = std::chrono::high_resolution_clock::now();
    while (idx < num_load) {
        // tree.insert(*it++ + offset, idx++);
        auto key = *it++ + offset;
        tree.insert(key, key);
        // std::cout << idx << std::endl;
        idx++;
    }
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now() - start);
    results << ", " << duration.count();
    std::cout << "duration = " << duration.count() << std::endl;

    std::cerr << "Raw write (" << raw_writes << "/" << num_inserts << ")\n";
    start = std::chrono::high_resolution_clock::now();
    while (idx < num_load + raw_writes) {
        tree.insert(*it++ + offset, idx++);
    }
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now() - start);
    results << ", " << duration.count();

    std::cerr << "Mixed load (2*" << mixed_size << "/" << num_inserts << ")\n";
    auto mixed_start = std::chrono::high_resolution_clock::now();
    while (mix_inserts < mixed_size || mix_queries < mixed_reads) {
        if (mix_queries >= mixed_reads ||
            (mix_inserts < mixed_size && distribution(generator))) {
            tree.insert(*it++ + offset, idx++);

            mix_inserts++;
        } else {
            key_type query_index = generator() % idx + offset;

            const bool res = tree.contains(query_index);

            ctr_empty += !res;
            mix_queries++;
        }
    }
    auto mixed_stop = std::chrono::high_resolution_clock::now();
    results << ", " << duration.count();

    std::cerr << "Raw read (" << raw_queries << "/" << num_inserts << ")\n";
    std::uniform_int_distribution<unsigned> range_distribution(0,
                                                               num_inserts - 1);
    start = std::chrono::high_resolution_clock::now();
    for (unsigned i = 0; i < raw_queries; i++) {
        tree.contains(data[range_distribution(generator) % data.size()] +
                      offset);
    }
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now() - start);
    results << ", " << duration.count();

    std::cerr << "Updates (" << updates << "/" << num_inserts << ")\n";
    start = std::chrono::high_resolution_clock::now();
    for (unsigned i = 0; i < updates; i++) {
        tree.insert(data[range_distribution(generator) % data.size()] + offset,
                    0);
    }
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now() - start);
    results << ", " << duration.count();

    size_t leaf_accesses = 0;
    size_t k = data.size() / 1000;
    std::cerr << "Range " << k << " (" << conf.short_range << ")\n";
    start = std::chrono::high_resolution_clock::now();
    for (unsigned i = 0; i < conf.short_range; i++) {
        const key_type min_key =
            data[range_distribution(generator) % (data.size() - k)] + offset;
        leaf_accesses += tree.top_k(k, min_key);
    }
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now() - start);
    results << ", " << duration.count() << ", ";
    if (conf.short_range) {
        results << (leaf_accesses - 1 + conf.short_range) /
                       conf.short_range;  // ceil
    }

    leaf_accesses = 0;
    k = data.size() / 100;
    std::cerr << "Range " << k << " (" << conf.mid_range << ")\n";
    start = std::chrono::high_resolution_clock::now();
    for (unsigned i = 0; i < conf.mid_range; i++) {
        const key_type min_key =
            data[range_distribution(generator) % (data.size() - k)] + offset;
        leaf_accesses += tree.top_k(k, min_key);
    }
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now() - start);
    results << ", " << duration.count() << ", ";
    if (conf.mid_range) {
        results << (leaf_accesses - 1 + conf.mid_range) /
                       conf.mid_range;  // ceil
    }

    leaf_accesses = 0;
    k = data.size() / 10;
    std::cerr << "Range " << k << " (" << conf.long_range << ")\n";
    start = std::chrono::high_resolution_clock::now();
    for (unsigned i = 0; i < conf.long_range; i++) {
        const key_type min_key =
            data[range_distribution(generator) % (data.size() - k)] + offset;
        leaf_accesses += tree.top_k(k, min_key);
    }
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now() - start);
    results << ", " << duration.count() << ", ";
    if (conf.long_range) {
        results << (leaf_accesses - 1 + conf.long_range) /
                       conf.long_range;  // ceil
    }

    results << ", " << ctr_empty << ", " << tree << "\n";
#ifndef BENCHMARK
    unsigned count = 0;
    for (const auto &item : data) {
        if (!tree.contains(item)) {
            // std::cerr << item << " not found" << std::endl;
            // break;
            count++;
        }
    }
    if (count) {
        std::cerr << "Error: " << count << " not found\n";
    } else {
        std::cerr << "All good\n";
    }
#endif
}

std::size_t cmp(const key_type &max, const key_type &min) { return max - min; }

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cerr << "Usage: ./tree_analysis <input_file> [<input_file>...]"
                  << std::endl;
        return -1;
    }

    auto config_file = "config.toml";
    auto tree_dat = "tree.dat";

    Config conf(config_file);
    BlockManager manager(tree_dat, conf.blocks_in_memory);

    std::vector<std::vector<key_type>> data;
    for (int i = 1; i < argc; i++) {
        std::cerr << "Reading " << argv[i] << std::endl;
        data.emplace_back(read_file(argv[i]));
    }
    std::ofstream results("results.csv", std::ofstream::app);
    std::string name =
        ""
#ifdef TAIL_FAT
        "TAIL"
#endif
#ifdef LOL_FAT
        "LOL"
#endif
#ifdef LIL_FAT
        "LIL"
#endif
#ifdef LOL_FAT
#ifdef VARIABLE_SPLIT
#ifdef DOUBLE_IQR
        "_DOUBLE_IQR"
#endif
#ifdef REDISTRIBUTE
        "_REDISTRIBUTE"
#endif
        "_VARIABLE"
#endif
#ifdef LOL_RESET
        "_RESET"
#endif
#endif
        ;
    for (unsigned i = 0; i < conf.runs; ++i) {
        manager.reset();
        // bp_tree<key_type, value_type> tree(cmp, manager);
        index_bench::BPTreeIndex<key_type, value_type> tree(cmp, manager);
        key_type offset = 0;
        for (unsigned j = 0; j < conf.repeat; ++j) {
            for (unsigned k = 0; k < data.size(); ++k) {
                const auto &input = data[k];
                results << (name.empty() ? "SIMPLE" : name) << ", "
                        << argv[k + 1] << ", " << offset;
                workload(tree, input, conf, results, offset);
                results.flush();
                offset += input.size();
            }
        }
    }
    return 0;
}
