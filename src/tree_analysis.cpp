#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <atomic>
#include <thread>
#include <vector>

#include "bptree/config.h"
#include "bptree/bp_tree.h"

using key_type = unsigned;
using value_type = unsigned;

std::vector<key_type> read_txt(const char *filename) {
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
    assert(inputFile.is_open());
    inputFile.seekg(0, std::ios::end);
    const std::streampos fileSize = inputFile.tellg();
    inputFile.seekg(0, std::ios::beg);
    std::vector<key_type> data(fileSize / sizeof(key_type));
    inputFile.read(reinterpret_cast<char *>(data.data()), fileSize);
    return data;
}

class Ticket {
    std::atomic<unsigned> _idx;
    const size_t size;
public:
    unsigned get() {
        unsigned idx = _idx++;
        return idx < size ? idx : size;
    }

    Ticket(size_t first, size_t size) : _idx(first), size(size) {}
    explicit Ticket(size_t size) : _idx(0), size(size) {}
};

unsigned hash(unsigned num) {
    return (num >> 1) + (num & 1) * 48;
}

void insert_worker(bp_tree<key_type, value_type> &tree, const std::vector<key_type> &data, Ticket &line,
                   const key_type &offset) {
    auto idx = line.get();
    const auto &size = data.size();
    while (idx < size) {
        const key_type &key = data[idx] + offset;
        tree.insert(key, 0);
        idx = line.get();
    }
}

void query_worker(bp_tree<key_type, value_type> &tree, const std::vector<key_type> &data, Ticket &line,
                  const key_type &offset) {
    unsigned idx = line.get();
    const auto &size = data.size();
    while (idx < size) {
        const key_type &key = data[idx];
        tree.contains(key + offset);
        idx = line.get();
    }
}

void workload(bp_tree<key_type, value_type> &tree, const std::vector<key_type> &data, const Config &conf,
              std::ofstream &results, const key_type &offset) {
    const unsigned num_inserts = data.size();
    const unsigned raw_queries = conf.raw_read_perc / 100.0 * num_inserts;
    const unsigned raw_writes = conf.raw_write_perc / 100.0 * num_inserts;
    const unsigned mixed_writes = conf.mixed_writes_perc / 100.0 * num_inserts;
    const unsigned mixed_reads = conf.mixed_reads_perc / 100.0 * num_inserts;
    const unsigned updates = conf.updates_perc / 100.0 * num_inserts;
    assert(num_inserts >= raw_writes + mixed_writes);
    const unsigned num_load = num_inserts - raw_writes - mixed_writes;

    std::mt19937 generator(conf.seed);
    uint32_t ctr_empty = 0;

    results << ", ";
    if (num_load > 0) {
        Ticket line(num_load);
        std::cerr << "Preloading (" << num_load << ")\n";
        auto start = std::chrono::high_resolution_clock::now();
        {
            std::vector<std::thread> threads;

            for (int i = 0; i < conf.num_w_threads; ++i) {
                threads.emplace_back(insert_worker, std::ref(tree), std::ref(data), std::ref(line), std::ref(offset));

                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);
                CPU_SET(hash(i), &cpuset);
                int rc = pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
                if (rc != 0) {
                    std::cerr << "Error calling pthread_setaffinity_np: " << rc << '\n';
                }
            }

            for (auto &thread: threads) {
                thread.join();
            }
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        results << duration.count();
    }

    results << ", ";
    if (raw_writes > 0) {
        Ticket line(num_load, num_load + raw_writes);
        std::cerr << "Raw write (" << raw_writes << ")\n";
        auto start = std::chrono::high_resolution_clock::now();
        insert_worker(tree, data, line, offset);
        auto duration = std::chrono::high_resolution_clock::now() - start;
        results << duration.count();
    }

    results << ", ";
    if (mixed_writes > 0 || mixed_reads > 0) {
        std::uniform_int_distribution coin(0, 1);
        unsigned mix_inserts = 0;
        unsigned mix_queries = 0;
        Ticket line(num_load + raw_writes, num_load + raw_writes + mixed_writes);
        std::cerr << "Mixed load (" << mixed_reads << '+' << mixed_writes << ")\n";
        auto start = std::chrono::high_resolution_clock::now();
        while (mix_inserts < mixed_writes || mix_queries < mixed_reads) {
            auto idx = line.get();
            if (mix_queries >= mixed_reads || (mix_inserts < mixed_writes && coin(generator))) {
                const key_type &key = data[idx] + offset;
                tree.insert(key, idx);

                mix_inserts++;
            } else {
                key_type query_index = generator() % idx + offset;

                const bool res = tree.contains(query_index);

                ctr_empty += !res;
                mix_queries++;
            }
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        results << duration.count();
    }

    results << ", ";
    if (raw_queries > 0) {
        std::vector<key_type> queries;
        std::uniform_int_distribution<unsigned> index(0, num_inserts - 1);
        for (unsigned i = 0; i < raw_queries; i++) {
            queries.emplace_back(data[index(generator)]);
        }
        Ticket line(raw_queries);
        auto start = std::chrono::high_resolution_clock::now();
        {
            std::vector<std::thread> threads;

            for (int i = 0; i < conf.num_r_threads; ++i) {
                threads.emplace_back(query_worker, std::ref(tree), std::ref(queries), std::ref(line), std::ref(offset));

                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);
                CPU_SET(hash(i), &cpuset);
                int rc = pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
                if (rc != 0) {
                    std::cerr << "Error calling pthread_setaffinity_np: " << rc << '\n';
                }
            }

            for (auto &thread: threads) {
                thread.join();
            }
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        results << duration.count();
    }

    results << ", ";
    if (updates > 0) {
        std::cerr << "Updates (" << updates << ")\n";
        std::uniform_int_distribution<unsigned> index(0, num_inserts - 1);
        auto start = std::chrono::high_resolution_clock::now();
        for (unsigned i = 0; i < updates; i++) {
            tree.insert(data[index(generator)] + offset, 0);
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        results << duration.count();
    }

    results << ", ";
    if (conf.short_range > 0) {
        size_t leaf_accesses = 0;
        size_t k = num_inserts / 1000;
        std::cerr << "Range " << k << " (" << conf.short_range << ")\n";
        std::uniform_int_distribution<unsigned> index(0, num_inserts - k - 1);
        auto start = std::chrono::high_resolution_clock::now();
        for (unsigned i = 0; i < conf.short_range; i++) {
            const key_type min_key = data[index(generator)] + offset;
            leaf_accesses += tree.select_k(k, min_key);
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        auto accesses = (leaf_accesses - 1 + conf.short_range) / conf.short_range;  // ceil
        results << duration.count() << ", " << accesses;
    } else {
        results << ", ";
    }

    results << ", ";
    if (conf.mid_range > 0) {
        size_t leaf_accesses = 0;
        size_t k = num_inserts / 100;
        std::cerr << "Range " << k << " (" << conf.mid_range << ")\n";
        std::uniform_int_distribution<unsigned> index(0, num_inserts - k - 1);
        auto start = std::chrono::high_resolution_clock::now();
        for (unsigned i = 0; i < conf.mid_range; i++) {
            const key_type min_key = data[index(generator)] + offset;
            leaf_accesses += tree.select_k(k, min_key);
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        auto accesses = (leaf_accesses - 1 + conf.mid_range) / conf.mid_range;  // ceil
        results << duration.count() << ", " << accesses;
    } else {
        results << ", ";
    }


    results << ", ";
    if (conf.long_range > 0) {
        size_t leaf_accesses = 0;
        size_t k = num_inserts / 10;
        std::cerr << "Range " << k << " (" << conf.long_range << ")\n";
        std::uniform_int_distribution<unsigned> index(0, num_inserts - k - 1);
        auto start = std::chrono::high_resolution_clock::now();
        for (unsigned i = 0; i < conf.long_range; i++) {
            const key_type min_key = data[index(generator)] + offset;
            leaf_accesses += tree.select_k(k, min_key);
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        auto accesses = (leaf_accesses - 1 + conf.long_range) / conf.long_range;  // ceil
        results << duration.count() << ", " << accesses;
    } else {
        results << ", ";
    }

    results << ", " << ctr_empty << ", " << tree << "\n";

    if (conf.validate) {
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
    }
}

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cerr << "Usage: ./tree_analysis <input_file>..." << std::endl;
        return -1;
    }

    auto config_file = "config.toml";
    auto tree_dat = "tree.dat";

    Config conf(config_file);
    BlockManager manager(tree_dat, conf.blocks_in_memory);

    auto results_csv = conf.results_csv;
    std::cerr << "Writing results to: " << results_csv << std::endl;

    std::vector<std::vector<key_type>> data;
    for (int i = 1; i < argc; i++) {
        std::cerr << "Reading " << argv[i] << std::endl;
        if (conf.binary_input) {
            data.emplace_back(read_bin(argv[i]));
        } else {
            data.emplace_back(read_txt(argv[i]));
        }
    }
    std::ofstream results(results_csv, std::ofstream::app);

    const char *name =
#ifndef FAST_PATH
            "SIMPLE"
#else
#ifdef QUIT_FAT
            "QUIT"
#else
#ifdef TAIL_FAT
            "TAIL"
#else
#ifdef LIL_FAT
            "LIL"
#else
    #ifdef LOL_FAT
    "LOL"
#ifdef REDISTRIBUTE
    "_REDISTRIBUTE"
#endif
#ifdef VARIABLE_SPLIT
    "_VARIABLE"
#endif
#ifdef LOL_RESET
    "_RESET"
#endif
#endif
#endif
#endif
#endif
#endif
    ;

    for (unsigned i = 0; i < conf.runs; ++i) {
        manager.reset();
        bp_tree<key_type, value_type> tree(manager);
        key_type offset = 0;
        for (unsigned j = 0; j < conf.repeat; ++j) {
            for (unsigned k = 0; k < data.size(); ++k) {
                const auto &input = data[k];
                results << name << '_' << conf.num_r_threads << '_' << conf.num_w_threads << ", " << argv[k + 1] << ", " << offset;
                workload(tree, input, conf, results, offset);
                results.flush();
                offset += input.size();
            }
        }
    }
    return 0;
}
