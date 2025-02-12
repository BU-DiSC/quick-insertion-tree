#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <atomic>

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
//    unsigned _idx;
    const size_t size;
public:
    unsigned get() {
        unsigned idx = _idx++;
        return idx < size ? idx : size;
    }

    Ticket(size_t first, size_t size) : _idx(first), size(size) {}
    explicit Ticket(size_t size) : _idx(0), size(size) {}
};

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
    const unsigned mixed_size = conf.mix_load_perc / 100.0 * num_inserts;
    /*
     * mixed reads is a percentage of the mixed size.
     * E.g., if we are going to do 20% of writes (from the total workload) in the mixed r-w phase for say 100M entries,
     * then mixed_size = 20M = 20% of 100M = total writes in mixed phase.
     * Now, if we define a read-write ratio of 90:10, then 10% writes = 20M,
     * so 90% = 20M/10 * 90 = 180M reads= mixed_size/(100-mixed_reads_perc) * mixed_reads_perc
     */
    const unsigned mixed_reads = mixed_size / (100 - conf.mixed_reads_perc) * conf.mixed_reads_perc;
    const unsigned updates = conf.updates_perc / 100.0 * num_inserts;
    const unsigned num_load = num_inserts - raw_writes - mixed_size;

    std::mt19937 generator(conf.seed);
    std::uniform_int_distribution distribution(0, 1);
    std::uniform_int_distribution<unsigned> range_distribution(0, num_inserts - 1);

    unsigned mix_inserts = 0;
    unsigned mix_queries = 0;
    uint32_t ctr_empty = 0;

    results << ", ";
    if (num_load > 0) {
        Ticket line(num_load);
        std::cerr << "Preloading (" << num_load << "/" << num_inserts << ")\n";
        auto start = std::chrono::high_resolution_clock::now();
        insert_worker(tree, data, line, offset);
        auto duration = std::chrono::high_resolution_clock::now() - start;
        results << duration.count();
    }

    results << ", ";
    if (raw_writes > 0) {
        Ticket line(num_load, num_load + raw_writes);
        std::cerr << "Raw write (" << raw_writes << "/" << num_inserts << ")\n";
        auto start = std::chrono::high_resolution_clock::now();
        insert_worker(tree, data, line, offset);
        auto duration = std::chrono::high_resolution_clock::now() - start;
        results << duration.count();
    }

    results << ", ";
    if (mixed_size > 0) {
        Ticket line(num_load + raw_writes, num_load + raw_writes + mixed_size);
        std::cerr << "Mixed load (2*" << mixed_size << "/" << num_inserts << ")\n";
        auto start = std::chrono::high_resolution_clock::now();
        while (mix_inserts < mixed_size || mix_queries < mixed_reads) {
            auto idx = line.get();
            if (mix_queries >= mixed_reads || (mix_inserts < mixed_size && distribution(generator))) {
                const key_type &key = data[idx] + offset;
                tree.insert(key, idx++);

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
        std::cerr << "Raw read (" << raw_queries << "/" << num_inserts << ")\n";
        auto start = std::chrono::high_resolution_clock::now();
        for (unsigned i = 0; i < raw_queries; i++) {
            key_type key = data[range_distribution(generator) % data.size()] + offset;
            tree.contains(key);
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        results << duration.count();
    }

    results << ", ";
    if (updates > 0) {
        std::cerr << "Updates (" << updates << "/" << num_inserts << ")\n";
        auto start = std::chrono::high_resolution_clock::now();
        for (unsigned i = 0; i < updates; i++) {
            tree.insert(data[range_distribution(generator) % data.size()] + offset, 0);
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        results << duration.count();
    }

    results << ", ";
    if (conf.short_range > 0) {
        size_t leaf_accesses = 0;
        size_t k = data.size() / 1000;
        std::cerr << "Range " << k << " (" << conf.short_range << ")\n";
        auto start = std::chrono::high_resolution_clock::now();
        for (unsigned i = 0; i < conf.short_range; i++) {
            const key_type min_key = data[range_distribution(generator) % (data.size() - k)] + offset;
            leaf_accesses += tree.top_k(k, min_key);
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        auto avg = (leaf_accesses - 1 + conf.short_range) / conf.short_range;  // ceil
        results << duration.count() << ", " << avg;
    } else {
        results << ", ";
    }

    results << ", ";
    if (conf.mid_range > 0) {
        size_t leaf_accesses = 0;
        size_t k = data.size() / 100;
        std::cerr << "Range " << k << " (" << conf.mid_range << ")\n";
        auto start = std::chrono::high_resolution_clock::now();
        for (unsigned i = 0; i < conf.mid_range; i++) {
            const key_type min_key = data[range_distribution(generator) % (data.size() - k)] + offset;
            leaf_accesses += tree.top_k(k, min_key);
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        auto avg = (leaf_accesses - 1 + conf.mid_range) / conf.mid_range;  // ceil
        results << duration.count() << ", " << avg;
    } else {
        results << ", ";
    }

    results << ", ";
    if (conf.long_range > 0) {
        size_t leaf_accesses = 0;
        size_t k = data.size() / 10;
        std::cerr << "Range " << k << " (" << conf.long_range << ")\n";
        auto start = std::chrono::high_resolution_clock::now();
        for (unsigned i = 0; i < conf.long_range; i++) {
            const key_type min_key = data[range_distribution(generator) % (data.size() - k)] + offset;
            leaf_accesses += tree.top_k(k, min_key);
        }
        auto duration = std::chrono::high_resolution_clock::now() - start;
        auto avg = (leaf_accesses - 1 + conf.long_range) / conf.long_range;  // ceil
        results << duration.count() << ", " << avg;
    } else {
        results << ", ";
    }

    results << ", " << ctr_empty << ", " << tree << "\n";

    if (conf.validate) {
        unsigned count = 0;
        for (const auto &item: data) {
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
                results << name << ", " << argv[k + 1] << ", " << offset;
                workload(tree, input, conf, results, offset);
                results.flush();
                offset += input.size();
            }
        }
    }
    return 0;
}
