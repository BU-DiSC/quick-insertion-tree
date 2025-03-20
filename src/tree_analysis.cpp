#include <atomic>
#include <cassert>
#include <chrono>
#include <config.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <thread>
#include <trees.hpp>
#include <vector>

using key_type = uint32_t;
using value_type = uint32_t;

// Default to SimpleBTree if nothing else is defined
#if defined(FOR_TAILBTREE)
using namespace TailBTree;
#elif defined(FOR_LILBTREE)
using namespace LILBTree;
#elif defined(FOR_QUIT)
using namespace QuITBTree;
#elif defined(FOR_CONCURRENT_SIMPLE)
using namespace ConcurrentSimpleBTree;
#elif defined(FOR_CONCURRENT_TAIL)
using namespace ConcurrentTailBTree;
#elif defined(FOR_CONCURRENT_QUIT)
using namespace ConcurrentQuITBTree;
#elif defined(FOR_CONCURRENT_QUIT_APPENDS)
using namespace ConcurrentQuITBTree;
#else
using namespace SimpleBTree;  // FOR_SIMPLEBTREE or fallback
#endif

using tree_t = BTree<key_type, value_type>;

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

struct Ticket {
    std::atomic<size_t> _idx;
    const size_t _size;

    size_t get() {
        size_t idx = _idx++;
        return idx < _size ? idx : _size;
    }

    Ticket(size_t begin, size_t end) : _idx(begin), _size(end) {}
};

void insert_worker(tree_t &tree, const std::vector<key_type> &data,
                   Ticket &line, const key_type &offset) {
    auto idx = line.get();
    const auto &size = line._size;
    while (idx < size) {
        // std::cout << "Inserting " << data[idx] << std::endl;
        const key_type &key = data[idx] + offset;
        tree.insert(key, {});
        // ensure all keys inserted so far are found
        idx = line.get();
    }
    std::cout << "done inserting" << std::endl;
}

void update_worker(tree_t &tree, const std::vector<key_type> &data,
                   Ticket &line, const key_type &offset) {
    auto idx = line.get();
    const auto &size = line._size;
    while (idx < size) {
        const key_type &key = data[idx] + offset;
        tree.update(key, {});
        idx = line.get();
    }
}

void query_worker(tree_t &tree, const std::vector<key_type> &data, Ticket &line,
                  const key_type &offset) {
    size_t idx = line.get();
    const auto &size = line._size;
    while (idx < size) {
        const key_type &key = data[idx];
        tree.contains(key + offset);
        idx = line.get();
    }
}

template <typename WorkerFunc>
auto work(WorkerFunc worker_func, tree_t &tree,
          const std::vector<key_type> &data, size_t begin, size_t end,
          uint8_t num_threads, const key_type &offset) {
    Ticket line(begin, end);
    auto start = std::chrono::high_resolution_clock::now();
    {
        std::vector<std::jthread> threads;
        for (size_t i = 0; i < num_threads; ++i) {
            threads.emplace_back(worker_func, std::ref(tree), std::ref(data),
                                 std::ref(line), std::ref(offset));

            cpu_set_t cpuset;
            size_t cpu = (i >> 1) + (i & 1) * 48;
            CPU_ZERO(&cpuset);
            CPU_SET(cpu, &cpuset);
            pthread_setaffinity_np(threads[i].native_handle(),
                                   sizeof(cpu_set_t), &cpuset);
        }
    }
    return std::chrono::high_resolution_clock::now() - start;
}

size_t range_queries(tree_t &tree, const std::vector<key_type> &data,
                     size_t num_inserts, size_t range, const key_type &offset,
                     size_t size, std::mt19937 &generator) {
    size_t leaf_accesses = 0;
    size_t k = num_inserts / size;
    std::uniform_int_distribution<size_t> index(0, num_inserts - k - 1);
    for (size_t i = 0; i < range; i++) {
        const key_type min_key = data[index(generator)] + offset;
        leaf_accesses += tree.select_k(k, min_key);
    }
    return leaf_accesses;
}

class Workload {
    tree_t &tree;
    const Config &conf;
    const key_type offset;
    std::ofstream results;
    std::mt19937 generator;

   public:
    Workload(tree_t &tree, const Config &conf)
        : tree(tree),
          conf(conf),
          offset(0),
          results(conf.results_csv, std::ofstream::app),
          generator(conf.seed) {
        if (!results) {
            std::cerr << "Error: could not open config file "
                      << conf.results_csv << std::endl;
        }
    }

    void run_all(std::vector<std::vector<key_type>> &data) {
        for (size_t j = 0; j < conf.repeat; ++j) {
            for (size_t k = 0; k < data.size(); ++k) {
                run(conf.files[k], data[k]);
            }
        }
    }

    void run_preload(const std::vector<key_type> &data, size_t begin,
                     size_t num_load) {
        if (num_load > 0) {
            std::cout << "Preload (" << num_load << ")\n";
            auto duration = work(insert_worker, tree, data, begin, num_load,
                                 conf.num_threads, offset);
            results << ", " << duration.count();
        }
    }

    void run_writes(const std::vector<key_type> &data, size_t begin,
                    size_t raw_writes) {
        if (raw_writes > 0) {
            std::cout << "Raw write (" << raw_writes << ")\n";
            auto duration = work(insert_worker, tree, data, begin,
                                 begin + raw_writes, conf.num_threads, offset);
            results << ", " << duration.count();
        }
    }

    void run_reads(const std::vector<key_type> &data, size_t num_inserts,
                   size_t raw_queries) {
        if (raw_queries > 0) {
            std::cout << "Raw read (" << raw_queries << ")\n";
            std::vector<key_type> queries;
            std::uniform_int_distribution<size_t> index(0, num_inserts - 1);
            for (size_t i = 0; i < raw_queries; i++) {
                queries.emplace_back(data[index(generator)] + offset);
            }
            auto duration = work(query_worker, tree, data, 0, raw_queries,
                                 conf.num_threads, offset);
            results << ", " << duration.count();
        }
    }

    void run_mixed(const std::vector<key_type> &data, size_t begin,
                   size_t mixed_writes, size_t mixed_reads) {
        if (mixed_writes > 0 || mixed_reads > 0) {
            uint32_t ctr_empty = 0;
            std::uniform_int_distribution coin(0, 1);
            size_t mix_inserts = 0;
            size_t mix_queries = 0;
            Ticket line(begin, begin + mixed_writes);
            std::cout << "Mixed load (" << mixed_reads << '+' << mixed_writes
                      << ")\n";
            auto start = std::chrono::high_resolution_clock::now();
            while (mix_inserts < mixed_writes || mix_queries < mixed_reads) {
                auto idx = line.get();
                if (mix_queries >= mixed_reads ||
                    (mix_inserts < mixed_writes && coin(generator))) {
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
            results << ", " << duration.count() << ", " << ctr_empty;
        }
    }

    void run_updates(const std::vector<key_type> &data, size_t num_inserts,
                     size_t num_updates) {
        if (num_updates > 0) {
            std::cout << "Updates (" << num_updates << ")\n";
            std::vector<key_type> queries;
            std::uniform_int_distribution<size_t> index(0, num_inserts - 1);
            for (size_t i = 0; i < num_updates; i++) {
                queries.emplace_back(data[index(generator)] + offset);
            }
            auto duration = work(update_worker, tree, data, 0, num_updates,
                                 conf.num_threads, offset);
            results << ", " << duration.count();
        }
    }

    void run_range(const std::vector<key_type> &data, size_t num_inserts,
                   size_t range, size_t size) {
        if (range > 0) {
            std::cout << "Range (" << range << ")\n";
            auto start = std::chrono::high_resolution_clock::now();
            size_t leaf_accesses = range_queries(tree, data, num_inserts, range,
                                                 offset, size, generator);
            auto duration = std::chrono::high_resolution_clock::now() - start;
            auto accesses = (leaf_accesses + range - 1) / range;  // ceil
            results << ", " << duration.count() << ", " << accesses;
        }
    }

    void run(const char *name, const std::vector<key_type> &data) {
        const size_t num_inserts = data.size();
        const size_t raw_writes = conf.raw_write_perc / 100.0 * num_inserts;
        const size_t mixed_writes =
            conf.mixed_writes_perc / 100.0 * num_inserts;
        assert(num_inserts >= raw_writes + mixed_writes);
        const size_t num_load = num_inserts - raw_writes - mixed_writes;
        const size_t raw_queries = conf.raw_read_perc / 100.0 * num_inserts;
        const size_t mixed_reads = conf.mixed_reads_perc / 100.0 * num_inserts;
        const size_t num_updates = conf.updates_perc / 100.0 * num_inserts;

        // tree.reset_ctr();

        std::filesystem::path file(name);
        results << tree_t::name << ", " << conf.num_threads << ", "
                << file.filename().c_str() << ", " << offset;

        run_preload(data, 0, num_load);
        run_writes(data, num_load, raw_writes);
        run_mixed(data, num_load + raw_writes, mixed_writes, mixed_reads);
        run_reads(data, num_inserts, raw_queries);
        run_updates(data, num_inserts, num_updates);
        run_range(data, num_inserts, conf.short_range, 1000);
        run_range(data, num_inserts, conf.mid_range, 100);
        run_range(data, num_inserts, conf.long_range, 10);

        if (conf.validate) {
            size_t count = 0;
            for (const auto &item : data) {
                if (!tree.contains(item)) {
                    count++;
#ifdef DEBUG
                    std::cerr << item << " not found" << std::endl;
                    break;
#endif
                }
            }
            if (count) {
                std::cerr << "Error: " << count << " keys not found\n";
            } else {
                std::cerr << "All good\n";
            }
        }

        results << ", ";
        results << tree << std::endl;
    }
};

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cerr << "Usage: ./tree_analysis <input_file>..." << std::endl;
        return -1;
    }

    auto config_file = "config.toml";

    Config conf;
    conf.parse(config_file);
    conf.parse(argc, argv);
    // conf.print();
    tree_t::BlockManager manager(conf.blocks_in_memory);

    std::cout << "Writing results to: " << conf.results_csv << std::endl;

    std::vector<std::vector<key_type>> data;
    for (const auto &file : conf.files) {
        std::filesystem::path fsPath(file);
        std::cout << "Reading " << fsPath.filename().c_str() << std::endl;
        if (conf.binary_input) {
            data.emplace_back(read_bin(file));
        } else {
            data.emplace_back(read_txt(file));
        }
    }

    std::cout << "Running " << tree_t::name << " with " << conf.num_threads
              << " threads\n";
    for (size_t i = 0; i < conf.runs; ++i) {
        manager.reset();
        tree_t tree(manager);
        Workload workload(tree, conf);
        workload.run_all(data);
    }
    return 0;
}
