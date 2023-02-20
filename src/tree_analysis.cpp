#include <random>
#include <chrono>
#include <filesystem>
#include <fstream>

#include "config.h"
#include "dual_tree.h"

std::vector<int> read_file(const char *filename) {
    std::vector<int> data;
    std::string line;
    std::ifstream ifs(filename);
    while (std::getline(ifs, line)) {
        int key = std::stoi(line);
        data.push_back(key);
    }
    ifs.close();
    return data;
}

void workload(kv_store<int, int> &store, const std::vector<int> &data, unsigned raw_read_perc, unsigned raw_write_perc,
              unsigned mix_load_perc, unsigned updates_perc, const std::string &seed) {
    unsigned num_inserts = data.size();

    unsigned raw_queries = raw_read_perc / 100.0 * num_inserts;
    unsigned raw_writes = raw_write_perc / 100.0 * num_inserts;
    unsigned mixed_size = mix_load_perc / 100.0 * num_inserts;
    unsigned updates = updates_perc / 100.0 * num_inserts;
    unsigned num_load = num_inserts - raw_writes - mixed_size;

    std::seed_seq seq(seed.begin(), seed.end());
    std::mt19937 generator{seq};
    std::uniform_int_distribution<int> distribution(0, 1);

    std::ofstream results("results.csv", std::ofstream::app);
    unsigned mix_inserts = 0;
    unsigned mix_queries = 0;
    unsigned empty_queries = 0;

    int idx = 0;
    auto it = data.cbegin();
    std::cout << "Preloading (" << num_load << "/" << num_inserts << ")\n";
    auto start = std::chrono::high_resolution_clock::now();
    while (idx < num_load) {
        store.insert(*it++, idx++);
    }
    auto duration = std::chrono::high_resolution_clock::now() - start;
    results << duration.count();

    std::cout << "Raw write (" << raw_writes << "/" << num_inserts << ")\n";
    start = std::chrono::high_resolution_clock::now();
    while (idx < num_load + raw_writes) {
        store.insert(*it++, idx++);
    }
    duration = std::chrono::high_resolution_clock::now() - start;
    results << ", " << duration.count();

    std::cout << "Mixed load (2*" << mixed_size << "/" << num_inserts << ")\n";
    auto insert_time = start;
    auto query_time = start;
    while (mix_inserts < mixed_size || mix_queries < mixed_size) {
        if (mix_queries >= mixed_size || (mix_inserts < mixed_size && distribution(generator))) {
            auto start_ins = std::chrono::high_resolution_clock::now();
            store.insert(*it++, idx++);
            auto stop_ins = std::chrono::high_resolution_clock::now();
            insert_time += stop_ins - start_ins;
            mix_inserts++;
        } else {
            int query_index = (int) (generator() % idx);
            auto start_q = std::chrono::high_resolution_clock::now();
            bool res = store.contains(query_index);
            auto stop_q = std::chrono::high_resolution_clock::now();
            query_time += stop_q - start_q;
            empty_queries += !res;
            mix_queries++;
        }
    }
    duration = (insert_time - start);
    results << ", " << duration.count();
    duration = (query_time - start);
    results << ", " << duration.count();

    std::cout << "Raw read (" << raw_queries << "/" << num_inserts << ")\n";
    std::uniform_int_distribution<unsigned> range_distribution(0, num_inserts - 1);
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < raw_queries; i++) {
        store.contains(data[range_distribution(generator) % data.size()]);
    }
    duration = std::chrono::high_resolution_clock::now() - start;
    results << ", " << duration.count();

    std::cout << "Updates (" << updates << "/" << num_inserts << ")\n";
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < updates; i++) {
        store.insert(data[range_distribution(generator) % data.size()], 0);
    }
    duration = std::chrono::high_resolution_clock::now() - start;
    results << ", " << duration.count() << ", " << empty_queries << ", " << store << "\n";

    int count = 0;
    for (const auto &item: data) {
        if (!store.contains(item)) {
            count++;
        }
    }
    if (count) {
        std::cerr << "Error: " << count << " not found\n";
    }
}

void display_help(const char *name) {
    std::cout << "Usage: " << name << " <input_file> [OPTION...]\n"
                                      "  --help                       Display this information.\n"
                                      "  --seed                       Seed used by the random generator.\n"
                                      "  --config <config_file>       Use a Dual B+ tree. If not provided a simple B+ tree will be used.\n"
                                      "  --perc_load <perc_load>      The percentage of input file that should be loaded initially [0-100]. Default value 100.\n"
                                      "  --num_queries <num_queries>  The number of queries to be executed after the initial load. Default value 0.\n";
}

enum TreeType {
    SIMPLE, FAST, DUAL
};

int main(int argc, char **argv) {
    if (argc < 2) {
        display_help(argv[0]);
        return -1;
    }

    const char *input_file = argv[1];
    const char *config_file = "config.toml";
    const char *tree_dat = "tree.dat";
    const char *outlier_dat = "outlier.dat";
    TreeType type = TreeType::DUAL;
    int raw_read_perc = 10;
    int raw_write_perc = 10;
    int mix_load_perc = 10;
    int updates_perc = 10;
    const char *seed = "1234";
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0) {
            display_help(argv[0]);
            return 0;
        } else if (strcmp(argv[i], "--config") == 0) {
            config_file = argv[++i];
        } else if (strcmp(argv[i], "--tree") == 0) {
            tree_dat = argv[++i];
        } else if (strcmp(argv[i], "--outlier") == 0) {
            outlier_dat = argv[++i];
        } else if (strcmp(argv[i], "--seed") == 0) {
            seed = argv[++i];
        } else if (strcmp(argv[i], "--raw_write") == 0) {
            raw_write_perc = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--raw_read") == 0) {
            raw_read_perc = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--mixed") == 0) {
            mix_load_perc = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--updates") == 0) {
            updates_perc = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--simple") == 0) {
            type = TreeType::SIMPLE;
        } else if (strcmp(argv[i], "--fast") == 0) {
            type = TreeType::FAST;
        } else if (strcmp(argv[i], "--dual") == 0) {
            type = TreeType::DUAL;
        } else {
            std::cerr << "Discarding option: " << argv[i] << std::endl;
        }
    }

    srand(std::stoi(seed));
    std::vector<int> data = read_file(input_file);
    int idx = 0;
    int cnt = 0;
    for (const auto &i: data) {
        cnt += idx++ == i;
    }
    std::cout << "Number of keys in sorted position: " << cnt << std::endl;

    Config config(config_file);
    switch (type) {
        case SIMPLE: {
            std::cout << "Single B+ tree" << std::endl;
            bp_tree<int, int> tree(tree_dat, config.blocks_in_memory, config.unsorted_tree_split_frac);
            workload(tree, data, raw_read_perc, raw_write_perc, mix_load_perc, updates_perc, seed);
            break;
        }
        case FAST: {
            std::cout << "Fast Append B+ tree" << std::endl;
            FastAppendTree<int, int> tree(tree_dat, config.blocks_in_memory, config.sorted_tree_split_frac);
            workload(tree, data, raw_read_perc, raw_write_perc, mix_load_perc, updates_perc, seed);
            break;
        }
        case DUAL: {
            std::cout << "Dual B+ tree" << std::endl;
            dual_tree<int, int> tree(tree_dat, outlier_dat, config);
            workload(tree, data, raw_read_perc, raw_write_perc, mix_load_perc, updates_perc, seed);
            break;
        }
    }
    return 0;
}
