#include <random>
#include <chrono>
#include <filesystem>
#include <fstream>

#include "config.h"
#include "dual_tree.h"

std::vector<unsigned> read_file(const char *filename) {
    std::vector<unsigned> data;
    std::string line;
    std::ifstream ifs(filename);
    while (std::getline(ifs, line)) {
        unsigned key = std::stoi(line);
        data.push_back(key);
    }
    ifs.close();
    return data;
}

void workload(kv_store<unsigned, unsigned> &store, const char *input_file, unsigned raw_read_perc, unsigned raw_write_perc,
              unsigned mix_load_perc, unsigned updates_perc, unsigned seed) {

    std::vector<unsigned> data = read_file(input_file);

    unsigned num_inserts = data.size();

    unsigned raw_queries = raw_read_perc / 100.0 * num_inserts;
    unsigned raw_writes = raw_write_perc / 100.0 * num_inserts;
    unsigned mixed_size = mix_load_perc / 100.0 * num_inserts;
    unsigned updates = updates_perc / 100.0 * num_inserts;
    unsigned num_load = num_inserts - raw_writes - mixed_size;

    std::mt19937 generator(seed);
    std::uniform_int_distribution<int> distribution(0, 1);

    std::ofstream results("results.csv", std::ofstream::app);
    unsigned mix_inserts = 0;
    unsigned mix_queries = 0;
    unsigned empty_queries = 0;

    unsigned idx = 0;
    auto it = data.cbegin();
    std::cout << "Preloading (" << num_load << "/" << num_inserts << ")\n";
    auto start = std::chrono::high_resolution_clock::now();
    while (idx < num_load) {
        store.insert(*it++, idx++);
    }
    auto duration = std::chrono::high_resolution_clock::now() - start;
    results << input_file <<", " << duration.count();

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

#ifdef DEBUG
    unsigned count = 0;
    for (const auto &item: data) {
        if (!store.contains(item)) {
            count++;
        }
    }
    if (count) {
        std::cerr << "Error: " << count << " not found\n";
    }
#endif
}

void display_help(const char *name) {
    std::cout << "Usage: " << name << " <input_file> [OPTION...]\n"
                                      "  --help                       Display this information.\n"
                                      "  --tree <tree_file>           Tree data will be written to tree_file.\n"
                                      "  --outlier <tree_file>        Outlier data will be written to tree_file.\n"
                                      "  --seed <seed>                Seed used by the random generator.\n"
                                      "  --config <config_file>       Experiment configurations.\n"
                                      "  --raw_write <perc>           The percentage of input file that should be used for raw writes [0-100]. Default value 0.\n"
                                      "  --raw_read <perc>            The percentage of input file that should be used for raw reads [0-100]. Default value 0.\n"
                                      "  --mixed <perc>               The percentage of input file that should be used for mixed [0-100]. Default value 0.\n"
                                      "  --updates <perc>             The percentage of input file that should be used for updates [0-100]. Default value 0.\n"
                                      "  --simple                     Use a simple B+ tree\n"
                                      "  --fast                       Use a fast append B+ tree\n"
                                      "  --dual                       Use a dual B+ tree\n";
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
    unsigned raw_read_perc = 10;
    unsigned raw_write_perc = 10;
    unsigned mix_load_perc = 10;
    unsigned updates_perc = 10;
    unsigned seed = 1234;
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
            seed = std::stoi(argv[++i]);
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

    Config config(config_file);
    switch (type) {
        case SIMPLE: {
            std::cout << "Simple B+ tree" << std::endl;
            bp_tree<unsigned, unsigned> tree(tree_dat, config.blocks_in_memory, config.unsorted_tree_split_frac);
            workload(tree, input_file, raw_read_perc, raw_write_perc, mix_load_perc, updates_perc, seed);
            break;
        }
        case FAST: {
            std::cout << "Fast Append B+ tree" << std::endl;
            std::cout <<" Split factor = "<<config.sorted_tree_split_frac<<std::endl;
            FastAppendTree<unsigned, unsigned> tree(tree_dat, config.blocks_in_memory, config.sorted_tree_split_frac);
            workload(tree, input_file, raw_read_perc, raw_write_perc, mix_load_perc, updates_perc, seed);
            break;
        }
        case DUAL: {
            std::cout << "Dual B+ tree" << std::endl;
            dual_tree<unsigned, unsigned> tree(tree_dat, outlier_dat, config);
            workload(tree, input_file, raw_read_perc, raw_write_perc, mix_load_perc, updates_perc, seed);
            break;
        }
    }
    return 0;
}
