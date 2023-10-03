#include <random>
#include <chrono>
#include <filesystem>
#include <fstream>

#include "bptree/config.h"
#include "bptree/bp_tree.h"

using key_type = unsigned;
using value_type = unsigned;

std::vector<key_type> read_file(const char *filename) {
    std::vector<key_type> data;
    std::string line;
    std::ifstream ifs(filename);
    while (std::getline(ifs, line)) {
        unsigned key = std::stoul(line);
        data.push_back(key);
    }
    return data;
}

std::vector<key_type> read_bin(const char *filename) {
    std::ifstream inputFile(filename, std::ios::binary);
    inputFile.seekg(0, std::ios::end);
    std::streampos fileSize = inputFile.tellg();
    inputFile.seekg(0, std::ios::beg);
    std::vector<key_type> data(fileSize / sizeof(key_type));
    inputFile.read(reinterpret_cast<char*>(data.data()), fileSize);
    return data;
}

void workload(bp_tree<key_type, value_type> &tree, const char *input_file, unsigned seed,
              unsigned raw_read_perc, unsigned raw_write_perc, unsigned mix_load_perc, unsigned updates_perc) {
    std::vector<key_type> data = read_bin(input_file);

    unsigned num_inserts = data.size();

    unsigned raw_queries = raw_read_perc / 100.0 * num_inserts;
    unsigned raw_writes = raw_write_perc / 100.0 * num_inserts;
    unsigned mixed_size = mix_load_perc / 100.0 * num_inserts;
    unsigned updates = updates_perc / 100.0 * num_inserts;
    unsigned num_load = num_inserts - raw_writes - mixed_size;

    std::mt19937 generator(seed);
    std::uniform_int_distribution<int> distribution(0, 1);

    std::ofstream results("results.csv", std::ofstream::app);
    std::string name = ""
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
#ifdef REDISTRIBUTE
                       "_REDISTRIBUTE"
#else
                       "_VARIABLE"
#endif
#endif
#endif
    ;
    results << (name.empty() ? "SIMPLE" : name) << ", ";

    unsigned mix_inserts = 0;
    unsigned mix_queries = 0;
    uint32_t ctr_empty = 0;

    value_type idx = 0;
    auto it = data.cbegin();
    std::cout << "Preloading (" << num_load << "/" << num_inserts << ")\n";
    auto start = std::chrono::high_resolution_clock::now();
    while (idx < num_load) {
        tree.insert(*it++, idx++);
    }
    auto duration = std::chrono::high_resolution_clock::now() - start;
    results << input_file << ", " << duration.count();

    std::cout << "Raw write (" << raw_writes << "/" << num_inserts << ")\n";
    start = std::chrono::high_resolution_clock::now();
    while (idx < num_load + raw_writes) {
        tree.insert(*it++, idx++);
    }
    duration = std::chrono::high_resolution_clock::now() - start;
    results << ", " << duration.count();

    std::cout << "Mixed load (2*" << mixed_size << "/" << num_inserts << ")\n";
    auto insert_time = start;
    auto query_time = start;
    while (mix_inserts < mixed_size || mix_queries < mixed_size) {
        if (mix_queries >= mixed_size || (mix_inserts < mixed_size && distribution(generator))) {
            auto _start = std::chrono::high_resolution_clock::now();
            tree.insert(*it++, idx++);
            insert_time += std::chrono::high_resolution_clock::now() - _start;
            mix_inserts++;
        } else {
            key_type query_index = generator() % idx;
            auto _start = std::chrono::high_resolution_clock::now();
            bool res = tree.contains(query_index);
            query_time += std::chrono::high_resolution_clock::now() - _start;
            ctr_empty += !res;
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
        tree.contains(data[range_distribution(generator) % data.size()]);
    }
    duration = std::chrono::high_resolution_clock::now() - start;
    results << ", " << duration.count();

    std::cout << "Updates (" << updates << "/" << num_inserts << ")\n";
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < updates; i++) {
        tree.insert(data[range_distribution(generator) % data.size()], 0);
    }
    duration = std::chrono::high_resolution_clock::now() - start;
    results << ", " << duration.count() << ", " << ctr_empty << ", " << tree << "\n";
    results.close();

#ifndef BENCHMARK
    unsigned count = 0;
    for (const auto &item: data) {
        if (!tree.contains(item)) {
            // std::cout << item << " not found" << std::endl;
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

void display_help(const char *name) {
    std::cout << "Usage: " << name << " <input_file> [OPTION...]\n"
                                      "  --help                        Display this information.\n"
                                      "  --config <config_file>        Experiment configurations.\n"
                                      "  --tree <tree_file>            Tree data will be written to tree_file.\n"
                                      "  --seed <seed>                 Seed used by the random generator.\n"
                                      "  --raw_write <perc>            The percentage of input file that should be used for raw writes [0-100]. Default value 0.\n"
                                      "  --raw_read <perc>             The percentage of input file that should be used for raw reads [0-100]. Default value 0.\n"
                                      "  --mixed <perc>                The percentage of input file that should be used for mixed [0-100]. Default value 0.\n"
                                      "  --updates <perc>              The percentage of input file that should be used for updates [0-100]. Default value 0.\n";
}

std::size_t cmp(const key_type &max, const key_type &min) {
    return max - min;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        display_help(argv[0]);
        return -1;
    }

    const char *input_file = argv[1];
    const char *config_file = "config.toml";
    const char *tree_dat = "tree.dat";
    unsigned raw_read_perc = 0;
    unsigned raw_write_perc = 0;
    unsigned mix_load_perc = 0;
    unsigned updates_perc = 0;
    unsigned seed = 1234;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0) {
            display_help(argv[0]);
            return 0;
        } else if (strcmp(argv[i], "--config") == 0) {
            config_file = argv[++i];
        } else if (strcmp(argv[i], "--tree") == 0) {
            tree_dat = argv[++i];
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
        } else {
            std::cerr << "Discarding option: " << argv[i] << std::endl;
        }
    }

    Config config(config_file);

#ifdef TAIL_FAT
    std::cout << "TAIL" << std::endl;
#endif
#ifdef LOL_FAT
    std::cout << "LOL" << std::endl;
#endif
#ifdef LIL_FAT
    std::cout << "LIL" << std::endl;
#endif
    bp_tree<key_type, value_type> tree(tree_dat, config.blocks_in_memory, cmp);
    workload(tree, input_file, seed, raw_read_perc, raw_write_perc, mix_load_perc, updates_perc);

    return 0;
}
