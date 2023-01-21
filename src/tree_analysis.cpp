#include <random>
#include <chrono>
#include <filesystem>
#include "dual_tree.h"

#ifndef INSERT_TO_QUERY_RATIO
#define INSERT_TO_QUERY_RATIO 0.5
#endif

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

void workload(collection<int, int> *tree, const std::vector<int> &data, unsigned int num_queries,
              unsigned int perc_load, const std::string &seed) {
    unsigned int num_inserts = data.size();
    unsigned int num_load = perc_load / 100.0 * num_inserts;

    std::cout << "Loading " << perc_load << "% (" << num_load << "/" << num_inserts << ")\n";

    std::seed_seq seq(seed.begin(), seed.end());
    std::mt19937 generator{seq};
    std::uniform_real_distribution<double> distribution(0.0, 1.0);

    unsigned int tot_inserts = num_load;
    unsigned int tot_queries = 0;
    unsigned int empty_queries = 0;

    int idx = 0;
    auto it = data.cbegin();
    auto start = std::chrono::high_resolution_clock::now();
    while (idx < num_load) {
        tree->add(*it++, idx++);
    }
    auto stop_load = std::chrono::high_resolution_clock::now();

    auto insert_time = start;
    auto query_time = start;
    while (tot_inserts < num_inserts || tot_queries < num_queries) {
        if (tot_queries >= num_queries ||
            (tot_inserts < num_inserts && distribution(generator) < INSERT_TO_QUERY_RATIO)) {
            auto start_ins = std::chrono::high_resolution_clock::now();
            tree->add(*it++, idx++);
            auto stop_ins = std::chrono::high_resolution_clock::now();
            insert_time += stop_ins - start_ins;
            tot_inserts++;
        } else {
            int query_index = static_cast<int>(generator() % tot_inserts);
            auto start_q = std::chrono::high_resolution_clock::now();
            bool res = tree->contains(query_index);
            auto stop_q = std::chrono::high_resolution_clock::now();
            query_time += stop_q - start_q;
            tot_queries++;
            empty_queries += !res;
        }
    }

    std::uniform_int_distribution<unsigned int> range_distribution(0, num_inserts - 1);
    auto raw_start = std::chrono::high_resolution_clock::now();
    // 10% random queries from workload, no empty queries
    for (int i = 0; i < num_inserts * 0.1; i++) {
        tree->contains(data[range_distribution(generator) % data.size()]);
    }
    auto stop = std::chrono::high_resolution_clock::now();

    auto duration = (stop - start).count();
    auto load_duration = (stop_load - start).count();
    auto insert_duration = (insert_time - start).count();
    auto query_duration = (query_time - start).count();
    auto raw_duration = (stop - raw_start).count();

    std::ofstream results("results.csv", std::ofstream::app);
    results << load_duration << ", " << insert_duration << ", " << query_duration << ", " << raw_duration << ", "
            << duration << ", " << tot_inserts << ", " << tot_queries << ", " << empty_queries << ", " << *tree << "\n";
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
    const char *config_file = nullptr;
    TreeType type = TreeType::DUAL;
    int num_queries = 0;
    int perc_load = 100;
    std::string seed;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0) {
            display_help(argv[0]);
            return 0;
        } else if (strcmp(argv[i], "--config") == 0) {
            config_file = argv[++i];
        } else if (strcmp(argv[i], "--seed") == 0) {
            seed = argv[++i];
        } else if (strcmp(argv[i], "--num_queries") == 0) {
            num_queries = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--perc_load") == 0) {
            perc_load = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--simple") == 0) {
            type = TreeType::SIMPLE;
        } else if (strcmp(argv[i], "--fast") == 0) {
            type = TreeType::FAST;
        } else {
            std::cerr << "Discarding option: " << argv[++i] << std::endl;
        }
    }

    std::vector<int> data = read_file(input_file);
    int idx = 0;
    int cnt = 0;
    for (const auto &i: data) {
        cnt += idx++ == i;
    }
    std::cout << "Number of keys in sorted position: " << cnt << std::endl;

    collection<int, int> *tree;
    std::string root_dir = "tree_dat";
    std::filesystem::create_directories(root_dir);
    switch (type) {
        case SIMPLE:
            std::cout << "Single Btree" << std::endl;
            tree = new BeTree<int, int>("manager", root_dir, BeTree_Default_Knobs<int, int>::BLOCK_SIZE,
                                        BeTree_Default_Knobs<int, int>::BLOCKS_IN_MEMORY);
            break;
        case FAST:
            std::cout << "Fast Append Btree" << std::endl;
            tree = new FastAppendBeTree<int, int>("manager", root_dir,
                                                  BeTree_Default_Knobs<int, int>::BLOCK_SIZE,
                                                  BeTree_Default_Knobs<int, int>::BLOCKS_IN_MEMORY);
            break;
        case DUAL:
            std::cout << "Dual Btree" << std::endl;
            DUAL_TREE_KNOBS::CONFIG_FILE_PATH = config_file;
            tree = new dual_tree<int, int>("tree_1", "tree_2", root_dir);
            break;
    }
    workload(tree, data, num_queries, perc_load, seed);
    delete tree;
    return 0;
}
