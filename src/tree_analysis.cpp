#include <random>
#include <memory>
#include <dual_tree.h>
#include <chrono>
#include <filesystem>

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

void workload(const std::unique_ptr<collection<int, int>> &tree, const std::vector<int> &data, int num_queries,
              int perc_load, const std::string &seed) {
    auto num_inserts = data.size();
    auto num_load = num_inserts * perc_load / 100;

    std::cout << "Loading " << perc_load << "% (" << num_load << "/" << num_inserts << ")\n";

    std::seed_seq seq(seed.begin(), seed.end());
    std::mt19937 generator{seq};
    std::uniform_real_distribution<double> distribution(0.0, 1.0);

    unsigned long tot_inserts = num_load;
    unsigned long tot_queries = 0;
    unsigned long empty_queries = 0;

    auto start = std::chrono::high_resolution_clock::now();
    int idx = 0;
    auto it = data.cbegin();
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
    auto stop = std::chrono::high_resolution_clock::now();

    auto duration = (stop - start).count();
    auto load_duration = (stop_load - start).count();
    auto insert_duration = (insert_time - start).count();
    auto query_duration = (query_time - start).count();

    std::ofstream results("results.csv", std::ofstream::app);
    results << duration << ", " << load_duration << ", " << insert_duration << ", " << query_duration << ", "
            << tot_inserts << ", " << tot_queries << ", " << empty_queries << ", " << *tree << "\n";
}

void display_help(const char *name) {
    std::cout << "Usage: " << name << " <input_file> [OPTION...]\n"
                                      "  --help                       Display this information.\n"
                                      "  --seed                       Seed used by the random generator.\n"
                                      "  --config <config_file>       Use a Dual B+ tree. If not provided a simple B+ tree will be used.\n"
                                      "  --perc_load <perc_load>      The percentage of input file that should be loaded initially [0-100]. Default value 100.\n"
                                      "  --num_queries <num_queries>  The number of queries to be executed after the initial load. Default value 0.\n";
}

int main(int argc, char **argv) {
    if (argc < 2) {
        display_help(argv[0]);
        return -1;
    }

    const char *input_file = argv[1];
    const char *config_file = nullptr;
    int num_queries = 0;
    int perc_load = 100;
    std::string seed;
    for (int i = 0; i < argc; i++) {
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
        }
    }

    std::vector<int> data = read_file(input_file);
    int idx = 0;
    int cnt = 0;
    for (const auto &i: data) {
        cnt += idx++ == i;
    }
    std::cout << "Number of keys in sorted position: " << cnt << std::endl;

    std::unique_ptr<collection<int, int>> tree;
    std::string root_dir = "tree_dat";
    std::filesystem::create_directories(root_dir);
    if (config_file) {
        tree = std::make_unique<dual_tree<int, int>>("tree_1", "tree_2", root_dir, config_file);
    } else {
        tree = std::make_unique<BeTree<int, int>>("manager", root_dir, BeTree_Default_Knobs<int, int>::BLOCK_SIZE,
                                                  BeTree_Default_Knobs<int, int>::BLOCKS_IN_MEMORY);
    }
    workload(tree, data, num_queries, perc_load, seed);
    return 0;
}
