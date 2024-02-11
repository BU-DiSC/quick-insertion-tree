#include <spdlog/spdlog.h>
#include <utilities/lib/stats/stats.h>
#include <utilities/lib/stats/stats_printer.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <tabulate/table.hpp>
#include <unordered_set>

#include "bptree/config.h"
#include "sware/satree.h"
#include "sware/swareBuffer.h"

using namespace std;
using namespace tabulate;

using key_type = unsigned;

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
    std::ifstream inputFile(filename);
    inputFile.seekg(0, std::ios::end);
    std::streampos fileSize = inputFile.tellg();
    inputFile.seekg(0, std::ios::beg);
    std::vector<key_type> data(fileSize / sizeof(key_type));
    inputFile.read(reinterpret_cast<char *>(data.data()), fileSize);
    return data;
}

template <typename counter_type>
void display_stats(utils::stats::Counters<counter_type> stats,
                   std::string title) {
#ifdef TABULATE_COUNTERS
    utils::stats::tabulate_stats<counter_type>(stats, title);
#elif defined(SPDLOG_COUNTERS)
    utils::stats::log_stats<counter_type>(stats);
#else
    utils::stats::print_stats<counter_type>(stats);
#endif
}

std::size_t cmp(const key_type &max, const key_type &min) { return max - min; }

int main(int argc, char *argv[]) {
    string input_file = "";
    string output_file = "";
    int l, num_nodes_for_buffer_pool, num_entries_buffer_can_hold;
    int fill_factor_p;

    key_type k, n;

    key_type num_queries;

    if (argc > 9) {
        input_file = argv[1];
        output_file = argv[2];
        num_nodes_for_buffer_pool = atoi(argv[3]);
        l = atoi(argv[5]);
        k = atoi(argv[4]);
        n = atoi(argv[6]);

        num_entries_buffer_can_hold = atoi(argv[7]);
        fill_factor_p = atoi(argv[8]);
        if (fill_factor_p < 1 || fill_factor_p > 100) {
            cout << "Fill Factor has to be between 1% and 100%" << endl;
            return 0;
        }
        num_queries = atol(argv[9]);
    } else {
        cerr << "Invalid no. of arguments" << endl;
        cerr << "Use: ./test_osmtree <input_workload_file> <output_file> "
                "<num_nodes_in_buffer_pool_cache> <k> <l> <num_inserts> "
                "<num_entries_osmBuffer_can_hold> <fill_factor_percentage> "
                "<num_queries>";
        return 0;
    }

    cout << "input file = " << input_file << endl;
    cout << "output file = " << output_file << endl;
    double fill_factor = fill_factor_p / 100.0;

    const char *config_file = "../config.toml";
    const char *tree_dat = "tree.dat";
    Config conf(config_file);
    BlockManager manager(tree_dat, conf.blocks_in_memory);

#ifdef SPDLOG_STATS
    spdlog::info("blocks in memory = {}", conf.blocks_in_memory);
#else
    cout << "blocks in memory = " << conf.blocks_in_memory << endl;
#endif
    OsmTree<key_type, key_type> tree(cmp, manager, num_entries_buffer_can_hold,
                                     fill_factor);

    long int size = 0;

    ofstream outfile(output_file, ios::out | ios::app);
    if (!outfile) {
        cout << "Cannot open output file" << endl;
        return 0;
    }

    std::vector<key_type> data = read_file(input_file.c_str());

    key_type num = data.size();
#ifdef SPDLOG_STATS
    spdlog::info("# entries in data file = {}", num);
    spdlog::info("size of data element = {}", sizeof(data[0]));
#else
    cout << "# entries in data file = " << num << endl;
    cout << "size of data element = " << sizeof(data[0]) << endl;
#endif
    int j = 0;

    key_type progress_counter = 0;
    key_type workload_size = n;
    for (key_type i = 0; i < n; i++, progress_counter++) {
        tree.osmInsert(data[i] + 1, data[i] + 1);
    }

    int cap = tree.getOsmBufCap();
    int t = tree.getOsmBufSize();
    int emp = 0;
    progress_counter = 0;

    int nops = num_queries;

    OsmTreeCounters tree_insert_counters = tree.tree_counters;
    // display_stats<unsigned long>(tree_insert_counters, "Tree Insert Stats");

    OsmBufferCounters buffer_insert_counters = tree.getBufferCounters();
    // display_stats<unsigned long>(buffer_insert_counters, "Tree Buffer
    // Stats");

#ifdef OSMTIMER
#ifdef SPDLOG_STATS
    spdlog::info("Time taken for insert (nanoseconds) = {}",
                 tree.osmTimer.insert_time);
#else
    cout << "time in nanoseconds for insert = " << tree.osmTimer.insert_time
         << endl;
#endif
#endif

    key_type x = 0;
    progress_counter = 0;
    for (int i = 0; i < nops; i++, progress_counter++) {
        uint query_index = (rand() % n) + 1;
        bool flag = tree.osmQuery(query_index);
        if (!flag) {
            x++;
        }
    }
#ifdef OSMTIMER
#ifdef SPDLOG_STATS
    spdlog::info("Time taken for point query (nanoseconds) = {}",
                 tree.osmTimer.point_query_time);
    spdlog::info("Keys not found = {}", x);
#else
    cout << "time in nanoseconds for point query = "
         << tree.osmTimer.point_query_time << endl;
    cout << "Not found keys = " << x << endl;
#endif
#endif

#ifdef VALIDATE
    x = 0;
    for (int i = 0; i < n; i++) {
        bool flag = tree.osmQuery(data[i] + 1);
        if (!flag) {
            x++;
        }
    }
#ifdef SPDLOG_STATS
    spdlog::info("Keys not found = {}", x);
#else
    cout << "Not found keys = " << x << endl;
#endif
#endif

    OsmBufferCounters read_counters = tree.getBufferCounters();
    // display_stats(read_counters, "Query Stats");

    std::cout << tree << std::endl;

#ifdef OSMTIMER
    outfile << k << "," << l << "," << n << "," << tree.osmTimer.insert_time
            << "," << num_queries << "," << tree.osmTimer.point_query_time
            << endl;
#endif
    outfile.close();
    return 0;
}