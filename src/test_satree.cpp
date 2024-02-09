#include <spdlog/spdlog.h>

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

void print_stats(OsmBufferCounters insert_counters, OsmBufferCounters counters,
                 key_type nops) {
#ifdef TABULATE_STATS
    tabulate::Table insert_stats;
    insert_stats.add_row({"Top inserts", to_string(top_inserts)});
    insert_stats.add_row({"# flushes", to_string(num_flushes)});
    insert_stats.add_row(
        {"Total # sort attempts", to_string(insert_counters.total_sort)});
    insert_stats.add_row(
        {"# quick sort", to_string(insert_counters.num_quick)});
    insert_stats.add_row(
        {"# adaptive sort", to_string(insert_counters.num_adaptive)});
    insert_stats.add_row({"# adaptive sort failures",
                          to_string(insert_counters.num_adaptive_fail)});

    std::cout << insert_stats << std::endl;

    tabulate::Table query_stats;
    query_stats.add_row({"# point lookups", to_string(nops)});
    query_stats.add_row({"SWARE Buffer"});
    query_stats[1]
        .format()
        .font_style({FontStyle::bold})
        .font_align(FontAlign::center);

    // SWARE buffer stats nested table
    Table sware_buffer_stats;
    sware_buffer_stats.add_row(
        {"# OSM Fence Pointer Scans", to_string(counters.osm_fence_queries)});
    sware_buffer_stats.add_row({"# OSM Fence Pointer (Positive)",
                                to_string(counters.osm_fence_positive)});
    sware_buffer_stats.add_row({"# Unsorted Section Zonemap Queries",
                                to_string(counters.unsorted_zonemap_queries)});
    sware_buffer_stats.add_row({"# Unsorted Section Zonemap (Positive)",
                                to_string(counters.unsorted_zonemap_positive)});
    sware_buffer_stats.add_row(
        {"# Sequential Scans", to_string(counters.num_seq_scan)});
    sware_buffer_stats.add_row({"# Global Bloom Filter Queries",
                                to_string(counters.global_bf_queried)});
    sware_buffer_stats.add_row({"# Global Bloom Filter (Positive)",
                                to_string(counters.global_bf_positive)});
    sware_buffer_stats.add_row(
        {"# Zones Queried", to_string(counters.num_zones_queried)});
    sware_buffer_stats.add_row(
        {"# Zones (Positive)", to_string(counters.num_zones_positive)});
    sware_buffer_stats.add_row(
        {"# Per-Page BFs Queried", to_string(counters.sublevel_bf_queried)});
    sware_buffer_stats.add_row({"# Per-Page BFs (Positive)",
                                to_string(counters.sublevel_bf_positive)});
    sware_buffer_stats.add_row(
        {"# Pages Scanned Sequentially", to_string(counters.seq_zone_queries)});
    sware_buffer_stats.add_row(
        {"# Pages (Positive)", to_string(counters.seq_zone_positive)});
    sware_buffer_stats.add_row({"# Keys found in seq scan of unsorted section",
                                to_string(counters.num_seq_found)});
    sware_buffer_stats.add_row(
        {"# Interpolation Search Queries",
         to_string(counters.subblock_interpolation_queries)});
    sware_buffer_stats.add_row(
        {"# Interpolation Search (Positive)",
         to_string(counters.subblock_interpolation_positive)});
    sware_buffer_stats.add_row({"# Keys found in unsorted section",
                                to_string(counters.num_unsorted_positive)});
    sware_buffer_stats.add_row({"# Sorted Section Zonemap Queries",
                                to_string(counters.sorted_zonemap_queries)});
    sware_buffer_stats.add_row({"# Sorted Section Zonemap (Positive)",
                                to_string(counters.sorted_zonemap_positive)});
    sware_buffer_stats.add_row(
        {"# Interpolation Search Queries (Sorted Section)",
         to_string(counters.num_bin_scan)});
    sware_buffer_stats.add_row(
        {"# Interpolation Search (Positive) (Sorted Section)",
         to_string(counters.num_bin_found)});

    sware_buffer_stats[0].format().hide_border_top();
    query_stats.add_row({sware_buffer_stats});
    query_stats.add_row({"B+-Tree"});
    query_stats[3]
        .format()
        .font_style({FontStyle::bold})
        .font_align(FontAlign::center);
    Table tree_stats;
    tree_stats.add_row(
        {"# Tree Zonemap Queries", to_string(tree_zone_queries)});
    tree_stats.add_row(
        {"# Tree Zonemap (Positive)", to_string(tree_zone_positive)});
    tree_stats.add_row({"# Tree Queries", to_string(num_tree_scan)});
    tree_stats.add_row({"# Tree (Positive)", to_string(num_tree_found)});
    tree_stats[0].format().hide_border_top();
    query_stats.add_row({tree_stats});

    std::cout << query_stats << std::endl;
#elif defined(SPDLOG_STATS)
    spdlog::info("Printing insert stats ----------------------- ");
    spdlog::info("Top inserts = {}", top_inserts);
    spdlog::info("Num flushes = {}", num_flushes);
    spdlog::info("Total number of sort attempts = {}",
                 insert_counters.total_sort);
    spdlog::info("Number of quick sort = {}", insert_counters.num_quick);
    spdlog::info("Num adaptive sort = {}", insert_counters.num_adaptive);
    spdlog::info("Number of times Adaptive Sort failed = {}",
                 insert_counters.num_adaptive_fail);

    spdlog::info("Printing query stats ----------------------- ");
    spdlog::info("#. of Point Lookups = {}", nops);
    spdlog::info("#. of OSM Fence Pointer Scans = {}",
                 counters.osm_fence_queries);
    spdlog::info(
        "#. times of OSM Fence Pointer returned yes (#. of times "
        "query went into buffer) = {}",
        counters.osm_fence_positive);
    spdlog::info("#. of times Unsorted Section Zonemap queried = {}",
                 counters.unsorted_zonemap_queries);
    spdlog::info("#. of times unsorted section zonemap answered yes = {}",
                 counters.unsorted_zonemap_positive);
    spdlog::info("#. of Sequential Scans = {}", counters.num_seq_scan);
    spdlog::info("#. of Global Bloom filter Queries = {}",
                 counters.global_bf_queried);
    spdlog::info("#. times Global Bloom Filter returned yes = {}",
                 counters.global_bf_positive);
    spdlog::info("#. of Zones Queried = {}", counters.num_zones_queried);
    spdlog::info("#. of Zones returned yes = {}", counters.num_zones_positive);
    spdlog::info("#. of times Sublevel BFs queried = {}",
                 counters.sublevel_bf_queried);
    spdlog::info("#. of times Sublevel BFs returned yes = {}",
                 counters.sublevel_bf_positive);
    spdlog::info("#. of Pages Scanned through sequential search = {}",
                 counters.seq_zone_queries);
    spdlog::info("#. of pages with positive result = {}",
                 counters.seq_zone_positive);
    spdlog::info("#. of Keys found in seq scan of unsorted section = {}",
                 counters.num_seq_found);
    spdlog::info("#. of interpolation search queries (post sorting subblocks) ",
                 "in unsorted section = {}",
                 counters.subblock_interpolation_queries);
    spdlog::info("#. of keys found through interpolation search in unsorted ",
                 "section = {}", counters.subblock_interpolation_positive);
    spdlog::info("#. of keys found in the unsorted section of the buffer ",
                 "(section includes subsorted blocks for queries) = {}",
                 counters.num_unsorted_positive);
    spdlog::info("#. of times sorted section zonemap queried = {}",
                 counters.sorted_zonemap_queries);
    spdlog::info("#. of times sorted section zonemap answered yes = {}",
                 counters.sorted_zonemap_positive);
    spdlog::info("#. of times interpolation search performed = {}",
                 counters.num_bin_scan);
    spdlog::info("#. of times interpolation search yielded positive result ",
                 "(number of keys found in sorted section) = {}",
                 counters.num_bin_found);

    spdlog::info("Printing tree stats ----------------------- ");
    spdlog::info("#. of times Tree's Zonemap was queried = {}",
                 tree_zone_queries);
    spdlog::info("#. of times tree's zonemap returned yes = {}",
                 tree_zone_positive);
    spdlog::info("#. of times Tree was queried = {}", num_tree_scan);
    spdlog::info(
        "#. of times tree scan returned positive (#. of keys found in ",
        "tree) = {}", num_tree_found);

#else
    cout << "----------------- \t Insert Metrics \t -----------------" << endl;
    cout << "Top inserts = " << top_inserts << endl;
    cout << "Num flushes = " << num_flushes << endl;
    cout << "Total number of sort attempts = " << insert_counters.total_sort
         << endl;
    cout << "Number of quick sort = " << insert_counters.num_quick << endl;
    cout << "Num adaptive sort = " << insert_counters.num_adaptive << endl;
    cout << "Number of times Adaptive Sort failed = "
         << insert_counters.num_adaptive_fail << endl;

    cout << "----------------- \t Query Metrics \t -----------------" << endl;

    cout << "#. of Point Lookups = " << nops << endl;
    cout << "#. of OSM Fence Pointer Scans = " << counters.osm_fence_queries
         << endl;
    cout << "#. times of OSM Fence Pointer returned yes (#. of times query "
            "went into buffer) = "
         << counters.osm_fence_positive << endl;
    cout << "#. of times Unsorted Section Zonemap queried = "
         << counters.unsorted_zonemap_queries << endl;
    cout << "#. of times unsorted section zonemap answered yes = "
         << counters.unsorted_zonemap_positive << endl;
    cout << "#. of Sequential Scans = " << counters.num_seq_scan << endl;
    cout << "#. of Global Bloom filter Queries = " << counters.global_bf_queried
         << endl;
    cout << "#. times Global Bloom Filter returned yes = "
         << counters.global_bf_positive << endl;
    cout << "#. of Zones Queried = " << counters.num_zones_queried << endl;
    cout << "#. of Zones returned yes = " << counters.num_zones_positive
         << endl;
    cout << "#. of times Sublevel BFs queried = "
         << counters.sublevel_bf_queried << endl;
    cout << "#. of times Sublevel BFs returned yes = "
         << counters.sublevel_bf_positive << endl;
    cout << "#. of Pages Scanned through sequential search = "
         << counters.seq_zone_queries << endl;
    cout << "#. of pages with positive result = " << counters.seq_zone_positive
         << endl;
    cout << "#. of Keys found in seq scan of unsorted section = "
         << counters.num_seq_found << endl;
    cout << "#. of interpolation search queries (post sorting subblocks) in "
            "unsorted section = "
         << counters.subblock_interpolation_queries << endl;
    cout << "#. of keys found through interpolation search in unsorted section "
            "= "
         << counters.subblock_interpolation_positive << endl;
    cout << "#. of keys found in the unsorted section of the buffer (section "
            "includes subsorted blocks for queries) = "
         << counters.num_unsorted_positive << endl;
    cout << endl;

    cout << "#. of times sorted section zonemap queried = "
         << counters.sorted_zonemap_queries << endl;
    cout << "#. of times sorted section zonemap answered yes = "
         << counters.sorted_zonemap_positive << endl;
    cout << "#. of times interpolation search performed = "
         << counters.num_bin_scan << endl;
    cout << "#. of times interpolation search yielded positive result (number "
            "of keys found in sorted section) ="
         << counters.num_bin_found << endl;
    cout << endl;

    cout << "#. of times Tree's Zonemap was queried = " << tree_zone_queries
         << endl;
    cout << "#. of times tree's zonemap returned yes = " << tree_zone_positive
         << endl;
    cout << "#. of times Tree was queried = " << num_tree_scan << endl;
    cout << "#. of times tree scan returned positive (#. of keys found in "
            "tree) = "
         << num_tree_found << endl
         << endl;
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
    // auto start = std::chrono::high_resolution_clock::now();
    for (key_type i = 0; i < n; i++, progress_counter++) {
        tree.osmInsert(data[i] + 1, data[i] + 1);
    }
    // auto end = std::chrono::high_resolution_clock::now();
    // auto duration =
    //     std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    // spdlog::info("Insert duration = {}", duration.count());

    int cap = tree.getOsmBufCap();
    int t = tree.getOsmBufSize();
    int emp = 0;
    progress_counter = 0;

    int nops = num_queries;

    OsmBufferCounters insert_counters = tree.getBufferCounters();

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

    print_stats(insert_counters, read_counters, nops);

    std::cout << tree << std::endl;

#ifdef OSMTIMER
    outfile << k << "," << l << "," << n << "," << tree.osmTimer.insert_time
            << "," << num_queries << "," << tree.osmTimer.point_query_time
            << endl;
#endif
    outfile.close();
    return 0;
}