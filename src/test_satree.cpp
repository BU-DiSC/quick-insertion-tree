#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <unordered_set>

#include "bptree/config.h"
#include "sware/satree.h"
#include "sware/swareBuffer.h"

using namespace std;

typedef unsigned long key_type;

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
        cout << "Invalid no. of arguments" << endl;
        cout << "Use: ./test_osmtree <input_workload_file> <output_file> "
                "<num_nodes_in_buffer_pool_cache> <k> <l> <num_inserts> "
                "<num_entries_osmBuffer_can_hold> <fill_factor_percentage> "
                "<num_queries>"
             << endl;
        return 0;
    }

    cout << "Input file name = " << input_file << endl;
    cout << "Output file name = " << output_file << endl;
    double fill_factor = fill_factor_p / 100.0;

    const char *config_file = "config.toml";
    const char *tree_dat = "tree.dat";

    Config conf(config_file);
    BlockManager manager(tree_dat, 2000000);
    cout << "blocks_in_memory = " << conf.blocks_in_memory << endl;
    OsmTree<unsigned long, unsigned long> tree(
        cmp, manager, num_entries_buffer_can_hold, fill_factor);

    long int size = 0;

    ofstream outfile(output_file, ios::out | ios::app);
    if (!outfile) {
        cout << "Cannot open output file!" << endl;
        return 0;
    }

    std::vector<key_type> data = read_file(input_file.c_str());

    key_type num = data.size();
    cout << "Num inserts = " << num << endl;
    int j = 0;

    cout << "Size of one data element =" << sizeof(data[0]) << endl;
    // exit(0);

    cout << "\n\t\t********** Inserts **********" << endl;
    // insert into buffer

    key_type progress_counter = 0;
    key_type workload_size = n;
    for (key_type i = 0; i < n; i++, progress_counter++) {
        tree.osmInsert(data[i] + 1, data[i] + 1);
        // tree.osmInsert(i + 1, i + 1);
    }

    int cap = tree.getOsmBufCap();
    int t = tree.getOsmBufSize();
    int emp = 0;
    progress_counter = 0;

    int nops = num_queries;

    OsmBufferCounters counters = tree.getBufferCounters();
    cout << "Top inserts = " << top_inserts << endl;
    cout << "Num flushes = " << num_flushes << endl;
    cout << "Total number of sort attempts = " << counters.total_sort << endl;
    cout << "Number of quick sort = " << counters.num_quick << endl;
    cout << "Num adaptive sort = " << counters.num_adaptive << endl;
    cout << "Number of times Adaptive Sort failed = "
         << counters.num_adaptive_fail << endl;
#ifdef OSMTIMER
    cout << "Time in nanoseconds for insert = " << tree.osmTimer.insert_time
         << endl;
#endif

    cout << "\n\t\t********** Point QUERY **********" << endl;
    key_type x = 0;
    progress_counter = 0;
    // for (int i = 0; i < nops; i++, progress_counter++) {
    //     uint query_index = (rand() % n) + 1;
    //     bool flag = tree.osmQuery(query_index);
    //     if (!flag) {
    //         x++;
    //     }
    // }
    for (int i = 0; i < n; i++) {
        bool flag = tree.osmQuery(data[i] + 1);
        if (!flag) {
            x++;
        }
    }
    cout << "Not found keys = " << x << endl;

#ifdef OSMTIMER
    cout << "Time in nanoseconds for query = " << tree.osmTimer.point_query_time
         << endl;
#endif

    counters = tree.getBufferCounters();

    cout << "----------------- \t Important Query Metrics \t -----------------"
         << endl;

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

#ifdef OSMTIMER
    outfile << k << "," << l << "," << n << "," << tree.osmTimer.insert_time
            << "," << num_queries << "," << tree.osmTimer.point_query_time
            << endl;
#endif
    outfile.close();
    return 0;
}