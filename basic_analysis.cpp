#include <iostream>
#include "betree.h"
#include "dual_tree.h"

using namespace std; 

int mixed_workload_test(string input_file, string output_file, int num_queries, int perc_load, int n){
    dual_tree<int,int> tree = dual_tree<int,int>();
    long int size = 0;
    int *data;

    ifstream infile(input_file, ios::in | ios::binary);
    if (!infile)
    {
        cout << "Cannot open file!" << endl;
        return 0;
    }

    ofstream outfile(output_file, ios::out | ios::app);
    if (!outfile)
    {
        cout << "Cannot open output file!" << endl;
        return 0;
    }

    FILE *file = fopen(input_file.c_str(), "rb");
    if (file == NULL)
        return 0;

    fseek(file, 0, SEEK_END);
    size = ftell(file);
    fclose(file);

    cout << "size = " << size << endl;

    data = new int[size / sizeof(int)];
    infile.read((char *)data, size);
    infile.close();

    int num = size / sizeof(int);

    
    int j = 0;

    cout << "Size of one data element =" << sizeof(data[0]) << endl;

    int num_load = (int)((perc_load / 100.0) * n);

    cout << "Loading Tree with " << perc_load << "% Data" << endl;
    auto start_l = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_load; i++)
    {
        tree.insert(data[i] + 1, data[i] + 1);
        if (num_load < 100)
            num_load = 100;
    }
    auto stop_l = std::chrono::high_resolution_clock::now();
    auto duration_l = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_l - start_l).count();

    cout << "Time in nanoseconds for loading = " << duration_l << endl;
    // OsmBufferCounters counters = tree.getBufferCounters();

    // cout << "\tTime in nanoseconds for sorting after Loading = " << counters.sort_buffer_time << endl;
    // cout << "\tTime in nanoseconds for general sorting after Loading = " << counters.gen_sort_time << endl;
    // cout << "\tTime in nanoseconds for merge sorting after Loading = " << counters.merge_sort_time << endl;

    // shuffle indexes
    srand(time(0));
    std::random_device r;
    std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
    std::mt19937 eng(seed);

    srand((unsigned)time(NULL));

    int existing_domain = num_load;

    int tot_inserts = num_load, tot_queries = 0;
    int i = num_load;
    int q = 0;
    int empty_queries = 0;
    int num_oper = 0;

    cout << "Num Queries = " << num_queries << endl;
    cout << "\nStarting Operations" << endl;
    unsigned long long only_insert_time = 0;
    unsigned long long only_query_time = 0;

    auto start = std::chrono::high_resolution_clock::now();

    int progress_counter = 0;
    int tot_iter = n - num_load + num_queries;
    while (true)
    {
        assert(num_oper <= (n - num_load + num_queries));
        if (tot_inserts < n && tot_queries < num_queries)
        {
            double p = (double)rand() / RAND_MAX;
            if (p < 0.5)
            {
                auto start_ins = std::chrono::high_resolution_clock::now();
                tree.insert(data[i] + 1, data[i] + 1);
                auto stop_ins = std::chrono::high_resolution_clock::now();
                only_insert_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop_ins - start_ins).count();
                i++;
                tot_inserts++;
            }
            else
            {
                // pick a number between 1 and tot_inserts
                int query_index = (rand() % tot_inserts) + 1;
                auto start_q = std::chrono::high_resolution_clock::now();

                bool res = tree.query(query_index);
                auto stop_q = std::chrono::high_resolution_clock::now();
                only_query_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop_q - start_q).count();
                q++;
                tot_queries++;
                if (!res)
                {
                    empty_queries++;
                }
            }
            num_oper++;
        }
        else if (tot_inserts < n && tot_queries >= num_queries)
        {
            auto start_ins = std::chrono::high_resolution_clock::now();
            tree.insert(data[i] + 1, data[i] + 1);
            auto stop_ins = std::chrono::high_resolution_clock::now();
            only_insert_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop_ins - start_ins).count();
            i++;
            tot_inserts++;
            num_oper++;
        }
        else if (tot_inserts >= n && tot_queries <= num_queries)
        {
            // pick a number between 1 and tot_inserts
            int query_index = (rand() % tot_inserts) + 1;
            auto start_q = std::chrono::high_resolution_clock::now();

            bool res = tree.query(query_index);
            auto stop_q = std::chrono::high_resolution_clock::now();
            only_query_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop_q - start_q).count();
            j++;
            tot_queries++;
            if (!res)
            {
                empty_queries++;
            }
            num_oper++;
        }

        if (tot_inserts >= n && tot_queries >= num_queries)
        {
            cout << "Tot inserts = " << tot_inserts << endl;
            cout << "Tot queries = " << tot_queries << endl;
            break;
        }

    }

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();

    cout << "Time in nanoseconds for all operations = " << duration << endl;
    cout << "Time in nanoseconds for only inserts = " << only_insert_time << endl;
    cout << "Time in nanoseconds for only queries = " << only_query_time << endl;

    cout << "Total Elements Inserted Including Load = " << tot_inserts << endl;
    cout << "Total Point Queries Executed           = " << tot_queries << endl;
    cout << "Total Empty Queries                    = " << empty_queries << endl;
    cout << "Total Number of Operations             = " << num_oper << endl;
    return 1;
}

void dual_tree_test(const std::vector<int>& data_set)
{
    auto start = std::chrono::high_resolution_clock::now();
    dual_tree<int, int> dt;
    int idx = 0;
    int cnt = 0;
    for(int i: data_set){
        if(idx == i) {
            cnt += 1;
        }
        idx += 1;
    }
    std::cout << "Number of keys in sorted position: " << cnt << std::endl;
    idx = 0;
    for(int i: data_set)
    {
        dt.insert(i, idx++); 
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

    std::cout << "Data Load time For dual tree(us):" << duration.count() << std::endl;
    std::cout << "Sorted tree size: " << dt.sorted_tree_size() << std::endl;
    std::cout << "Unsorted tree size: " << dt.unsorted_tree_size() << std::endl;
    dt.fanout();

}

void b_plus_tree_test(const std::vector<int>& data_set)
{
    // init a tree that takes integers for both key and value
    // the first argument is the name of the block manager for the cache (can be anything)
    // second argument is the directory where the block manager will operate (if changing this, make sure to update makefile)
    // 3rd argument is the size of every block in bytes. Suggest to keep it as is since 4096B = 4KB = 1 page
    // 4th argument is the number of blocks to store in memory. Right now, it is at 10000 blocks = 10000*4096 bytes = 40.96 MB
    // potentially, the only argument to change would be the last one (4th arg) to increase the memory capacity

    auto start = std::chrono::high_resolution_clock::now();
    BeTree<int,int> tree("manager", "./tree_dat", BeTree_Default_Knobs<int, int>::BLOCK_SIZE,
        BeTree_Default_Knobs<int, int>::BLOCKS_IN_MEMORY);

    int idx = 0;
    for(int i: data_set)
    {
        tree.insert(i, idx++);
    }

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

    std::cout << "Data Load time For b plus tree(us):" << duration.count() << std::endl;

}
int main(int argc, char **argv)
{
    if(argc < 2)
    {
        std::cout<< "Usage: ./main <input_file>" << std::endl;
    }

    // Read the input file
    std::string input_file = argv[1];
    std::ifstream ifs;
    std::vector<int> data;

    ifs.open(input_file);
    ifs.seekg(0, std::ios::end);
    size_t filesize = ifs.tellg();
    ifs.seekg(0, std::ios::beg);

    data.resize(filesize / sizeof(int));
    ifs.read((char*)data.data(), filesize);

    dual_tree<int, int>::show_tree_knobs();

    dual_tree_test(data);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    b_plus_tree_test(data);
    return 0;
}