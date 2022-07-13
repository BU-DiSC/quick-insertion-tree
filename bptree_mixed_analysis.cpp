#include <iostream>
#include "betree.h"
#include "dual_tree.h"
#include "file_reader.h"


using namespace std; 

int bptree_mixed_workload_test(string input_file, int num_queries, int perc_load, int n){
    BeTree<int,int> tree("manager", "./tree_dat", BeTree_Default_Knobs<int, int>::BLOCK_SIZE,
        BeTree_Default_Knobs<int, int>::BLOCKS_IN_MEMORY);    
    // long int size = 0;
    // int *data;

    // ifstream infile(input_file, ios::in | ios::binary);
    // if (!infile)
    // {
    //     cout << "Cannot open file!" << endl;
    //     return 0;
    // }

    // // ofstream outfile(output_file, ios::out | ios::app);
    // // if (!outfile)
    // // {
    // //     cout << "Cannot open output file!" << endl;
    // //     return 0;
    // // }
    // FILE *file = fopen(input_file.c_str(), "rb");
    // if (file == NULL)
    //     return 0;

    // fseek(file, 0, SEEK_END);
    // size = ftell(file);
    // fclose(file);

    // cout << "size = " << size << endl;
    // data.resize(size / sizeof(int));
    // ifs.read((char*)data.data(), size);
    // // data = new int[size / sizeof(int)];
    // // infile.read((char *)data, size);
    // infile.close();


    // std::ifstream infile;
    // std::vector<int> data;
    // infile.open(input_file);
    // infile.seekg(0, std::ios::end);
    // size_t size = infile.tellg();
    // infile.seekg(0, std::ios::beg);
    // data.resize(size / sizeof(int));
    // infile.read((char*)data.data(), size);
    // infile.close();

    FileReader fr = FileReader(input_file);
    std::vector<int> data = fr.read();

    // int num = size / sizeof(int);

    
    int j = 0;

    // cout << "Size of one data element =" << sizeof(data[0]) << endl;

    int num_load = (int)((perc_load / 100.0) * n);

    cout << "Loading Tree with " << perc_load << "% Data" << endl;
    auto start_l = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_load; i++)
    {
        cout << "loaded " << data[i]+1 <<endl;
        tree.insert(data[i], i+1);
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
                tree.insert(data[i] + 1, i);
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


int main(int argc, char **argv)
{
    if(argc < 5)
    {
        std::cout<< "Usage: ./bptree_mixed_analysis <input_file> <num_queries> <perc_load> <n>" << std::endl;
    }

    // Read the input file
    std::string input_file = argv[1];
    int num_queries = atoi(argv[2]);
    int perc_load = atoi(argv[3]);
    int n = atoi(argv[4]);
    std::ifstream ifs;
    std::vector<int> data;

    // ifs.open(input_file);
    // ifs.seekg(0, std::ios::end);
    // size_t filesize = ifs.tellg();
    // ifs.seekg(0, std::ios::beg);

    // data.resize(filesize / sizeof(int));
    // ifs.read((char*)data.data(), filesize);

    bptree_mixed_workload_test(input_file, num_queries, perc_load, n);
    
    return 0;
}