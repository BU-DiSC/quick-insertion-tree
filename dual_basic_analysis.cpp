#include <iostream>
#include <fstream>
#include "betree.h"
#include "dual_tree.h"
#include "file_reader.h"

using namespace std; 

void dual_tree_test(const std::string input_file_path, const std::string config_file_path)
{
    FileReader fr = FileReader(input_file_path);
    std::vector<int> data_set = fr.read();

    auto start = std::chrono::high_resolution_clock::now();
    dual_tree<int, int> dt("tree_1", "tree_2", config_file_path);
    dual_tree<int, int>::show_tree_knobs();
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
    auto load_time = duration.count();
    std::cout << "Data Load time For dual tree(us):" << load_time << std::endl;
    // std::cout << "Sorted tree size: " << dt.sorted_tree_size() << std::endl;
    // std::cout << "Unsorted tree size: " << dt.unsorted_tree_size() << std::endl;
    dt.display_stats();

    /** write to csv file
     *  csv file header would be:
     *  input_file, config_file, data_load_time, sorted_tree_size, unsorted_tree_size, 
     * sorted_tree_height, unsorted_tree_height, # of writes(sorted), # of writes(unsorted), call_times(lazy move)
    */ 
    std::ofstream csvfile;
    csvfile.open("dualtree_basic.csv", ofstream::app);
    csvfile << input_file_path << ",";
    csvfile << config_file_path << ",";
    csvfile << load_time << ",";
    csvfile << dt.get_stats() << "\n";
    csvfile.close();

}

int main(int argc, char **argv)
{
    if(argc < 3)
    {
        std::cout<< "Usage: ./dual_basic_analysis <input_file> <config_file>" << std::endl;
        return -1;
    }
    // read arguments
    std::string input_file = argv[1];
    std::string config_path = argv[2];

    dual_tree_test(input_file, config_path);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    return 0;
}