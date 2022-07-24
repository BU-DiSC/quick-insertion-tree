#include <iostream>
#include "betree.h"
#include "dual_tree.h"
#include "file_reader.h"

using namespace std; 

void dual_tree_test(const std::vector<int>& data_set, const std::string config_file_path)
{
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

    std::cout << "Data Load time For dual tree(us):" << duration.count() << std::endl;
    std::cout << "Sorted tree size: " << dt.sorted_tree_size() << std::endl;
    std::cout << "Unsorted tree size: " << dt.unsorted_tree_size() << std::endl;
    dt.display_stats();

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
    FileReader fr = FileReader(input_file);
    std::vector<int> data = fr.read();

    dual_tree_test(data, config_path);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    return 0;
}