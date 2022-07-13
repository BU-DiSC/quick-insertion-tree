#include <iostream>
#include "betree.h"
#include "dual_tree.h"
#include "file_reader.h"

using namespace std; 

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
        std::cout<< "Usage: ./bptree_basic_analysis <input_file>" << std::endl;
    }

    // Read the input file
    std::string input_file = argv[1];
    FileReader fr = FileReader(input_file);
    std::vector<int> data = fr.read();

    b_plus_tree_test(data);
    return 0;
}