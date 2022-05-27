#include <iostream>
#include "betree.h"

using namespace std; 

int main()
{
    // init a tree that takes integers for both key and value
    // the first argument is the name of the block manager for the cache (can be anything)
    // second argument is the directory where the block manager will operate (if changing this, make sure to update makefile)
    // 3rd argument is the size of every block in bytes. Suggest to keep it as is since 4096B = 4KB = 1 page
    // 4th argument is the number of blocks to store in memory. Right now, it is at 10000 blocks = 10000*4096 bytes = 40.96 MB
    // potentially, the only argument to change would be the last one (4th arg) to increase the memory capacity
    BeTree<int,int> tree("manager", "./tree_dat", 4096, 10000);

    int n = 100000;
    for(int i = 0; i < n; i++)
    {
        tree.insert(i+1, i+1);
    }

    int nops = n;

    int key = 0;
    for(int i =0; i < nops; i++)
    {
        if (!tree.query(i + 1))
        {
            cout << "Key = " << i << " Not found" << endl;
            key++;
        }
    }
    cout << "Not found Keys = " << key << endl;

    return 1; 
}