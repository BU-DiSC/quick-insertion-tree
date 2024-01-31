#ifndef MINHEAP_INCLUDE
#define MINHEAP_INCLUDE

#include <iostream>
#include <climits>

void swapElements(std::pair<int, int> *x, std::pair<int, int> *y);

class MinHeap
{
    std::pair<int,int> *harr;
    int capacity;
    int heap_size;

public:
    MinHeap(int capacity);

    void MinHeapify(int);

    int parent(int i) { return (i - 1) / 2; }

    // to get index of left child of node at index i
    int left(int i) { return (2 * i + 1); }

    // to get index of right child of node at index i
    int right(int i) { return (2 * i + 2); }

    // to extract the root which is the minimum element
    std::pair<int,int> extractMin();

    // Decreases key value of key at index i to new_val
    void decreaseKey(int i, std::pair<int,int> new_val);

    // Returns the minimum key (key at root) from min heap
    std::pair<int,int> getMin() { return harr[0]; }

    // Deletes a key stored at index i
    void deleteKey(int i);

    // Inserts a new key 'k'
    bool insertKey(std::pair<int,int> k);

    int size()
    {
        return heap_size;
    }
};

MinHeap::MinHeap(int cap)
{
    heap_size = 0;
    capacity = cap;
    harr = new std::pair<int, int>[cap];
}

bool MinHeap::insertKey(std::pair<int,int> k)
{
    if (heap_size == capacity)
    {
        // std::cout << "\nOverflow: Could not insertKey\n"<<"Heap size = "<<heap_size<<"\tCapacity = "<<capacity<<"\n";
        // std::cout << "Will switch to "
        return false;
    }

    // First insert the new key at the end
    heap_size++;
    int i = heap_size - 1;
    harr[i] = k;

    // Fix the min heap property if it is violated
    while (i != 0 && harr[parent(i)] > harr[i])
    {
        swapElements(&harr[i], &harr[parent(i)]);
        i = parent(i);
    }

    return true;
}

void MinHeap::decreaseKey(int i, std::pair<int,int> new_val)
{
    harr[i] = new_val;
    while (i != 0 && harr[parent(i)] > harr[i])
    {
        swapElements(&harr[i], &harr[parent(i)]);
        i = parent(i);
    }
}

// Method to remove minimum element (or root) from min heap
std::pair<int,int> MinHeap::extractMin()
{
    if (heap_size <= 0)
        return std::make_pair(INT_MAX,INT_MAX);
    if (heap_size == 1)
    {
        heap_size--;
        return harr[0];
    }

    // Store the minimum value, and remove it from heap
    std::pair<int,int> root = harr[0];
    harr[0] = harr[heap_size - 1];
    heap_size--;
    MinHeapify(0);

    return root;
}

// This function deletes key at index i. It first reduced value to minus
// infinite, then calls extractMin()
void MinHeap::deleteKey(int i)
{
    
    decreaseKey(i, std::make_pair(INT_MIN, INT_MIN));
    extractMin();
}

// A recursive method to heapify a subtree with the root at given index
// This method assumes that the subtrees are already heapified
void MinHeap::MinHeapify(int i)
{
    int l = left(i);
    int r = right(i);
    int smallest = i;
    if (l < heap_size && harr[l].first < harr[i].first)
        smallest = l;
    if (r < heap_size && harr[r].first < harr[smallest].first)
        smallest = r;
    if (smallest != i)
    {
        swapElements(&harr[i], &harr[smallest]);
        MinHeapify(smallest);
    }
}

// A utility function to swap two elements
void swapElements(std::pair<int,int> *x, std::pair<int, int> *y)
{
    std::pair<int,int> temp = *x;
    *x = *y;
    *y = temp;
}


#endif
