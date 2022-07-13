#ifndef BETREE_H
#define BETREE_H

#include <iostream>
#include <math.h>
#include <cassert>
#include <algorithm>
#include <string.h>
#include <queue>
#include <chrono>
#include <memory>
#include <vector>

#include "block_manager.h"
#include "serializable.h"

#define BE_MAX(a, b) ((a) < (b) ? (b) : (a))

// #define BETREE_FRIENDS

// Defining all required tuning knobs/sizes for the tree
template <typename _Key, typename _Value>
class BeTree_Default_Knobs
{
public:
    // Epsilon value
    static constexpr double EPSILON = 0.5;

    // size of every block in Bytes
    static const int BLOCK_SIZE = 4096;

    // size of metadata in a node
    static const int METADATA_SIZE = 40;

    // leftover data size after deducting space for metadata
    static const int DATA_SIZE = BLOCK_SIZE - METADATA_SIZE;

    // size of every leaf in Bytes
    static const int LEAF_SIZE = DATA_SIZE;

    // number of data pairs that the tree will hold per leaf
    static const int NUM_DATA_PAIRS = (LEAF_SIZE - sizeof(int)) / (sizeof(_Key) + sizeof(_Value));

    // size of a key-value pair unit
    static const int UNIT_SIZE = sizeof(_Key *) + sizeof(_Value *);

// number of buffer elements that can be held (at max)
// equal to (Buffer size - Buffer metadata size)/sizeof(pair)
#ifdef BPLUS
    static const int NUM_UPSERTS = 1;
    // Buffer size  = sizeof(pair) + sizeof(int metadata)
    // static const int BUFFER_SIZE = (2 * sizeof(std::pair<_Key, _Value>)) + sizeof(int);
    static const int BUFFER_SIZE = (sizeof(std::pair<_Key, _Value>)) + sizeof(int);
    static const int PIVOT_SIZE = DATA_SIZE - BUFFER_SIZE;
#else
    // size of pivots in Bytes
    // Number of keys = Block_size/sizeof(key)
    // Pivot size = ((Number of keys)^Epsilon) * sizeof(key)
    // static const int PIVOT_SIZE = pow((DATA_SIZE / sizeof(_Key)), EPSILON) * sizeof(_Key);
    static const int PIVOT_SIZE = int(pow(NUM_DATA_PAIRS, EPSILON)) * UNIT_SIZE;

    // size of Buffer in Bytes
    static const int BUFFER_SIZE = DATA_SIZE - PIVOT_SIZE;
    // static const int NUM_UPSERTS = (BUFFER_SIZE - sizeof(int)) / (2 * sizeof(std::pair<_Key, _Value>));
    static const int NUM_UPSERTS = (BUFFER_SIZE - sizeof(int)) / sizeof(std::pair<_Key, _Value>);
    // static const int NUM_UPSERTS = (BUFFER_SIZE - sizeof(int)) / (sizeof(_Key *) + sizeof(_Value *));
#endif

    // static const int S = (PIVOT_SIZE - sizeof(_Key *)) / (sizeof(uint *) + sizeof(_Key *));
    static const int S = (PIVOT_SIZE - sizeof(_Key)) / (sizeof(uint) + sizeof(_Key));
    // number of pivots for every node in the tree
    // static const int NUM_PIVOTS = (PIVOT_SIZE / (sizeof(_Key *) + sizeof(_Key *))) - 1;
    static const int NUM_PIVOTS = S;

    // number of children for every node
    static const int NUM_CHILDREN = NUM_PIVOTS + 1;

// internal node flush threshold
// the internal node will flush elements (equal to flush threshold)
// of its buffer once full
#ifdef BPLUS
    static const int FLUSH_LIMIT = 1;
    static const int LEAF_FLUSH_LIMIT = 1;
#else
    static const int FLUSH_LIMIT = NUM_UPSERTS;
    static const int LEAF_FLUSH_LIMIT = NUM_UPSERTS;
#endif

    static const int BLOCKS_IN_MEMORY = 500000;
};

// structure that holds all stats for the tree

struct BeTraits
{
    int internal_flushes = 0;
    int leaf_flushes = 0;
    int internal_splits = 0;
    int leaf_splits = 0;
    int num_blocks = 0;

    int max_fanout = 0;
    int average_fanout = 0;

    int max_buffer_occupancy = 0;
    int average_buffer_occupancy = 0;
    int empty_buffer_nodes = 0;
    int num_nodes = 0;

    int num_leaf_nodes = 0;
    int num_internal_nodes = 0;

    int min_fanout = 0;
    int min_buffer_occupancy = 0;

    int median_fanout = 0;
    int median_buffer_occupancy = 0;

    int min_leaf_occupancy = 0;
    int max_leaf_occupancy = 0;
    int avg_leaf_occupancy = 0;

    int num_gr_80 = 0;
    int num_50_80 = 0;
    int num_20_50 = 0;
    int num_le_20 = 0;

#ifdef IO
    unsigned long io_pointquery = 0;
    unsigned long io_rangequery = 0;
    unsigned long io_insert = 0;

    unsigned long io_max_pointquery = 1;
    unsigned long io_min_pointquery = 1;

    unsigned long io_max_rangequery = 1;
    unsigned long io_min_rangequery = 1;

    unsigned long io_max_insert = 0;
    unsigned long io_min_insert = 2;
#endif
};

#ifdef TIMER
struct BeTimer
{
    unsigned long insert_time = 0;
    unsigned long point_query_time = 0;
    unsigned long range_query_time = 0;
    unsigned long bulk_load_time = 0;
};
#endif

#ifdef PROFILE
extern unsigned long open_time;
extern unsigned long flushlevel_time;
extern unsigned long flushleaf_time;
extern unsigned long flushinternal_time;
extern unsigned long prepareflush_time;
extern unsigned long splitleaf_time;
extern unsigned long splitinternal_time;

extern unsigned long find_time;
extern unsigned long extract_time;
extern unsigned long replace_time;

extern unsigned long tree_leaf_bin_search_time;
extern unsigned long tree_slot_time;
extern unsigned long tree_buf_bin_search_time;

unsigned long open_time = 0;
unsigned long flushlevel_time = 0;
unsigned long flushleaf_time = 0;
unsigned long flushinternal_time = 0;
unsigned long prepareflush_time = 0;
unsigned long splitleaf_time = 0;
unsigned long splitinternal_time = 0;

unsigned long find_time = 0;
unsigned long extract_time = 0;
unsigned long replace_time = 0;

unsigned long deserialize_time = 0;
unsigned long tree_leaf_bin_search_time = 0;
unsigned long tree_slot_time = 0;
unsigned long tree_buf_bin_search_time = 0;
#endif

enum Result
{
    SPLIT,
    NOSPLIT,
};

template <typename key_type, typename value_type>
struct compare_pair_kv
{
    bool operator()(const std::pair<key_type, value_type> &value,
                    const key_type &key)
    {
        return std::less<key_type>()(value.first, key);
    }
    bool operator()(const key_type &key,
                    const std::pair<key_type, value_type> &value)
    {
        return std::less<key_type>()(key, value.first);
    }
};

template <typename _Key, typename _Value>
bool compare_pair(const std::pair<_Key, _Value> &p1, const std::pair<_Key, _Value> &p2)
{
    return std::less<_Key>()(p1.first, p2.first);
}

// Buffer for the tree that holds the elements until full
// will flush a batch of it (equal to flush_size)
// size holds the current number of elements in the buffer
template <typename key_type, typename value_type, typename knobs = BeTree_Default_Knobs<key_type, value_type>,
          typename compare = std::less<key_type>>
struct Buffer
{
    int size;
    std::pair<key_type, value_type> buffer[knobs::NUM_UPSERTS];
    // public:
    Buffer()
    {
        size = 0;
    }
};

// Structure that holds the data in the tree leaves
// size signifies the current number of data pairs in the leaf
template <typename key_type, typename value_type, typename knobs = BeTree_Default_Knobs<key_type, value_type>,
          typename compare = std::less<key_type>>
struct Data
{
    int size;
    std::pair<key_type, value_type> data[knobs::NUM_DATA_PAIRS];

    Data()
    {
        size = 0;
    }
};

// class that defines the B Epsilon tree Node
template <typename key_type, typename value_type, typename knobs = BeTree_Default_Knobs<key_type, value_type>,
          typename compare = std::less<key_type>>
class BeNode : public Serializable
{

    uint id;
    // boolean flag if leaf
    bool *is_leaf;

    // boolean flag if root
    bool *is_root;

    // parent of node (node id)
    // BeNode<key_type, value_type, knobs, compare> *parent;
    uint *parent;

    // counter for no. of pivots
    int *pivots_ctr;

    // node's buffer
    struct Buffer<key_type, value_type, knobs, compare> *buffer;

    // // node's child pointer keys

    // // node's pivots/children
    // node's child pointer keys
    key_type *child_key_values;

    // node's pivots/children
    // pivot_pointers will now be node id's rather than actual nodes
    uint *pivot_pointers;

    // data pairs for the node if leaf
    Data<key_type, value_type, knobs, compare> *data;

    // next node pointer (node id)
    uint *next_node;

    BlockManager *manager;

public:
    // opens the node from disk/memory for access
    void open()
    {
#ifdef PROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        bool miss = false;
        Deserialize(manager->internal_memory[manager->OpenBlock(id, miss)]);
        if (miss)
        {
            if (*is_leaf)

                manager->addLeafCacheMisses();
            else

                manager->addInternalCacheMisses();
        }
        else
        {
            if (*is_leaf)

                manager->addLeafCacheHits();
            else
                manager->addInternalCacheHits();
        }

#ifdef PROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        open_time += duration.count();
#endif
    }

    void setToId(uint _id)
    {
        id = _id;
        open();
    }

    // Parameterized constructor
    /**
     *  returns: N/A
     *  Function: creates a new node set to the given id
     *              and parent as set.
     */
    BeNode(BlockManager *_manager, uint _id, uint _parent, bool _is_leaf, bool _is_root, uint _next_node) : manager(_manager), id(_id)
    {

        manager = _manager;
        id = _id;
        parent = nullptr;
        is_leaf = nullptr;
        is_root = nullptr;
        buffer = nullptr;
        data = nullptr;
        next_node = nullptr;
        child_key_values = nullptr;
        pivot_pointers = nullptr;
        pivots_ctr = nullptr;
        open();

        // set parent now
        *parent = _parent;
        *is_leaf = _is_leaf;
        *is_root = _is_root;
        *next_node = _next_node;
    }

    // constructor used when we know that the node exists in disk
    /**
     *  returns: N/A
     *  Function: initializes a node to the given id and retrieves from disk.
     */

    BeNode(BlockManager *_manager, uint _id)
    {
        manager = _manager;
        id = _id;
        parent = nullptr;
        is_leaf = nullptr;
        is_root = nullptr;
        buffer = nullptr;
        data = nullptr;
        next_node = nullptr;
        child_key_values = nullptr;
        pivot_pointers = nullptr;
        pivots_ctr = nullptr;
        if (id != 0)
            open();
    }

    ~BeNode()
    {
    }

public:
    bool isLeaf()
    {
        open();
        return *is_leaf;
    }

    bool isRoot()
    {
        open();
        return *is_root;
    }

    void setLeaf(bool _is_leaf)
    {
        open();
        *is_leaf = _is_leaf;
        manager->addDirtyNode(id);
    }

    void setRoot(bool _is_root)
    {
        open();
        *is_root = _is_root;
        manager->addDirtyNode(id);
    }

    void setNextNode(uint *_next_node)
    {
        open();
        next_node = _next_node;
        manager->addDirtyNode(id);
    }

    void setNextNode(uint next_node_id)
    {
        open();
        *next_node = next_node_id;
        manager->addDirtyNode(id);
    }

    uint *getNextNode()
    {
        open();
        return next_node;
    }

public:
    /**
     *  returns: index of child slot
     *  Function finds suitable index where key can be placed
     *  in the current node (identifies the child)
     */
    uint slotOfKey(key_type key)
    {
        // make sure that the caller is an internal node
        open();
        // assert(!isLeaf());
        assert(!*is_leaf);

        // we have k pointers and k+1 pivots
        // where the first pivot will be for any key
        // X<= pointer[0] and last pivot will be for
        // any key X > pointer[k-1] (this will be pivot[k])

        uint pos;
        bool flag = false;

        uint lo = 0, hi = getPivotsCtr() - 1;

        while (lo < hi)
        {
            int mid = (lo + hi) >> 1;

            if (key <= child_key_values[mid])
            {
                hi = mid; // key <= mid
            }
            else
            {
                lo = mid + 1; // key > mid
            }
        }

        return lo;
    }

    /**
     *  returns: true or false indicating a split
     *  Function: inserts [num] data pairs in leaf. If leaf
     *  becomes full, returns true - indicating a split. Else
     *  returns false.
     */
    bool insertInLeaf(std::pair<key_type, value_type> buffer_elements[], int &num)
    {
        // make sure that caller node is a leaf
        open();
        // assert(isLeaf());
        assert(*is_leaf);

        // set node as dirty
        manager->addDirtyNode(id);

        // we want to maintain all elements in the leaf in a sorted order
        // since existing elements will be sorted and the buffer elements will also come
        // in a sorted order, we can simply merge the two arrays

        int i = data->size - 1;
        int j = num - 1;
        int last_index = data->size + num - 1;

        while (j >= 0)
        {
            if (i >= 0)
            {
                if (data->data[i] > buffer_elements[j])
                {
                    // copy element
                    data->data[last_index] = data->data[i];
                    i--;
                }
                else
                {
                    data->data[last_index] = buffer_elements[j];
                    j--;
                }
            }
            else
            {
                data->data[last_index] = buffer_elements[j];
                j--;
            }
            last_index--;
        }

        data->size += num;

        // check if after adding, the leaf  ` has exceeded limit and
        // return accordingly
        return data->size >= knobs::NUM_DATA_PAIRS;
    }

    bool insertInLeaf(std::pair<key_type, value_type> element)
    {
        open();
        // assert(isLeaf());
        assert(*is_leaf);
        // set node as dirty
        manager->addDirtyNode(id);

        if (data->size > 0)
            assert(element >= data->data[data->size - 1]);

        data->data[data->size++] = element;

        // check if after adding, the leaf  ` has exceeded limit and
        // return accordingly
        return data->size >= knobs::NUM_DATA_PAIRS;
    }


    /**
     * Swap the first key that is greater than the @key. This function can only be called by the BeTree::swap_in_tail_leaf
     * currently. It is used by the "lazy move" strategy of dual tree system, thus there must be a key that is greater than
     * @key
     * 
     * @param key: The key that will be inserted into the node.
     * @param val: The corresponding value of the key.
     * @param is_max: An indicator, set to true when the swapped key is the maximum key of the node.
     * @param is_min: An indicator, play a same role as @is_max.
     * 
     * @return The swapped key and its value.
    */
    std::pair<key_type, value_type> swap_in_leaf(key_type key, value_type val, bool &is_max, bool &is_min) {
        // When using this method, no need to consider splitting of leaf node.
        open();

        assert(*is_leaf);

        manager->addDirtyNode(id);
        // Search the first key that is greater than the inserted key.
        for(int i = 0; i < data->size; i++) {
            if(data->data[i].first > key) {
                if(i == data->size - 1) {
                    is_max = true;
                } else if(i == 0) {
                    is_min = true;
                }
                auto ret = data->data[i];
                data->data[i] = std::pair<key_type, value_type>(key, val); 
                return ret;

            }
        } 
        // Didn't find, should not arrive here, this could happen when a new leaf is created and the split fraction 
        //of the sorted tree is 1. Then no swap happens, and directly return the @key and @val
        return std::pair<key_type, value_type>(key, val);
    }
    /**
     *  returns: new node id
     *  Function: splits leaf into two
     */
    int splitLeaf(key_type &split_key, BeTraits &traits, uint &new_id, float split_frac = 0.5)
    {
#ifdef PROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        // make sure that caller is a leaf node
        open();
        // assert(isLeaf());
        assert(*is_leaf);
        // set node as dirty
        manager->addDirtyNode(id);

        new_id = manager->allocate();
        // create new node
        // BeNode<key_type, value_type, knobs, compare> new_sibling(manager, new_id, *parent, true, false, *next_node);
        BeNode<key_type, value_type, knobs, compare> new_sibling(manager, new_id);
        new_sibling.setParent(*parent);
        new_sibling.setLeaf(true);
        new_sibling.setRoot(false);
        new_sibling.setNextNode(*next_node);

        manager->addDirtyNode(new_id);

        // sort the data in the current node
        // std::sort(std::begin(data->data), std::end(data->data), compare_pair);
        // std::sort(data->data, data->data + (sizeof(data->data) * data->size));

        traits.num_blocks++;

        // start moving data pairs
        int start_index = (data->size) * split_frac;
#ifdef SPLIT70
        start_index = 0.7 * (data->size);
#elif SPLIT80
        start_index = 0.8 * (data->size);
#elif SPLIT90
        start_index = 0.9 * (data->size);
#elif SPLIT60
        start_index = 0.6 * (data->size);
#elif SPLIT50
        start_index = 0.5 * (data->size);
#endif
        for (int i = start_index; i < data->size; i++)
        {
            // new_sibling->data->data[new_sibling->data->size++] = data->data[i];
            new_sibling.data->data[new_sibling.data->size++] = data->data[i];
        }

        int data_init_size = data->size;

        // update size of old node
        data->size -= new_sibling.data->size;

#if defined(SPLIT70) || defined(SPLIT80) || defined(SPLIT90) || defined(SPLIT60) || defined(BULKLOAD)
        assert(data->size >= new_sibling.data->size);
#else
        if (split_frac <= 0.5) {
            assert(data->size <= new_sibling.data->size);
        } else {
            assert(data->size >= new_sibling.data->size);
        }
#endif

        // change current node's next node to new_node
        assert(new_sibling.getId() != 0);
        *next_node = new_sibling.getId();

        new_sibling.setParent(*parent);

        // split_key becomes lower bound of newly added sibling's keys
        // split_key = new_sibling->data->data[0].first;
        split_key = data->data[data->size - 1].first;
#ifdef PROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        splitleaf_time += duration.count();
#endif
        return new_id;
    }

    /**
     *  returns: node whose b
     *  Function: splits internal node into two. Distributes
     *              buffer and pivots as required
     */
    int splitInternal(key_type &split_key, BeTraits &traits, uint &new_id, float split_frac = 0.5)
    {
#ifdef PROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        open();
        // make sure that caller is not a leaf node
        // assert(!isLeaf());
        assert(!*is_leaf);

        manager->addDirtyNode(id);

        // make sure that we split only when we have hit the
        // pivot capacity
        assert(getPivotsCtr() == knobs::NUM_PIVOTS);

        // create a new node (blockid, parent = this->parent, is_leaf = false)
        new_id = manager->allocate();
        BeNode<key_type, value_type, knobs, compare> new_node(manager, new_id, *parent, false, false, *next_node);
        traits.num_blocks++;

        manager->addDirtyNode(new_id);

        BeNode temp_mover(manager, new_id);

        // move half the pivots to the new node
        int start_index = (getPivotsCtr()) * split_frac;

#ifdef SPLIT70
        start_index = 0.7 * (getPivotsCtr());
#elif SPLIT80
        start_index = 0.8 * (getPivotsCtr());
#elif BULKLOAD
        start_index = 0.95 * (getPivotsCtr());
#endif
        for (int i = start_index; i < getPivotsCtr(); i++)
        {
            open();
            new_node.open();
            // move all child keys
            if (i < getPivotsCtr() - 1)
            {
                new_node.child_key_values[i - start_index] = child_key_values[i];
            }
            // move all pointers
            new_node.pivot_pointers[i - start_index] = pivot_pointers[i];
            // new_node.pivots_ctr++;
            new_node.setPivotCounter(new_node.getPivotsCtr() + 1);

            // change the parent node for the pivots
            // new_node->pivot_pointers[i - start_index]->parent = new_node;
            temp_mover.setToId(pivot_pointers[i]);
            *temp_mover.parent = new_node.getId();
            manager->addDirtyNode(temp_mover.getId());
        }

        // reset pivots counter for old node
        // *pivots_ctr -= new_node.getPivotsCtr();
        setPivotCounter(getPivotsCtr() - new_node.getPivotsCtr());

        // split key is the first pointer of new node's children
        // alternatively, it is the pointer at pivots_ctr in the old node
        // since those have not been destroyed but the counter has been decreased
        // split_key = new_node->getChildKey(0); //[pivots_ctr]; // essentially, new_node.child_key_values[0]
        split_key = getChildKey(getPivotsCtr() - 1);

        // move buffer elements to new node as required
        // create a temp buffer that will later replace the old buffer with elements removed
        Buffer<key_type, value_type, knobs, compare> *temp = new Buffer<key_type, value_type, knobs, compare>();
        std::pair<key_type, value_type> empty_pair = temp->buffer[0];
        for (int i = 0; i < buffer->size; i++)
        {
            if (buffer->buffer[i].first >= split_key)
            {
                new_node.buffer->buffer[new_node.buffer->size] = buffer->buffer[i];
                new_node.buffer->size++;
            }
            else
            {
                temp->buffer[temp->size] = buffer->buffer[i];
                temp->size++;
            }
        }

        // copy temp to buffer
        for (int i = 0; i < buffer->size; i++)
        {
            if (i < temp->size)
            {
                buffer->buffer[i] = temp->buffer[i];
            }
            else
            {
                buffer->buffer[i] = empty_pair;
            }
        }

        buffer->size = temp->size;

#ifdef BPLUS
        assert(buffer->size == 0);
#endif
        // delete[] temp->buffer;
        delete temp;

        manager->addDirtyNode(id);

        // delete[] buffer;
        // // at this point, all elements have been distributed between the new node and the temp buffer
        // // temp buffer will contain elements that should be retained by the old node
        // // so replace the old node's buffer with temp
        // buffer = temp;

        *next_node = new_id;

        new_node.setParent(*parent);
        // return the newly created node

#ifdef PROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        splitinternal_time += duration.count();
#endif
        return new_id;
    }

    void prepare_for_flush(uint &chosen_child, int &num_to_flush, std::pair<key_type, value_type> *&elements_to_flush)
    {
#ifdef PROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        open();
        // assert(!isLeaf());
        assert(!*is_leaf);

        if (isRoot())
        {
            std::sort(buffer->buffer, buffer->buffer + buffer->size);
            manager->addDirtyNode(id);
        }

#ifdef PROFILE
        auto start_find = std::chrono::high_resolution_clock::now();
#endif
        // find no. of elements that can be flushed to each child
        int num_elements[knobs::NUM_CHILDREN];
        memset(num_elements, 0, sizeof(num_elements));

        uint buffer_spots[buffer->size];
        uint max_slot = 0;

        for (int i = 0; i < buffer->size; i++)
        {
            uint s = slotOfKey(buffer->buffer[i].first);
            num_elements[s] += 1;
            buffer_spots[i] = s;

            if (s != max_slot)
            {
                if (num_elements[s] > num_elements[max_slot])
                    max_slot = s;
            }
        }

        // find child that can accept maximum messages
        // for (int i = 0; i < getPivotsCtr(); i++)
        // {
        //     if (num_elements[i] > num_elements[chosen_child])
        //         chosen_child = i;
        // }

        chosen_child = max_slot;
#ifdef PROFILE
        auto stop_find = std::chrono::high_resolution_clock::now();
        auto duration_find = std::chrono::duration_cast<std::chrono::microseconds>(stop_find - start_find);
        find_time += duration_find.count();
#endif

#ifdef PROFILE
        auto start_extract = std::chrono::high_resolution_clock::now();
#endif
        // BeNode<key_type, value_type, knobs, compare> *child = pivot_pointers[chosen_child];
        BeNode<key_type, value_type, knobs, compare> child(manager, pivot_pointers[chosen_child]);

        int available_spots = knobs::NUM_UPSERTS - child.getBufferSize();

        int flush_limit = knobs::FLUSH_LIMIT;

#ifdef BPLUS
        if (!child.isLeaf())
        {
            if (child.getBufferSize() != 0)
            {
                int ix = 1;
                // get child's child and see if this error persists for lower level
                BeNode<key_type, value_type, knobs, compare> childchild(manager, child.getPivot(ix));
                std::cout << childchild.getPivotsCtr() << std::endl;
            }
            assert(child.getBufferSize() == 0);
        }
#endif

        if (child.isLeaf())
        {
            flush_limit = knobs::LEAF_FLUSH_LIMIT;
            available_spots = knobs::NUM_DATA_PAIRS - child.getDataSize();
        }
        assert(available_spots > 0);

#ifdef BPLUS
        flush_limit = 1;
#endif
        elements_to_flush = new std::pair<key_type, value_type>[flush_limit];
        // create a temp buffer that will later replace the current one
        // we will extract elements that need to be flushed into elements_to_flush
        // remaining elements will go into the temp buffer and will then replace
        // the original buffer

        Buffer<key_type, value_type, knobs, compare> *temp = new Buffer<key_type, value_type, knobs, compare>();
        std::pair<key_type, value_type> empty_pair = temp->buffer[0];
        for (int i = 0; i < buffer->size; i++)
        {
            // if (slotOfKey(buffer->buffer[i].first) == chosen_child)
            if (buffer_spots[i] == chosen_child)
            {
                // check if we are exceeding threshold
                // we are fine till the time we are less than the threshold limit
                // and we are less than available spots in the child's buffer
                if ((num_to_flush < flush_limit) && (num_to_flush < available_spots))
                {
                    elements_to_flush[num_to_flush] = buffer->buffer[i];
                    num_to_flush++;

                    // we can now move to the next iteration
                    continue;
                }
            }

            temp->buffer[temp->size] = buffer->buffer[i];
            temp->size++;
        }

#ifdef PROFILE
        auto stop_extract = std::chrono::high_resolution_clock::now();
        auto duration_extract = std::chrono::duration_cast<std::chrono::microseconds>(stop_extract - start_extract);
        extract_time += duration_extract.count();
#endif

#ifdef PROFILE
        auto start_replace = std::chrono::high_resolution_clock::now();
#endif
        // copy temp to buffer
        // for (int i = 0; i < buffer->size; i++)
        // {
        //     if (i < temp->size)
        //     {
        //         buffer->buffer[i] = temp->buffer[i];
        //     }
        //     else
        //     {
        //         buffer->buffer[i] = empty_pair;
        //     }
        // }
        memcpy(buffer->buffer, temp->buffer, temp->size * sizeof(buffer->buffer[0]));

        buffer->size = temp->size;
#ifdef BPLUS
        assert(buffer->size == 0);
#endif

        manager->addDirtyNode(id);

        // delete[] temp->buffer;
        delete temp;
#ifdef PROFILE
        auto stop_replace = std::chrono::high_resolution_clock::now();
        auto duration_replace = std::chrono::duration_cast<std::chrono::microseconds>(stop_replace - start_replace);
        replace_time += duration_replace.count();
#endif
#ifdef PROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        prepareflush_time += duration.count();
#endif
    }

    /**
     *  returns: true or false based on split or no split
     *  Function: flushes element of internal node buffer to its child leaf.
     *              If exceeding capacity of leaf, it splits
     */
    bool flushLeaf(BeNode<key_type, value_type, knobs, compare> &child, std::pair<key_type, value_type> *elements_to_flush, int &num_to_flush, key_type &split_key, uint &new_node_id, BeTraits &traits, float split_frac = 0.5)
    {
#ifdef PROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        // make sure caller is not a leaf node
        open();
        // assert(!isLeaf());
        assert(!*is_leaf);

        // set node as dirty
        manager->addDirtyNode(id);

        child.open();
        manager->addDirtyNode(child.getId());

        // make sure child is a leaf node
        assert(child.isLeaf());

        int init_data_size = child.getDataSize();
        // flush all elements to the leaf
        if (child.insertInLeaf(elements_to_flush, num_to_flush))
        {
            // require a split operation
            new_node_id = child.splitLeaf(split_key, traits, new_node_id, split_frac);

            assert(data->size <= knobs::NUM_DATA_PAIRS);

            // delete[] elements_to_flush;
            return true;
        }

        assert(child.getDataSize() == init_data_size + num_to_flush);
        // update trait characteristic
        // traits::leaf_flushes++;
#ifdef PROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        flushleaf_time += duration.count();
#endif
        // we have inserted and do not require a split so return false
        return false;
    }

    /**
     *  returns: true or false indicating buffer exceeding capacity or not
     *  Function: flushes elements of buffer from internal node to its
     *              child internal node.
     */
    bool flushInternal(BeNode<key_type, value_type, knobs, compare> &child, std::pair<key_type, value_type> *elements_to_flush, int &num_to_flush, float split_frac = 0.5)
    {
#ifdef PROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        // make sure caller is not a leaf node
        open();
        // assert(!isLeaf());
        assert(!*is_leaf);

        child.open();

        // set node as dirty
        manager->addDirtyNode(id);
        manager->addDirtyNode(child.getId());

        // make sure child is not a leaf node
        assert(!child.isLeaf());

        // add all elements to child's buffer
        // we want to maintain all elements in the buffer in a sorted order
        // since existing elements will be sorted and the buffer elements will also come
        // in a sorted order, we can simply merge the two arrays

        int i = child.buffer->size - 1;
        int j = num_to_flush - 1;
        int last_index = child.buffer->size + num_to_flush - 1;

        while (j >= 0)
        {
            if (i >= 0 && child.buffer->buffer[i] > elements_to_flush[j])
            {
                // copy element
                child.buffer->buffer[last_index] = child.buffer->buffer[i];
                i--;
            }
            else
            {
                child.buffer->buffer[last_index] = elements_to_flush[j];
                j--;
            }
            last_index--;
        }

#ifdef BPLUS
        assert(num_to_flush == 1);
#endif
        // memcpy(child.buffer->buffer + child.buffer->size, elements_to_flush, num_to_flush * sizeof(std::pair<key_type, value_type>));
        child.buffer->size += num_to_flush;

#ifdef PROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        flushinternal_time += duration.count();
#endif

        // delete[] elements_to_flush;
        // return true or false based on whether we exceeded capacity
        // if returning true, the caller for this function will have to
        // perform a subsequent flush operation at least for one more level
        return child.buffer->size >= knobs::NUM_UPSERTS;
    }

    /**
     *  returns: SPLIT or NOSPLIT
     *  Function: flushes a level of a tree from a node. If the internal node flush
     *  resulted in exceeding capacity, flushes a level again from that internal node
     */
    Result flushLevel(key_type &split_key, uint &new_node_id, BeTraits &traits, float split_frac = 0.5)
    {
        // #ifdef PROFILE
        //         auto start = std::chrono::high_resolution_clock::now();
        // #endif
        open();

        int num_to_flush = 0;
        uint chosen_child_idx = 0;

        // prepare current buffer for flush. Fetch elements required for flush from current buffer
        // and stores in elements_to_flush. It will also decrease the existing buffer after removing
        // elements to flush
        std::pair<key_type, value_type> *elements_to_flush = NULL;
        prepare_for_flush(chosen_child_idx, num_to_flush, elements_to_flush);

        assert(buffer->size <= knobs::NUM_UPSERTS);

        // fetch the child node that has been chosen to flush to
        BeNode<key_type, value_type, knobs, compare> child(manager, pivot_pointers[chosen_child_idx]);
        child.open();
        manager->addDirtyNode(id);

        Result res = NOSPLIT;

// this child might essentially be read from disk so account one I/O for reading it
#ifdef IO
        traits.io_insert++;
#endif

        if (child.isLeaf())
        {

            Result flag = flushLeaf(child, elements_to_flush, num_to_flush, split_key, new_node_id, traits, split_frac) ? SPLIT : NOSPLIT;

#ifdef BPLUS
            // since we have flushed, let's confirm if that flush worked correctly
            assert(buffer->size == 0);
#endif
            if (flag == SPLIT)
            {
                traits.leaf_splits++;
            }

            traits.leaf_flushes++;

// we are done modifying contents of child so writing back to disk takes 1 I/O
#ifdef IO
            traits.io_insert++;
#endif

            delete[] elements_to_flush;
            return flag;
        }

        traits.internal_flushes++;
        if (flushInternal(child, elements_to_flush, num_to_flush, split_frac))
        {

#ifdef BPLUS
            // since we have flushed from this node, let's confirm if that flush worked correctly
            assert(buffer->size == 0);
#endif
            res = child.flushLevel(split_key, new_node_id, traits, split_frac);

#ifdef BPLUS
            // we initiated a flushLevel for the child. When this returns, child's buffer should be empty
            assert(child.getBufferSize() == 0);
#endif
// we are done with the child here so need to write back to disk, account one i/o
#ifdef IO
            traits.io_insert++;
#endif
        }

        // if flushInternal was a success and buffer didn't exceed capacity
        // #ifdef PROFILE
        //         auto stop = std::chrono::high_resolution_clock::now();
        //         auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        //         flushlevel_time += duration.count();
        // #endif
        delete[] elements_to_flush;
        return res;
    }

    /**
     *  returns: true or false
     *  Function: adds a pivot to a node. Returns true if after adding the pivot,
     *          exceeds capacity for the node
     */
    bool addPivot(key_type &split_key, uint &new_node_id)
    {

        open();
        // assert(!isLeaf());
        assert(!*is_leaf);

        // set node as dirty
        manager->addDirtyNode(id);

        int node_position = slotOfKey(split_key);

        for (int i = getPivotsCtr() - 1; i >= node_position; i--)
        {
            pivot_pointers[i + 2] = pivot_pointers[i + 1];
            child_key_values[i + 1] = child_key_values[i];
        }

        child_key_values[node_position] = split_key;
        pivot_pointers[node_position + 1] = new_node_id;
        // *pivots_ctr++;
        setPivotCounter(getPivotsCtr() + 1);

        return getPivotsCtr() == knobs::NUM_PIVOTS;
    }

public:
    bool insertInBuffer(key_type key, value_type value)
    {
        open();

        std::pair<key_type, value_type> new_insert(key, value);
        buffer->buffer[buffer->size] = new_insert;
        buffer->size++;

        // set node as dirty
        manager->addDirtyNode(id);

        return buffer->size >= knobs::NUM_UPSERTS;
    }

    bool query(key_type key, BeTraits &traits)
    {
        open();
#ifdef IO
        // since we are into a new node which would be read from the disk, count an IO
        traits.io_pointquery++;
#endif
        // if current node is a leaf
        // search all data pairs
        if (*is_leaf)
        {
            // perform binary search

            // std::sort(data->data, data->data + sizeof(data->data));
#ifdef PROFILE
            auto start_tree_leaf = std::chrono::high_resolution_clock::now();
#endif
            bool found = std::binary_search(data->data, data->data + data->size, key, compare_pair_kv<key_type, value_type>());
#ifdef PROFILE
            auto stop_tree_leaf = std::chrono::high_resolution_clock::now();
            auto duration_tree_leaf = std::chrono::duration_cast<std::chrono::microseconds>(stop_tree_leaf - start_tree_leaf);
            tree_leaf_bin_search_time += duration_tree_leaf.count();
#endif
            return found;
        }

        // if current node is not a leaf, first check the buffer

        // binary search on buffer
        // std::sort(buffer->buffer, buffer->buffer + sizeof(buffer->buffer));
#ifdef PROFILE
        auto start_tree_buf = std::chrono::high_resolution_clock::now();
#endif
        bool found = std::binary_search(buffer->buffer, buffer->buffer + buffer->size, key, compare_pair_kv<key_type, value_type>());
#ifdef PROFILE
        auto stop_tree_buf = std::chrono::high_resolution_clock::now();
        auto duration_tree_buf = std::chrono::duration_cast<std::chrono::microseconds>(stop_tree_buf - start_tree_buf);
        tree_buf_bin_search_time += duration_tree_buf.count();
#endif

        if (found)
            return true;

#ifdef PROFILE
        auto start_tree_slot = std::chrono::high_resolution_clock::now();
#endif
        // if not found in buffer, we need to search its pivots
        int chosen_child_idx = slotOfKey(key);
        BeNode<key_type, value_type, knobs, compare> child(manager, pivot_pointers[chosen_child_idx]);
#ifdef PROFILE
        auto stop_tree_slot = std::chrono::high_resolution_clock::now();
        auto duration_tree_slot = std::chrono::duration_cast<std::chrono::microseconds>(stop_tree_slot - start_tree_slot);
        tree_slot_time += duration_tree_slot.count();
#endif

        return child.query(key, traits);
    }

public:
    std::vector<std::pair<key_type, value_type>> getElementsInRangeInBuffer(key_type low, key_type high, bool &corner)
    {
        open();

        std::vector<std::pair<key_type, value_type>> elements;
        // corner cases
        if (buffer->buffer[0].first > high)
        {
            corner = true;
            return elements;
        }

        for (int i = 0; i < buffer->size; i++)
        {
            if (buffer->buffer[i].first >= low && buffer->buffer[i].first <= high)
                elements.push_back(buffer->buffer[i]);

            // we can easily detect a corner case here itself
            // if buffer->buffer[i] > high, then we can conclude that no next node
            // will contain elements in the range after this node
            if (buffer->buffer[i].first > high)
                corner = true;
        }

        return elements;
    }

    std::vector<std::pair<key_type, value_type>> getElementsInRangeInLeaf(key_type low, key_type high, bool &corner)
    {
        open();
        std::vector<std::pair<key_type, value_type>> elements;

        // corner cases
        if (data->data[0].first > high)
        {
            corner = true;
            return elements;
        }

        for (int i = 0; i < data->size; i++)
        {
            if (data->data[i].first >= low && data->data[i].first <= high)
                elements.push_back(data->data[i]);

            // we can easily detect a corner case here itself
            // if data->data[i] > high, then we can conclude that no next node
            // will contain elements in the range after this node
            if (data->data[i].first > high)
                corner = true;
        }

        return elements;
    }

public:
    // helper function
    std::vector<std::pair<key_type, value_type>> mergeArrays(std::vector<std::pair<key_type, value_type>> left, std::vector<std::pair<key_type, value_type>> right)
    {
        open();
        std::vector<std::pair<key_type, value_type>> result;
        int l = 0, r = 0;

        while (l = r < left.size() + right.size())
        {
            if (l != left.size() && (r == right.size() || compare_pair<key_type, value_type>()(left[l], right[r])))
            {
                result.push_back(left[l]);
                l++;
            }
            else
            {
                result.push_back(right[r]);
                r++;
            }
        }
    }

public:
    std::vector<std::pair<key_type, value_type>> rangeQuery(key_type low, key_type high, BeTraits &traits)
    {
        open();
        BeNode<key_type, value_type, knobs, compare> start_node(manager, id);
        std::vector<std::vector<std::pair<key_type, value_type>>> elements;

// IO for reading start node
#ifdef IO
        traits.io_rangequery++;
#endif

        while (true)
        {
            start_node.open();
            // create new row of elements
            std::vector<std::pair<key_type, value_type>> row;

            // if current node is the root, collect elements of the buffer that qualify
            // then change current node to its first child that can contain lower limit
            if (start_node.isRoot() && !start_node.isLeaf())
            {
                if (start_node.getBufferSize() > 0)
                {
                    bool temp = false;
                    row = start_node.getElementsInRangeInBuffer(low, high, temp);
                    elements.push_back(row);
                }

                // find child that can contain lower range key
                int slot = start_node.slotOfKey(low);
                start_node.setToId(pivot_pointers[slot]);

// reading next node from memory so increase I/O counter
#ifdef IO
                traits.io_rangequery++;
#endif
                continue;
            }

            // if current node is leaf, gather all elements in leaf level. If we reach higher limit, stop collection.
            // break after collection since we have reached the last level of the tree
            else if (start_node.isLeaf())
            {
                bool flag = false;
                BeNode<key_type, value_type, knobs, compare> current_node(manager, start_node.getId());

                while (!flag)
                {
                    current_node.open();
                    // insert elements at end of row by fetching elements. flag will determine if we are
                    // reaching the upper limit
                    // row.insert(row.end(), current_node->getElementsInRangeInLeaf(low, high, flag));

                    if (current_node.getDataSize() > 0)
                    {
                        std::vector<std::pair<key_type, value_type>> fetched = current_node.getElementsInRangeInLeaf(low, high, flag);
                        // std::cout<<"Fetched Size = "<<fetched.size()<<std::endl;
                        for (int i = 0; i < fetched.size(); i++)
                        {
                            row.push_back(fetched[i]);
                        }
                    }

                    current_node.setToId(*current_node.getNextNode());

                    if (current_node.getId() == 0)
                        break;

                        // getting next node so increase I/O counter
#ifdef IO
                    traits.io_rangequery++;
#endif
                }

                if (row.size() > 0)
                    elements.push_back(row);

                // we probably don't need this as we are no longer using pointers
                // current_node = NULL;
                // delete current_node;
                break;
            }

            // in case of internal node, get elements in buffers of the same level like the leaf case
            // after collecting elements in a level, find the first pointer from
            else
            {
                bool flag = false;
                BeNode<key_type, value_type, knobs, compare> current_node(manager, start_node.getId());

                while (!flag)
                {
                    current_node.open();
                    // insert elements at end of row by fetching elements. flag will determine if we are
                    // reaching the upper limit
                    // row.insert(row.end(), current_node->getElementsInRangeInBuffer(low, high, flag));

                    if (current_node.getBufferSize() > 0)
                    {
                        std::vector<std::pair<key_type, value_type>> fetched = current_node.getElementsInRangeInBuffer(low, high, flag);
                        // std::cout << "Fetched Size = " << fetched.size() << std::endl;
                        for (int i = 0; i < fetched.size(); i++)
                        {
                            row.push_back(fetched[i]);
                        }
                    }
                    // std::cout<<"Goint to next node = "<<current_node.getNextNode().getId()<<std::endl;
                    current_node.setToId(*current_node.getNextNode());

                    if (current_node.getId() == 0)
                        break;

// reading next node from memory so increase I/O counter
#ifdef IO
                    traits.io_rangequery++;
#endif
                }

                if (row.size() > 0)
                    elements.push_back(row);

                // find next level's first child from current start node that can take in the lower key
                start_node.open();
                start_node.setToId(start_node.pivot_pointers[start_node.slotOfKey(low)]);

// reading next node from memory so increase I/O counter
#ifdef IO
                traits.io_rangequery++;
#endif
                // we probably don't need this since we are no longer using pointers
                // current_node = NULL;
                // delete current_node;
            }
        }

        // at this point we will have a 2d vector with all k level elements
        // we need to merge using a divide and conquer approach or priority queue approach

        typedef std::pair<std::pair<key_type, value_type>, std::pair<int, int>> ppi;

        std::vector<std::pair<key_type, value_type>> output;

        if (elements.size() == 1)
        {
            return elements[0];
        }
        for (int i = 0; i < elements.size(); i++)
        {
            // std::vector<std::pair<key_type, value_type> > row = elements[i];

            for (int j = 0; j < elements[i].size(); j++)
            {
                output.push_back(elements[i][j]);
            }
        }

        return output;
    }

public:
    uint getId()
    {
        open();
        return id;
    }

    void setId(uint _id)
    {
        open();
        id = _id;
    }

    int getBufferSize()
    {
        open();
        return buffer->size;
    }

    int getDataSize()
    {
        open();
        return data->size;
    }

    int getPivotsCtr()
    {
        open();
        return *pivots_ctr;
    }

    uint getParent()
    {
        open();
        return *parent;
    }

    std::pair<key_type, value_type> getLastDataPair()
    {
        open();
        // assert(isLeaf());
        assert(*is_leaf);

        return data->data[data->size - 1];
    }

    void setDataSize(int _size)
    {
        open();
        data->size = _size;
        manager->addDirtyNode(id);
    }

    void setPivotCounter(int _size)
    {
        open();
        *pivots_ctr = _size;
        manager->addDirtyNode(id);
    }

    void setParent(uint _parent)
    {
        open();
        *parent = _parent;
        manager->addDirtyNode(id);
    }

public:
    void setChildKey(key_type child_key, int slot)
    {
        open();
        assert(slot >= 0);

        child_key_values[slot] = child_key;
        manager->addDirtyNode(id);
    }

    void setPivot(uint pivot_node_id, int slot)
    {
        open();
        assert(slot >= 0 && slot < knobs::NUM_PIVOTS + 1);

        pivot_pointers[slot] = pivot_node_id;
        manager->addDirtyNode(id);
    }

    uint &getPivot(uint slot)
    {
        open();
        assert(slot >= 0);

        return pivot_pointers[slot];
    }

    key_type getChildKey(int slot)
    {
        open();
        assert(slot >= 0);

        return child_key_values[slot];
    }

    key_type *getChildKeyReference(int slot)
    {
        open();
        assert(slot >= 0);

        return &child_key_values[slot];
    }

    key_type getDataPairKey(int slot)
    {
        open();
        assert(slot >= 0);

        return data->data[slot].first;
    }

    key_type *getDataPairKeyReference(int slot)
    {
        open();
        assert(slot >= 0);

        return &data->data[slot].first;
    }

public:
    void fanout(int &num, int &total, int &max, int &min, int *arr, int &internal)
    {
        open();
        uint orig_id = id;
        if (!*is_leaf)
        {
            internal++;
            total += getPivotsCtr();
            arr[num] = getPivotsCtr();
            num++;

            if (getPivotsCtr() >= max)
                max = getPivotsCtr();

            if (getPivotsCtr() <= min)
                min = getPivotsCtr();

            for (int i = 0; i < getPivotsCtr(); i++)
            {
                uint curr_id = id;
                setToId(pivot_pointers[i]);
                fanout(num, total, max, min, arr, internal);
                setToId(curr_id);
            }
            setToId(orig_id);
        }
    }

    double buffer_occupancy(int &num, int &total, int &max, int &min, int &empty, int *arr)
    {
        open();
        uint orig_id = id;
        if (!*is_leaf)
        {
            total += buffer->size;
            arr[num] = buffer->size;
            num++;

            if (buffer->size >= max)
                max = buffer->size;

            if (buffer->size <= min)
                min = buffer->size;

            if (buffer->size == 0)
                empty++;

            for (int i = 0; i < getPivotsCtr(); i++)
            {
                uint curr_id = id;
                setToId(pivot_pointers[i]);
                buffer_occupancy(num, total, max, min, empty, arr);
                setToId(curr_id);
            }
            setToId(orig_id);
        }

        return total / (double)(num);
    }

    int depth()
    {
        open();
        uint orig_id = id;
        int d = 0;
        if (*is_leaf)
            return 0;

        for (int i = 0; i < getPivotsCtr(); i++)
        {
            setToId(pivot_pointers[i]);
            d = BE_MAX(d, depth());
        }
        setToId(orig_id);
        return d + 1;
    }

    int get_leaf_occupancy()
    {
        open();
        assert(*is_leaf);

        return data->size;
    }

public:
    int Serialize(Block *disk_store, int pos)
    {
        return 0;
    }

    //     void Deserialize(const Block &disk_store)
    //     {
    // #ifdef PROFILE
    //         auto start = std::chrono::high_resolution_clock::now();
    // #endif
    //         is_leaf = (bool *)(disk_store.block_buf);
    //         is_root = (bool *)(disk_store.block_buf + sizeof(is_leaf));
    //         parent = (uint *)(disk_store.block_buf + sizeof(is_leaf) + sizeof(is_root));
    //         next_node = (uint *)(disk_store.block_buf + sizeof(is_leaf) + sizeof(is_root) + sizeof(parent));
    //         pivots_ctr = (int *)(disk_store.block_buf + sizeof(is_leaf) + sizeof(is_root) + sizeof(parent) + sizeof(next_node));

    //         // every node has either a buffer or data. So essentially, both are in the same place
    //         data = (struct Data<key_type, value_type, knobs, compare> *)(disk_store.block_buf + sizeof(is_leaf) + sizeof(is_root) + sizeof(parent) + sizeof(next_node) + sizeof(pivots_ctr));
    //         buffer = (struct Buffer<key_type, value_type, knobs, compare> *)(disk_store.block_buf + sizeof(is_leaf) + sizeof(is_root) + sizeof(parent) + sizeof(next_node) + sizeof(pivots_ctr));

    //         // child_key_values = (key_type *)(disk_store.block_buf + sizeof(parent) + sizeof(is_leaf) + sizeof(is_root) + sizeof(next_node) + sizeof(pivots_ctr) + sizeof(struct Buffer<key_type, value_type, knobs, compare>*));
    //         child_key_values = (key_type *)(disk_store.block_buf + sizeof(parent) + sizeof(is_leaf) + sizeof(is_root) + sizeof(next_node) + sizeof(pivots_ctr) + sizeof(struct Buffer<key_type, value_type, knobs, compare>));

    //         int num = knobs::NUM_CHILDREN - 1;

    //         pivot_pointers = (uint *)(disk_store.block_buf + sizeof(parent) + sizeof(is_leaf) + sizeof(is_root) + sizeof(next_node) + sizeof(pivots_ctr) + sizeof(struct Buffer<key_type, value_type, knobs, compare>) + (num * sizeof(child_key_values)));

    //         assert(*is_leaf == true || *is_leaf == false);
    //         assert(*is_root == true || *is_root == false);
    //         assert(pivots_ctr >= 0);
    //         assert(*parent >= 0);
    //         assert(*next_node >=0);

    //         // std::cout<<sizeof(buffer)<<std::endl;
    //         // // std::cout<<sizeof(struct Buffer<key_type, value_type, knobs, compare>)<<std::endl;
    //         // // std::cout<<sizeof(struct Buffer<key_type, value_type, knobs, compare>*)<<std::endl;
    //         // std::cout<<"child "<<sizeof(child_key_values)<<std::endl;
    //         // std::cout<<sizeof(pivot_pointers)<<std::endl;
    //         // // std::cout<<sizeof(key_type)<<std::endl;
    //         // // std::cout<<sizeof(key_type*)<<std::endl;
    //         // std::cout<<knobs::S<<std::endl;
    //         // int num_pivs = knobs::NUM_PIVOTS;
    //         // int size1 = sizeof(parent) + sizeof(is_leaf) + sizeof(is_root) + sizeof(next_node) + sizeof(pivots_ctr) + sizeof(struct Buffer<key_type, value_type, knobs, compare>) + (num * sizeof(child_key_values)) + (num_pivs*sizeof(pivot_pointers));
    //         // int size2 = sizeof(is_leaf) + sizeof(is_root) + sizeof(parent) + sizeof(next_node) + sizeof(pivots_ctr) + sizeof(struct Data<key_type, value_type, knobs, compare>);
    //         // int size_buffer = sizeof(struct Buffer<key_type, value_type, knobs, compare>);
    //         // std::cout<<sizeof(buffer->size)<<std::endl;
    //         // std::cout<<sizeof(buffer->buffer)<<std::endl;
    //         // std::cout<<knobs::NUM_UPSERTS<<std::endl;
    //         // std::cout<<size1<<std::endl;
    //         // std::cout<<size2<<std::endl;
    // #ifdef PROFILE
    //         auto stop = std::chrono::high_resolution_clock::now();
    //         auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    //         deserialize_time += duration.count();
    // #endif
    //     }
    void Deserialize(const Block &disk_store)
    {
#ifdef PROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        is_leaf = (bool *)(disk_store.block_buf);
        is_root = (bool *)(disk_store.block_buf + sizeof(bool));
        parent = (uint *)(disk_store.block_buf + sizeof(bool) + sizeof(bool));
        next_node = (uint *)(disk_store.block_buf + sizeof(bool) + sizeof(bool) + sizeof(uint));
        pivots_ctr = (int *)(disk_store.block_buf + sizeof(bool) + sizeof(bool) + sizeof(uint) + sizeof(uint));

        // every node has either a buffer or data. So essentially, both are in the same place
        data = (struct Data<key_type, value_type, knobs, compare> *)(disk_store.block_buf + sizeof(bool) + sizeof(bool) + sizeof(uint) + sizeof(uint) + sizeof(int));
        buffer = (struct Buffer<key_type, value_type, knobs, compare> *)(disk_store.block_buf + sizeof(bool) + sizeof(bool) + sizeof(uint) + sizeof(uint) + sizeof(int));

        // child_key_values = (key_type *)(disk_store.block_buf + sizeof(parent) + sizeof(is_leaf) + sizeof(is_root) + sizeof(next_node) + sizeof(pivots_ctr) + sizeof(struct Buffer<key_type, value_type, knobs, compare>*));
        child_key_values = (key_type *)(disk_store.block_buf + sizeof(uint) + sizeof(bool) + sizeof(bool) + sizeof(uint) + sizeof(int) + sizeof(struct Buffer<key_type, value_type, knobs, compare>));

        int num = knobs::NUM_CHILDREN;

        pivot_pointers = (uint *)(disk_store.block_buf + sizeof(uint) + sizeof(bool) + sizeof(bool) + sizeof(uint) + sizeof(int) + sizeof(struct Buffer<key_type, value_type, knobs, compare>) + (num * sizeof(key_type)));

        assert(*is_leaf == true || *is_leaf == false);
        assert(*is_root == true || *is_root == false);
        assert(*pivots_ctr >= 0);
        assert(*parent >= 0);
        assert(*next_node >= 0);

#ifdef BPLUS
        if (!*is_leaf)
        {
            assert(buffer->size >= 0 && buffer->size <= 1);
        }

#endif

        // std::cout << sizeof(buffer) << std::endl;
        // std::cout << "child " << sizeof(child_key_values) << std::endl;
        // std::cout << sizeof(pivot_pointers) << std::endl;
        // std::cout << knobs::S << std::endl;
        // int num_pivs = knobs::NUM_PIVOTS;
        // int size1 = sizeof(parent) + sizeof(is_leaf) + sizeof(is_root) + sizeof(next_node) + sizeof(pivots_ctr) + sizeof(struct Buffer<key_type, value_type, knobs, compare>) + (num * sizeof(child_key_values)) + (num_pivs * sizeof(pivot_pointers));
        // int size2 = sizeof(is_leaf) + sizeof(is_root) + sizeof(parent) + sizeof(next_node) + sizeof(pivots_ctr) + sizeof(struct Data<key_type, value_type, knobs, compare>);

        // int size3 = sizeof(uint) + sizeof(bool) + sizeof(bool) + sizeof(uint) + sizeof(int) + sizeof(struct Buffer<key_type, value_type, knobs, compare>) + (num * sizeof(key_type)) + (num_pivs * sizeof(uint));
        // int size4 = sizeof(bool) + sizeof(bool) + sizeof(uint) + sizeof(uint) + sizeof(int) + sizeof(struct Data<key_type, value_type, knobs, compare>);

        // int size_buffer = sizeof(struct Buffer<key_type, value_type, knobs, compare>);
        // std::cout << sizeof(buffer->size) << std::endl;
        // std::cout << sizeof(buffer->buffer) << std::endl;
        // std::cout << knobs::NUM_UPSERTS << std::endl;
        // std::cout << size1 << std::endl;
        // std::cout << size2 << std::endl;
#ifdef PROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        deserialize_time += duration.count();
#endif
    }
};

template <typename _Key, typename _Value,
          typename _Knobs = BeTree_Default_Knobs<_Key, _Value>,
          typename _Compare = std::less<_Key>>
class BeTree
{
public:
    // *** Template parameter Types

    // key type of the tree
    typedef _Key key_type;

    // value type of the tree
    typedef _Value value_type;

    // knobs object that defines sizes for tree init.
    typedef _Knobs knobs;

    // compare function
    typedef _Compare compare;

    BlockManager *manager;

public:
    BeNode<key_type, value_type, knobs, compare> *root;

    BeNode<key_type, value_type, knobs, compare> *tail_leaf;

    BeNode<key_type, value_type, knobs, compare> *prev_tail;

    BeNode<key_type, value_type, knobs, compare> *head_leaf;

    uint head_leaf_id;
    uint tail_leaf_id;

    _Key min_key;
    _Key max_key;

    float split_frac;

public:
    BeTree(std::string _name, std::string _rootDir, unsigned long long _size_of_each_block, uint _blocks_in_memory, float split_frac = 0.5) : tail_leaf(nullptr), head_leaf(nullptr), split_frac(split_frac)
    {
        manager = new BlockManager(_name, _rootDir, _size_of_each_block, _blocks_in_memory);

        uint root_id = manager->allocate();
        root = new BeNode<key_type, value_type, knobs, compare>(manager, root_id);
        root->setRoot(true);
        root->setLeaf(true);

        // head_leaf = root;
        // tail_leaf = root;

        head_leaf_id = root_id;
        tail_leaf_id = root_id;

        max_key = -1;
        min_key = -1;

        /*std::cout << "B Epsilon Tree" << std::endl;
        std::cout << "Number of Upserts = " << knobs::NUM_UPSERTS << std::endl;
        std::cout << "Number of Pivots = " << knobs::NUM_PIVOTS << std::endl;
        std::cout << "Number of Children = " << knobs::NUM_CHILDREN << std::endl;
        std::cout << "Number of Data pairs = " << knobs::NUM_DATA_PAIRS << std::endl;*/

#ifdef UNITTEST

#else
        /*std::cout << "Block Size = " << knobs::BLOCK_SIZE << std::endl;
        std::cout << "Data Size = " << knobs::DATA_SIZE << std::endl;
        std::cout << "Block Size = " << knobs::BLOCK_SIZE << std::endl;
        std::cout << "Metadata Size = " << knobs::METADATA_SIZE << std::endl;
        std::cout << "Unit Size = " << knobs::UNIT_SIZE << std::endl;
        std::cout << "Pivots Size = " << knobs::PIVOT_SIZE << std::endl;
        std::cout << "Buffer Size = " << knobs::BUFFER_SIZE << std::endl;*/
#endif
    }

    ~BeTree()
    {
        // root->~BeNode();
        delete root;
        delete manager;
        // delete tail_leaf;
        // delete head_leaf;
    }

public:
    BeTraits traits;
#ifdef TIMER
    BeTimer timer;
#endif
public:
    key_type getMinimumKey()
    {
        return min_key;
    }

    key_type getMaximumKey()
    {
        return max_key;
    }

    key_type getTailMinimum(bool& is_empty) {
        // TODO: getDataSize may have issues when node.data is not initialized 
        if (tail_leaf == nullptr || tail_leaf->getDataSize() == 0) {
            // tail node is empty when tree is empty or after splitting with split_factor = 1
            is_empty = true;
            return max_key;
        } else {
            is_empty = false;
            return tail_leaf->getDataPairKey(0);
        }
    }

    key_type get_minimum_key_of_tail_leaf() {
        if(tail_leaf == nullptr) {
            return min_key;
        }
        return tail_leaf->getDataPairKey(0);
    }
public:
    std::pair<key_type, value_type> swap_in_tail_leaf(key_type key, value_type val) {
        // When using this method, no need to consider splitting of leaf node.
        assert(tail_leaf != nullptr);
        bool is_max = false;
        bool is_min = false;
        
        auto ret = tail_leaf->swap_in_leaf(key, val, is_max, is_min);
        if(is_max) {
            max_key = key;
        } else if(is_min && root->isLeaf()) {
            min_key = key;
        }
        return ret;
    }

    bool insert_to_tail_leaf(key_type key, value_type val, bool append)
    {   
        bool need_split;
        if(tail_leaf == nullptr)
        {
            // No tuple has been inserted, the tree is empty, but a root node is created when the
            //constructor was called, so we directly use it.
            BeNode<key_type, value_type, knobs, compare> *leaf = root;
            leaf->setLeaf(true);
            leaf->insertInLeaf(std::pair<key_type, value_type>(key, val));
            manager->addDirtyNode(root->getId());

            head_leaf = leaf;
            head_leaf_id = leaf->getId();
            tail_leaf = leaf;
            tail_leaf_id = leaf->getId();
            
            min_key = key;
            max_key = key;
            return true;
        }
        if(append)
        {
            need_split = tail_leaf->insertInLeaf(std::pair<key_type, value_type>(key, val));
            max_key = key;
        }
        else
        {
            std::pair<key_type, value_type> a[] = {std::pair<key_type, value_type>(key, val)};
            int num_to_insert = 1;
            need_split = tail_leaf->insertInLeaf(a, num_to_insert);
        }
        if(!need_split)
        {
            // No split is needed
            manager->addDirtyNode(tail_leaf_id);
            return true;
        }
        key_type split_key_leaf = tail_leaf->getDataPairKey(tail_leaf->getDataSize() - 1);
        uint new_leaf_id = manager->allocate();
        tail_leaf->splitLeaf(split_key_leaf, this->traits, new_leaf_id, split_frac);
        traits.leaf_splits++;
        BeNode<key_type, value_type, knobs, compare> *new_leaf = 
            new BeNode<key_type, value_type, knobs, compare>(manager, new_leaf_id);
        new_leaf->setLeaf(true);
        if(root->isLeaf())
        {   
            // only one node in the tree, the split is simple
            key_type split_key_new = root->getDataPairKey(root->getDataSize()-1);

            uint new_root_id = manager->allocate();
            BeNode<key_type, value_type, knobs, compare> *new_root = 
                new BeNode<key_type, value_type, knobs, compare>(manager, new_root_id);
            new_root->setRoot(true);

            new_root->setChildKey(split_key_new, 0);
            new_root->setPivot(root->getId(), 0);
            new_root->setPivot(new_leaf->getId(), 1);
            new_root->setPivotCounter(new_root->getPivotsCtr() + 2);
            manager->addDirtyNode(new_root_id);

            root->setRoot(false);
            root->setNextNode(new_leaf->getId());

            // set parents
            new_leaf->setParent(new_root_id);
            root->setParent(new_root_id);

            manager->addDirtyNode(new_leaf->getId());
            manager->addDirtyNode(root->getId());

            root = new_root;

            prev_tail = tail_leaf;
            tail_leaf = new_leaf;
            tail_leaf_id = new_leaf->getId(); 
        }  
        else
        {
            // The tree exists and:
            // add the leaf is enough or a split of internal node(s) is needed
            key_type split_key = tail_leaf->getDataPairKey(tail_leaf->getDataSize()-1);
            uint new_node_id = new_leaf->getId();
            new_leaf->setParent(tail_leaf->getParent());
            tail_leaf->setNextNode(new_leaf->getId());

            manager->addDirtyNode(new_leaf->getId());
            manager->addDirtyNode(tail_leaf->getId());

            prev_tail = tail_leaf;
            tail_leaf = new_leaf;
            tail_leaf_id = new_leaf->getId();

            // reload the newly added node
            BeNode<key_type, value_type, knobs, compare> new_node(manager, new_leaf->getId());
            while(true)
            {
                BeNode<key_type, value_type, knobs, compare> child_parent(manager, 
                    new_node.getParent());
                bool flag = child_parent.addPivot(split_key, new_node_id);
                manager->addDirtyNode(child_parent.getId());
                if(!flag)
                {
                    // Do not need split any more
                    break;
                }
                if(child_parent.isRoot())
                {
                    // Split root, and after spliting the root, the entire splitting process
                    //ends. 
                    // Here the splitInternal() will change the value @new_node_id to the 
                    //id of the node that is newly splitted.
                    child_parent.splitInternal(split_key, traits, new_node_id, split_frac);
                    BeNode<key_type, value_type, knobs, compare> new_sibling(manager, new_node_id);
                    manager->addDirtyNode(new_node_id);
                    traits.internal_splits++;

                    uint new_root_id = manager->allocate();
                    BeNode<key_type, value_type, knobs, compare> *new_root = new 
                        BeNode<key_type, value_type, knobs, compare>(manager, new_root_id);
                    new_root->setRoot(true);
                    new_root->setChildKey(split_key, 0);
                    new_root->setPivot(child_parent.getId(), 0);
                    new_root->setPivot(new_sibling.getId(), 1);
                    new_root->setPivotCounter(new_root->getPivotsCtr() + 2);
                    manager->addDirtyNode(new_root_id);

                    child_parent.setRoot(false);
                    child_parent.setParent(new_root->getId());
                    manager->addDirtyNode(child_parent.getId());
                    new_sibling.setParent(new_root->getId());
                    manager->addDirtyNode(new_sibling.getId());

                    root = new_root;
                    break;
                }

                // The parent node is not root, we just split it, and to see whether another 
                //split is needed.
                child_parent.splitInternal(split_key, traits, new_node_id, split_frac);
                traits.internal_splits++;
                manager->addDirtyNode(child_parent.getId());
                new_node.setToId(new_node_id);
                manager->addDirtyNode(new_node_id);
            }
        }

        return true;
    }

    bool insert(key_type key, value_type value)
    {
#ifdef TIMER
        auto start = std::chrono::high_resolution_clock::now();
#endif

// append i/o count for reading root from disk into memory
#ifdef IO
        traits.io_insert++;
#endif
        // if root is a leaf node, we insert in leaf until it exceeds capacity
        root->open();
        manager->addDirtyNode(root->getId());
        if (root->isLeaf())
        {
            std::pair<key_type, value_type> elements_to_insert[] = {std::pair<key_type, value_type>(key, value)};
            int num_to_insert = 1;
            bool flag = root->insertInLeaf(elements_to_insert, num_to_insert);
            manager->addDirtyNode(root->getId());
            uint new_id;
            if (tail_leaf == nullptr || tail_leaf == nullptr)
            {
                tail_leaf = root;
                head_leaf = root;
                tail_leaf_id = root->getId();
                head_leaf_id = root->getId();
            }

            if (root->getDataSize() == 1)
            {
                min_key = key;
                max_key = key;
            }

            // if flag returns true, it means we need to split the current leaf (actually the root)
            // once split, we create a new root that points to these two leaves
            if (flag)
            {
                key_type split_key_new;
                root->splitLeaf(split_key_new, traits, new_id, split_frac);
                BeNode<key_type, value_type, knobs, compare> new_leaf(manager, new_id);
                traits.leaf_splits++;

// writing new_leaf to disk takes one I/O
#ifdef IO
                traits.io_insert++;
#endif

                key_type old_root_key = root->getLastDataPair().first;

                // create new root node
                uint new_root_id = manager->allocate();
                BeNode<key_type, value_type, knobs, compare> *new_root = new BeNode<key_type, value_type, knobs, compare>(manager, new_root_id);
                new_root->setRoot(true);

// writing new root to disk takes one I/O
#ifdef IO
                traits.io_insert++;
#endif

                // new_root->addPivot(old_root_key, root);
                // new_root->addPivot(split_key_new, new_leaf);
                new_root->setChildKey(split_key_new, 0);
                new_root->setPivot(root->getId(), 0);
                new_root->setPivot(new_leaf.getId(), 1);
                new_root->setPivotCounter(new_root->getPivotsCtr() + 2);

                manager->addDirtyNode(new_root_id);

                // change old root
                root->setRoot(false);

                // set parents
                new_leaf.setParent(new_root->getId());
                root->setParent(new_root->getId());

                manager->addDirtyNode(root->getId());
                manager->addDirtyNode(new_leaf.getId());

                assert(root->getDataSize() <= knobs::NUM_DATA_PAIRS);

                // initially, the root is the head_leaf and tail_leaf. Now that we split, we need to update tail_leaf
                tail_leaf_id = new_leaf.getId();
                tail_leaf->setToId(new_leaf.getId());

                root = new_root;
            }
// we have added something to the root, so we need to write it back. This takes one I/O
// this case will also include if the root was split and a new root was created (here we append I/O counter for
// modifying old root node)
#ifdef IO
            traits.io_insert++;
#endif

#ifdef TIMER
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
            timer.insert_time += duration.count();
#endif
            return true;
        }

        if (root->insertInBuffer(key, value))
        {
            // buffer became full so we need to flush at root level

            key_type split_key;
            uint new_node_id = 0;

            // BeNode<key_type, value_type, knobs, compare> *leftovers[traits.num_blocks];
            // int left = 0;
#ifdef PROFILE
            auto start_flush = std::chrono::high_resolution_clock::now();
#endif
            Result result = root->flushLevel(split_key, new_node_id, traits, split_frac);
#ifdef PROFILE
            auto stop_flush = std::chrono::high_resolution_clock::now();
            auto duration_flush = std::chrono::duration_cast<std::chrono::microseconds>(stop_flush - start_flush);
            flushlevel_time += duration_flush.count();
#endif
            manager->addDirtyNode(root->getId());
            BeNode<key_type, value_type, knobs, compare> new_node(manager, new_node_id);

            bool flag = false;
            while (true)
            {
                if (result == SPLIT)
                {

                    // add pivot
                    BeNode<key_type, value_type, knobs, compare> child_parent(manager, new_node.getParent());
                    flag = child_parent.addPivot(split_key, new_node_id);

                    // since the result was a split, we check if  new_node's id matches with tail_leaf's next_node
                    BeNode<key_type, value_type, knobs, compare> tail(manager, tail_leaf_id);
                    if (*tail.getNextNode() == new_node.getId())
                    {
                        // update tail_leaf_id
                        tail_leaf_id = new_node.getId();
                        tail.setToId(tail_leaf_id);
                        tail_leaf->setToId(tail_leaf_id);
                    }

                    if (!flag)
                    {
                        result = NOSPLIT;
                        break;
                    }

                    if (child_parent.isRoot())
                    {
                        child_parent.splitInternal(split_key, traits, new_node_id, split_frac);
                        BeNode<key_type, value_type, knobs, compare> new_sibling(manager, new_node_id);
                        manager->addDirtyNode(new_node_id);
                        traits.internal_splits++;

                        // count one I/O for writing new_sibling
#ifdef IO
                        traits.io_insert++;
#endif

                        // create new root
                        uint new_root_id = manager->allocate();
                        BeNode<key_type, value_type, knobs, compare> *new_root = new BeNode<key_type, value_type, knobs, compare>(manager, new_root_id);
                        new_root->setRoot(true);
                        manager->addDirtyNode(new_root_id);

// count one I/O for writing new root to disk
#ifdef IO
                        traits.io_insert++;
#endif
                        new_root->setChildKey(split_key, 0);
                        new_root->setPivot(child_parent.getId(), 0);
                        new_root->setPivot(new_sibling.getId(), 1);
                        new_root->setPivotCounter(new_root->getPivotsCtr() + 2);

                        child_parent.setRoot(false);
                        child_parent.setParent(new_root->getId());
                        manager->addDirtyNode(child_parent.getId());

                        new_sibling.setParent(new_root->getId());
                        manager->addDirtyNode(new_sibling.getId());

                        root = new_root;

                        // counting one I/O for writing old root back to disk as it was modified done later

                        break;
                    }

                    // if flag returned true and child_parent's parent is not the root
                    // we need to split this internal node

                    // we set new_node to the newly split node
                    child_parent.splitInternal(split_key, traits, new_node_id, split_frac);
                    manager->addDirtyNode(child_parent.getId());
                    new_node.setToId(new_node_id);
                    manager->addDirtyNode(new_node_id);

// since we are splitting, new_node will be a newly created node form split
// this node needs to be written to disk which accounts for a single I/O
#ifdef IO
                    traits.io_insert++;
#endif

                    // we set the parent now to child_parent's parent
                    // child_parent = new_node->getParent();
                }

                else
                {
                    root->open();
                    break;
                }
            }
        }

        if (key < min_key)
            min_key = key;
        else if (key > max_key)
            max_key = key;

// all inserts start from root and we have already accounted one I/O for reading from disk
// we now need to write the root back to disk which takes one I/O
#ifdef IO
        traits.io_insert++;
#endif

#ifdef TIMER
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        timer.insert_time += duration.count();
#endif
        return true;
    }

    bool query(key_type key, key_type high = -1)
    {
        if (high < 0)
        {
#ifdef TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif
#ifdef IO
            int init_ios = traits.io_pointquery;
#endif

            bool flag = root->query(key, traits);

#ifdef IO
            int end_ios = traits.io_pointquery;
            int ios_for_query = end_ios - init_ios;
            assert(ios_for_query >= 1);
            if (ios_for_query > traits.io_max_pointquery)
            {
                traits.io_max_pointquery = ios_for_query;
            }
            else if (ios_for_query < traits.io_min_pointquery)
            {
                traits.io_min_pointquery = ios_for_query;
            }
#endif
#ifdef TIMER
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
            timer.point_query_time += duration.count();
#endif
            return flag;
        }

#ifdef TIMER
        auto start = std::chrono::high_resolution_clock::now();
#endif
#ifdef IO
        int init_range_ios = traits.io_rangequery;
#endif

        std::vector<std::pair<key_type, value_type>> elements = root->rangeQuery(key, high, traits);

#ifdef IO
        int end_range_ios = traits.io_rangequery;
        int ios_for_range_query = end_range_ios - init_range_ios;
        assert(ios_for_range_query >= 1);
        if (ios_for_range_query > traits.io_max_rangequery)
        {
            traits.io_max_rangequery = ios_for_range_query;
        }
        else if (ios_for_range_query < traits.io_min_rangequery)
        {
            traits.io_min_rangequery = ios_for_range_query;
        }
#endif
#ifdef TIMER
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        timer.range_query_time += duration.count();
#endif
        return true;
    }

    // bool rangeQuery(key_type low, key_type high)
    // {
    //     std::vector<std::pair<key_type, value_type>> elements = root->rangeQuery(low, high, traits);

    //     return true;
    // }
    std::vector<std::pair<key_type, value_type>> rangeQuery(key_type low, key_type high)
    {
#ifdef TIMER
        auto start = std::chrono::high_resolution_clock::now();
#endif
        std::vector<std::pair<key_type, value_type>> elements = root->rangeQuery(low, high, traits);
#ifdef TIMER
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        timer.range_query_time += duration.count();
#endif
        return elements;
    }

public:
    template <typename Iterator>
    bool bulkLoad(Iterator ibegin, Iterator iend)
    {
#ifdef TIMER
        auto start = std::chrono::high_resolution_clock::now();
#endif
        size_t num_items = iend - ibegin;
        size_t num_leaves = (num_items + knobs::NUM_DATA_PAIRS - 1) / knobs::NUM_DATA_PAIRS;

        Iterator it = ibegin;
        for (size_t i = 0; i < num_leaves; ++i)
        {
            // create new leaf
            uint new_leaf_id = manager->allocate();

            BeNode<key_type, value_type, knobs, compare> *leaf = new BeNode<key_type, value_type, knobs, compare>(manager, new_leaf_id);
            leaf->setLeaf(true);

            int slots_to_use = static_cast<int>(num_items / (num_leaves - i));

            for (size_t s = 0; s < slots_to_use; ++s, ++it)
            {
                leaf->insertInLeaf(*it);
            }

            // set next leaf pointers
            if (tail_leaf != nullptr)
            {
                tail_leaf->open();
                tail_leaf->setNextNode(leaf->getId());
            }
            else
            {
                // head_leaf = new BeNode<key_type, value_type, knobs, compare>(manager, leaf.getId());
                head_leaf = leaf;
            }

            tail_leaf = leaf;
            // tail_leaf = new BeNode<key_type, value_type, knobs, compare>(manager, leaf.getId());

            num_items -= slots_to_use;
        }

        assert(num_items == 0);
        head_leaf->open();
        tail_leaf->open();
        // if head and tail are the same, then we only have one node
        // this will be the root and we are done with bulk loading
        if (head_leaf->getId() == tail_leaf->getId())
        {
            root = head_leaf;
            return true;
        }

        // create first level of internal nodes that point to the leaves
        size_t num_parents = (num_leaves + (knobs::NUM_PIVOTS + 1) - 1) / (knobs::NUM_PIVOTS + 1);

        // save internal nodes and maxkey for next level
        // typedef std::pair<BeNode<key_type, value_type, knobs, compare> *, const key_type *> nextlevel_type;
        // typedef std::pair<int, const key_type *> nextlevel_type;
        typedef std::pair<uint, const key_type *> nextlevel_type;
        nextlevel_type *next_level = new nextlevel_type[num_parents];

        // swtich leaf node back to the head leaf
        BeNode<key_type, value_type, knobs, compare> *leaf = head_leaf;
        key_type *level_keys = new key_type[num_parents];

        for (size_t i = 0; i < num_parents; i++)
        {
            leaf->open();
            // create internal node
            uint n_id = manager->allocate();
            // BeNode<key_type, value_type, knobs, compare> *n = new BeNode<key_type, value_type, knobs, compare>();
            BeNode<key_type, value_type, knobs, compare> *n = new BeNode<key_type, value_type, knobs, compare>(manager, n_id);

            int slots_to_use = static_cast<int>((num_leaves - 1) / (num_parents - i));
            // since internal node has one more pointer than keys, we reduce the counter

            n->setPivotCounter(slots_to_use);

            // copy last key from each leaf and set child
            for (int s = 0; s < slots_to_use; ++s)
            {
                leaf->open();
                n->open();
                n->setChildKey(leaf->getDataPairKey(leaf->getDataSize() - 1), s);
                // n->setChildKey(leaf->getNextNode()->getDataPairKey(0), s);
                n->setPivot(leaf->getId(), s);
                // leaf = leaf->getNextNode();
                leaf->setToId(*leaf->getNextNode());
            }

            n->open();
            leaf->open();
            n->setPivot(leaf->getId(), n->getPivotsCtr());
            n->setPivotCounter(n->getPivotsCtr() + 1);

            // track max key of any descendant
            next_level[i].first = n->getId();
            // next_level[i].second = leaf->getDataPairKeyReference(leaf->getDataSize() - 1);
            // next_level[i].second = leaf->getDataPairKey(leaf->getDataSize() - 1);
            level_keys[i] = *leaf->getDataPairKeyReference(leaf->getDataSize() - 1);
            next_level[i].second = &level_keys[i];

            // next_level[i].second = leaf->getNextNode()->getDataPairKeyReference(0);
            // next_level[i].second = &leaf->child_key_values[leaf->getPivotsCtr() - 1];

            // leaf = leaf->getNextNode();
            leaf->setToId(*leaf->getNextNode());
            num_leaves -= n->getPivotsCtr();
        }

        leaf->open();
        assert(leaf->getId() == 0 && num_leaves == 0);

        // start recursively building subsequent levels bototm up
        for (int level = 2; num_parents != 1; ++level)
        {
            size_t num_children = num_parents;
            num_parents = (num_children + (knobs::NUM_PIVOTS + 1) - 1) / (knobs::NUM_PIVOTS + 1);

            size_t inner_index = 0;
            for (size_t i = 0; i < num_parents; ++i)
            {
                // allocate internal node
                uint n_id = manager->allocate();
                BeNode<key_type, value_type, knobs, compare> *n = new BeNode<key_type, value_type, knobs, compare>(manager, n_id);

                int slots_to_use = static_cast<int>((num_children - 1) / (num_parents - i));
                n->setPivotCounter(slots_to_use);

                for (int s = 0; s < slots_to_use; ++s)
                {
                    n->setChildKey(*next_level[inner_index].second, s);
                    n->setPivot(next_level[inner_index].first, s);
                    ++inner_index;
                }
                n->setPivot(next_level[inner_index].first, n->getPivotsCtr());
                n->setPivotCounter(n->getPivotsCtr() + 1);

                // reuse nextlevel array for parents
                next_level[i].first = n->getId();
                next_level[i].second = next_level[inner_index].second;

                ++inner_index;
                num_children -= n->getPivotsCtr();
            }

            assert(num_children == 0);
        }

        root->setToId(next_level[0].first);
        root->open();

        delete[] level_keys;
        delete[] next_level;

#ifdef TIMER
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        timer.bulk_load_time += duration.count();
#endif
        return true;
    }

    template <typename Iterator>
    bool bulkload_leaf(Iterator ibegin, Iterator iend)
    {
        // #ifdef TIMER
        //         auto start = std::chrono::high_resolution_clock::now();
        // #endif
        // first create leaf node
        uint new_leaf_id = manager->allocate();
        BeNode<key_type, value_type, knobs, compare> *leaf = new BeNode<key_type, value_type, knobs, compare>(manager, new_leaf_id);
        leaf->setLeaf(true);

        // add all elements to leaf.
        size_t num_items = iend - ibegin;
        Iterator it = ibegin;
        for (size_t s = 0; s < num_items + 1; ++s, ++it)
        {
            leaf->insertInLeaf(*it);
        }

        // now add to tree

        // case 1: tail leaf is null -> meaning tree is empty
        if (tail_leaf == nullptr)
        {
            root = leaf;
            root->setRoot(true);
            head_leaf = leaf;
            head_leaf_id = leaf->getId();
            // newly added leaf is always tail
            tail_leaf = leaf;
            tail_leaf_id = leaf->getId();

            min_key = ibegin->first;
            max_key = iend->first;
        }

        else
        {
            // if tree is not empty, we are only going to add rightwards
            // so we only need to update max key
            max_key = iend->first;

            // case 2: tail leaf is not null, but root and tail leaf are the same, i.e. root is a leaf
            // this means that there is only one node in tree
            if (root->isLeaf())
            {
                // key_type split_key = root->getLastDataPair().first;
                key_type split_key_new = root->getDataPairKey(root->getDataSize() - 1);

                // create new root
                uint new_root_id = manager->allocate();
                BeNode<key_type, value_type, knobs, compare> *new_root = new BeNode<key_type, value_type, knobs, compare>(manager, new_root_id);
                new_root->setRoot(true);

                // set child keys and pivots
                new_root->setChildKey(split_key_new, 0);
                new_root->setPivot(root->getId(), 0);
                new_root->setPivot(leaf->getId(), 1);
                new_root->setPivotCounter(new_root->getPivotsCtr() + 2);
                manager->addDirtyNode(new_root_id);

                // change old root
                root->setRoot(false);
                root->setNextNode(leaf->getId());

                // set parents
                leaf->setParent(new_root_id);
                root->setParent(new_root_id);

                manager->addDirtyNode(leaf->getId());
                manager->addDirtyNode(root->getId());

                root = new_root;
                // newly added leaf is always tail
                tail_leaf = leaf;
                tail_leaf_id = leaf->getId();
            }
            else
            {
                // case 3: tree exists and we just need to add the leaf
                // here we can have two possibilities: add and done, or we need to split internal nodes/root

                // key_type split_key = tail_leaf->getLastDataPair().first;
                key_type split_key = tail_leaf->getDataPairKey(tail_leaf->getDataSize() - 1);
                uint new_node_id = leaf->getId();
                leaf->setParent(tail_leaf->getParent());
                tail_leaf->setNextNode(leaf->getId());

                manager->addDirtyNode(leaf->getId());
                manager->addDirtyNode(tail_leaf->getId());

                // newly added leaf is always tail
                tail_leaf = leaf;
                tail_leaf_id = leaf->getId();

                // BeNode<key_type, value_type, knobs, compare> *new_node = new BeNode<key_type, value_type, knobs, compare> (manager, leaf->getId());
                BeNode<key_type, value_type, knobs, compare> new_node(manager, leaf->getId());
                while (true)
                {
                    BeNode<key_type, value_type, knobs, compare> child_parent(manager, new_node.getParent());
                    bool flag = child_parent.addPivot(split_key, new_node_id);
                    manager->addDirtyNode(child_parent.getId());
                    if (!flag)
                    {
                        break;
                    }

                    if (child_parent.isRoot())
                    {
                        // split root
                        child_parent.splitInternal(split_key, traits, new_node_id, split_frac);
                        BeNode<key_type, value_type, knobs, compare> new_sibling(manager, new_node_id);
                        manager->addDirtyNode(new_node_id);
                        traits.internal_splits++;

                        // create new root
                        uint new_root_id = manager->allocate();
                        BeNode<key_type, value_type, knobs, compare> *new_root = new BeNode<key_type, value_type, knobs, compare>(manager, new_root_id);
                        new_root->setRoot(true);
                        manager->addDirtyNode(new_root_id);

                        new_root->setChildKey(split_key, 0);
                        new_root->setPivot(child_parent.getId(), 0);
                        new_root->setPivot(new_sibling.getId(), 1);
                        new_root->setPivotCounter(new_root->getPivotsCtr() + 2);

                        child_parent.setRoot(false);
                        child_parent.setParent(new_root->getId());
                        manager->addDirtyNode(child_parent.getId());

                        new_sibling.setParent(new_root->getId());
                        manager->addDirtyNode(new_sibling.getId());

                        root = new_root;
                        break;
                    }
                    // if flag returned true but child parent is not root
                    // split internal node and check for propagating splits upwards

                    child_parent.splitInternal(split_key, traits, new_node_id, split_frac);
                    traits.internal_splits++;
                    manager->addDirtyNode(child_parent.getId());
                    new_node.setToId(new_node_id);
                    manager->addDirtyNode(new_node_id);

#ifdef IO
                    traits.io_insert++;
#endif
                }
            }
        }

        // #ifdef TIMER
        //         auto stop = std::chrono::high_resolution_clock::now();
        //         auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        //         timer.bulk_load_time += duration.count();
        // #endif

        return true;
    }

    template <typename Iterator>
    bool bulkload_helper(Iterator ibegin, Iterator iend)
    {
#ifdef TIMER
        auto start = std::chrono::high_resolution_clock::now();
#endif
        size_t num_items = iend - ibegin + 1;
        // size_t num_leaves = (num_items + knobs::NUM_DATA_PAIRS - 1) / knobs::NUM_DATA_PAIRS;
        size_t num_leaves = (num_items) / (knobs::NUM_DATA_PAIRS - 1 - 1);
        int slots_to_use = static_cast<int>(num_items / num_leaves);

        Iterator it = ibegin;
        for (size_t i = 0; i < num_leaves; ++i)
        {
            bulkload_leaf(it, std::next(it, slots_to_use - 1));
            std::advance(it, slots_to_use);
        }
#ifdef TIMER
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        timer.bulk_load_time += duration.count();
#endif
        return true;
    }

public:
    bool more_than_one_leaf() {
        return head_leaf != nullptr && tail_leaf != nullptr && head_leaf != tail_leaf;
    }

    bool is_tail_leaf_empty() {
        BeNode<key_type, value_type, knobs, compare> *leaf = new BeNode<key_type, value_type, knobs, compare>(manager, tail_leaf_id);
        int tail_size = leaf->get_leaf_occupancy();
        return tail_size == 0;
    }

    key_type get_prev_tail_maximum_key() {
        assert(prev_tail != nullptr);
        return prev_tail->getLastDataPair().first;
    }

    double findMedian(int *a, int n)
    {
        std::sort(a, a + n);

        if (n % 2 != 0)
        {
            return (double)a[n / 2];
        }

        return double((a[(n - 1) / 2] + a[n / 2]) / 2.0);
    }

    void fanout()
    {
        int num = 0, total = 0, max = 0, min = 0, internal = 0;
        int n = manager->getCurrentBlocks();
        int *arr = new int[n];

        min = root->getPivotsCtr();
        root->fanout(num, total, max, min, arr, internal);

        std::cout << "--------------Internal = " << internal << std::endl;

        // here num will return number of internal nodes
        traits.num_leaf_nodes = n - num;
        traits.num_internal_nodes = num;

        traits.median_fanout = findMedian(arr, num);

        traits.max_fanout = max;
        traits.min_fanout = min;
        traits.average_fanout = ceil(total / num);
        traits.num_nodes = n;

        delete[] arr;
    }

    void buffer_occupancy()
    {
        int num = 0, total = 0, max = 0, min = 0, empty = 0;

        int n = manager->getCurrentBlocks();
        int *arr = new int[n];

        min = root->getBufferSize();
        root->buffer_occupancy(num, total, max, min, empty, arr);

        // find median and assign
        traits.median_buffer_occupancy = findMedian(arr, num);

        traits.max_buffer_occupancy = max;
        traits.min_buffer_occupancy = min;
        traits.average_buffer_occupancy = ceil(total / num);
        traits.empty_buffer_nodes = empty;
        // traits.num_nodes = num;

        delete[] arr;
    }

    int depth()
    {
        return root->depth();
    }

    std::vector<int> get_leaves_occupancy()
    {
        int min = 0, max = 0;
        int tot = 0;
        int num = 0;

        std::vector<int> occs;

        int limit_80 = knobs::NUM_DATA_PAIRS * 0.8;
        int limit_50 = knobs::NUM_DATA_PAIRS * 0.5;
        int limit_20 = knobs::NUM_DATA_PAIRS * 0.2;

        // start with heaf leaf
        uint next_leaf_id = head_leaf_id;
        BeNode<key_type, value_type, knobs, compare> *leaf = new BeNode<key_type, value_type, knobs, compare>(manager, head_leaf_id);
        bool head = true;
        while (next_leaf_id != tail_leaf_id && next_leaf_id != 0)
        {
            int occ = leaf->get_leaf_occupancy();
            occs.push_back(occ);
            tot += occ;
            num++;

            if (head)
            {
                min = occ;
                max = occ;
                head = false;
            }
            else
            {
                if (occ < min)
                    min = occ;
                if (occ > max)
                    max = occ;
            }

            // stats collection
            if (occ >= limit_80)
                traits.num_gr_80++;
            else if (occ >= limit_50 && occ < limit_80)
                traits.num_50_80++;
            else if (occ >= limit_20 && occ < limit_50)
                traits.num_20_50++;
            else if (occ < limit_20)
                traits.num_le_20++;

            next_leaf_id = *leaf->getNextNode();
            leaf->setToId(next_leaf_id);
        }

        traits.min_leaf_occupancy = min;
        traits.max_leaf_occupancy = max;
        traits.avg_leaf_occupancy = tot / num;
        std::cout << "num = " << num << std::endl;

        delete leaf;

        return occs;
    }

    unsigned long long getLeafCacheMisses() { return manager->getLeafCacheMisses(); }

    unsigned long long getInternalCacheMisses() { return manager->getInternalCacheMisses(); }

    unsigned long long getLeafCacheHits() { return manager->getLeafCacheHits(); }

    unsigned long long getInternalCacheHits() { return manager->getInternalCacheHits(); }

    unsigned long long getTotalCacheReqs() { return manager->getTotalCacheReqs(); }

    unsigned long long getNumReads() { return manager->num_reads; }

    unsigned long long getNumWrites() { return manager->num_writes; }

    uint getNumBlocks() { return manager->current_blocks; }

    uint getBlocksInMemoryCap() { return manager->blocks_in_memory_cap; }

#ifdef PROFILE
    void getTimers(unsigned long &openb, unsigned long &readb, unsigned long &writeb)
    {
        return manager->getTimers(openb, readb, writeb);
    }
#endif
};

#endif