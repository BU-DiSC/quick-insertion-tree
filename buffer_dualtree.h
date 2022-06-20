#ifndef DUALTREE_H
#define DUALTREE_H
#define TYPE int
#define DIST 1
#define STDEV 2

#include <stdlib.h>
#include <bits/stdc++.h>

#include "betree.h"
#include "dist_detector.h"

template<typename _key, typename _value>
class DUAL_TREE_KNOBS
{
public:
    // Sorted tree split fraction, it will affect the space utilization of the tree. It means how
    // many elements will stay in the original node.
    static constexpr float SORTED_TREE_SPLIT_FRAC = 1.0F;

    // Unsorted tree splitting fraction.
    static constexpr float UNSORTED_TREE_SPLIT_FRAC = 0.5F;

    static const uint HEAP_SIZE = 15;

    // The initial tolerance threshold, determine whether the key of the newly added tuple is too far from the previous
    //tuple in the sorted tree. If set it to 0, the dual tree will disable the outlier detector.
    static const uint INIT_TOLERANCE_FACTOR = 100;

    // The minimum value of the TOLERANCE_FACTOR, when the value of tolerance factor is too small, 
    //most tuples will be inserted to the unsorted tree, thus we need to keep the value from too small.
    //This value should be less than @INIT_TOLERANCE_FACTOR
    static constexpr float MIN_TOLERANCE_FACTOR = 20;

    // The expected average distance between any two consecutive tuples in the sorted tree. This
    //tuning knob helps to modify the tolerance factor in the outlier detector. If it is less or equal to 
    //1, then the tolerance factor becomes a constant.
    static constexpr float EXPECTED_AVG_DISTANCE = 2.5;

    static const bool ENABLE_OUTLIER_DETECTOR = false;

    static const TYPE OUTLIER_DETECTOR_TYPE = DIST;

};

template<typename _key, typename _value>
class Heap : public std::priority_queue<std::pair<_key, _value>, 
                                    std::vector<std::pair<_key, _value>>, 
                                    std::greater<std::pair<_key, _value>>>
{
public:  
    typename std::vector<std::pair<_key, _value>>::iterator begin(){
        return std::priority_queue<std::pair<_key, _value>, std::vector<std::pair<_key, _value>>, 
                    std::greater<std::pair<_key, _value>>>::c.begin();
    }
    typename std::vector<std::pair<_key, _value>>::iterator end(){
        return std::priority_queue<std::pair<_key, _value>, std::vector<std::pair<_key, _value>>, 
                    std::greater<std::pair<_key, _value>>>::c.end();
    }
};

template <typename _key, typename _value, typename _dual_tree_knobs=DUAL_TREE_KNOBS<_key, _value>,
            typename _betree_knobs = BeTree_Default_Knobs<_key, _value>, 
            typename _compare=std::less<_key>>
class dual_tree
{
    // Left tree to accept unsorted input data.
    BeTree<_key, _value, _betree_knobs, _compare>* unsorted_tree;
    // Right tree to accept sorted input data.
    BeTree<_key, _value, _betree_knobs, _compare>* sorted_tree;

    uint sorted_size;
    uint unsorted_size;

    // Root directory for storing data.
    static const std::string TREE_DATA_ROOT_DIR;

    Heap<_key, _value>* heap_buffer;

    // TODO: create a super class for all outlier detectors
    DistDetector<_key>* outlier_detector;

public:

    // Default constructor, disable the buffer.
    dual_tree(std::string tree_path_1, std::string tree_path_2)
    {   
        const std::string TREE_DATA_ROOT_DIR = "./tree_dat";
        // initialize sorted tree and unsorted tree
        unsorted_tree = new BeTree<_key, _value, _betree_knobs, _compare>(tree_path_1, TREE_DATA_ROOT_DIR, 
                                    _betree_knobs::BLOCK_SIZE, _betree_knobs::BLOCKS_IN_MEMORY, _dual_tree_knobs::UNSORTED_TREE_SPLIT_FRAC);
        sorted_tree = new BeTree<_key, _value, _betree_knobs, _compare>(tree_path_2, TREE_DATA_ROOT_DIR, 
                                    _betree_knobs::BLOCK_SIZE, _betree_knobs::BLOCKS_IN_MEMORY, _dual_tree_knobs::SORTED_TREE_SPLIT_FRAC);
        
        sorted_size = 0;
        unsorted_size = 0;

        if (_dual_tree_knobs::HEAP_SIZE > 0) {
            // initialize heap buffer if the tuning knob::heap size > 0
            heap_buffer = new Heap<_key, _value>();
        }

        if (_dual_tree_knobs::ENABLE_OUTLIER_DETECTOR) {
            if (_dual_tree_knobs::OUTLIER_DETECTOR_TYPE == DIST) {
                outlier_detector = new DistDetector<_key>(_dual_tree_knobs::INIT_TOLERANCE_FACTOR, 
                                                        _dual_tree_knobs::MIN_TOLERANCE_FACTOR, _dual_tree_knobs::EXPECTED_AVG_DISTANCE);
            }
        }
    }

    // Deconstructor
    ~dual_tree()
    {
        delete sorted_tree;
        delete unsorted_tree;
        if (_dual_tree_knobs::HEAP_SIZE > 0) {
            delete heap_buffer;
        }
        if (_dual_tree_knobs::ENABLE_OUTLIER_DETECTOR) {
            delete outlier_detector;
        }
    }

    uint sorted_tree_size() { return sorted_size;}

    uint unsorted_tree_size() { return unsorted_size;}

    bool insert(_key key, _value value)
    {
        bool is_empty = false;
        _key tail_min = _get_required_minimum_inserted_key(is_empty);
        // _key tail_min = sorted_tree->getTailMinimum(is_empty);
        if (!is_empty && key < tail_min) {
            // when key is smaller than tail_min, insert directly to unsorted tree
            unsorted_tree->insert(key, value);
            unsorted_size++;
        } else {
            // push to heap buffer
            // if buffer is full, pop the key of min heap
            if(_dual_tree_knobs::HEAP_SIZE > 0)
            {
                assert(heap_buffer->size() <= _dual_tree_knobs::HEAP_SIZE);
                if(heap_buffer->size() < _dual_tree_knobs::HEAP_SIZE)
                {
                    // heap is not full, add new tuple to the heap and return
                    heap_buffer->push(std::pair<_key, _value>(key, value));
                    return true;
                }
                else
                {
                    if(std::pair<_key, _value>(key, value) > heap_buffer->top())
                    {
                        // if current key is greater than the minimum key in the heap
                        // pop the heap_min
                        std::pair<_key, _value> heap_min = heap_buffer->top();
                        heap_buffer->pop();
                        heap_buffer->push(std::pair<_key, _value>(key, value));
                        // push current key to heap buffer
                        key = heap_min.first;
                        value = heap_min.second;
                    }
                }
            }
            // insert current key to sorted tree if it pass outlier check
            if (!_dual_tree_knobs::ENABLE_OUTLIER_DETECTOR || !outlier_detector->is_outlier(key)) {
                bool append = key >= sorted_tree->getMaximumKey();
                sorted_tree->insert_to_tail_leaf(key, value, append);
                sorted_size++;
            } else {
                // insert outlier key to unsorted tree
                unsorted_tree->insert(key, value);
                unsorted_size++;
            }
        }
        return true;
    }

    bool query(_key key)
    {
        bool found = false;
        // Search the one with more tuples at first.
        if(sorted_size > unsorted_size)
        {
            found = sorted_tree->query(key) || unsorted_tree->query(key);
        }
        else
        {
            found = unsorted_tree->query(key) || sorted_tree->query(key);
        }

        if (!found) {
            // search the buffer
            typename std::vector<std::pair<_key, _value>>::iterator iter = heap_buffer->begin();
            while(iter!=heap_buffer->end()) {
                if (iter->first == key) {
                    return true;
                }
                iter++;
            }
        }

        return found;
    }

    

    void display_stats()
    {
        sorted_tree->fanout();
        std::cout << "Sorted Tree: number of splitting leaves = " << sorted_tree->traits.leaf_splits
            << std::endl;
        std::cout << "Sorted Tree: number of splitting internal nodes = " << 
            sorted_tree->traits.internal_splits << std::endl;
        std::cout << "Sorted Tree: number of leaves = " << sorted_tree->traits.num_leaf_nodes 
            << std::endl;
        std::cout << "Sorted Tree: number of internal nodes = " << 
            sorted_tree->traits.num_internal_nodes << std::endl;
        std::cout << "Sorted Tree: Maximum value = " << sorted_tree->getMaximumKey() << std::endl;
        std::cout << "Sorted Tree: Minimum value = " << sorted_tree->getMinimumKey() << std::endl;

        std::vector<int> sorted_tree_occs = sorted_tree->get_leaves_occupancy();
        std::cout << "Sorted Tree Leaf Occupancy: ";
        for (auto i : sorted_tree_occs) {
            std::cout << i << " ";
        }
        std::cout << std::endl;

        unsorted_tree->fanout();
        std::cout << "Unsorted Tree: number of splitting leaves = " << unsorted_tree->traits.leaf_splits
            << std::endl;
        std::cout << "Unsorted Tree: number of splitting internal nodes = " << 
            unsorted_tree->traits.internal_splits << std::endl;
        std::cout << "Unsorted Tree: number of leaves = " << unsorted_tree->traits.num_leaf_nodes 
            << std::endl;
        std::cout << "Unsorted Tree: number of internal nodes = " << 
            unsorted_tree->traits.num_internal_nodes << std::endl;
        std::cout << "Unsorted Tree: Maximum value = " << unsorted_tree->getMaximumKey() << std::endl;
        std::cout << "Unsorted Tree: Minimum value = " << unsorted_tree->getMinimumKey() << std::endl;

        std::vector<int> unsorted_tree_occs = unsorted_tree->get_leaves_occupancy();
        std::cout << "Unsorted Tree Leaf Occupancy: ";
        for (auto i : unsorted_tree_occs) {
            std::cout << i << " ";
        }
        std::cout << std::endl;

        
    }

    static void show_tree_knobs()
    {
        std::cout << "B Epsilon Tree Knobs:" << std::endl;
        std::cout << "Number of Upserts = " << _betree_knobs::NUM_UPSERTS << std::endl;
        std::cout << "Number of Pivots = " << _betree_knobs::NUM_PIVOTS << std::endl;
        std::cout << "Number of Children = " << _betree_knobs::NUM_CHILDREN << std::endl;
        std::cout << "Number of Data pairs = " << _betree_knobs::NUM_DATA_PAIRS << std::endl;
#ifdef UNITTEST

#else
        std::cout << "Block Size = " << _betree_knobs::BLOCK_SIZE << std::endl;
        std::cout << "Data Size = " << _betree_knobs::DATA_SIZE << std::endl;
        std::cout << "Block Size = " << _betree_knobs::BLOCK_SIZE << std::endl;
        std::cout << "Metadata Size = " << _betree_knobs::METADATA_SIZE << std::endl;
        std::cout << "Unit Size = " << _betree_knobs::UNIT_SIZE << std::endl;
        std::cout << "Pivots Size = " << _betree_knobs::PIVOT_SIZE << std::endl;
        std::cout << "Buffer Size = " << _betree_knobs::BUFFER_SIZE << std::endl;
#endif
        std::cout << "--------------------------------------------------------------------------" << std::endl;

        std::cout << "Dual Tree Knobs:" << std::endl;
        std::cout << "Sorted tree split fraction = " << _dual_tree_knobs::SORTED_TREE_SPLIT_FRAC << std::endl;
        std::cout << "Unsorted tree split fraction = " << _dual_tree_knobs::UNSORTED_TREE_SPLIT_FRAC << std::endl;

        std::cout << "--------------------------------------------------------------------------" << std::endl;
    }

    unsigned long long get_sorted_tree_true_size() {return sorted_tree->getNumKeys();}

    unsigned long long get_unsorted_tree_true_size() {return unsorted_tree->getNumKeys();}
    
private:

    _key _get_required_minimum_inserted_key(bool& no_lower_bound) {
        // std::cout << (sorted_tree->more_than_one_leaf() && !sorted_tree->is_tail_leaf_empty()) << std::endl;
        if(sorted_tree->more_than_one_leaf()){
            no_lower_bound = false;

            if (sorted_tree->is_tail_leaf_empty()) {
                return sorted_tree->get_prev_tail_maximum_key();
            } else {
                return sorted_tree->get_minimum_key_of_tail_leaf();
            }
        } else {
            // Since there is only 1 leaf in the sorted tree, no lower bound for insertion range.
            no_lower_bound = true;
            // return a random value;
            return sorted_tree->getMaximumKey();
        }
    }
    
};

#endif