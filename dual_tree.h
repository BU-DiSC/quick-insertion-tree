#ifndef DUALTREE_H
#define DUEALTREE_H
#include "betree.h"
#include <stdlib.h>

template<typename _key, typename _value>
class DUAL_TREE_KNOBS
{
public:
    // Sorted tree split fraction, it will affect the space utilization of the tree. It means how
    // many elements will stay in the original node.
    static constexpr float SORTED_TREE_SPLIT_FRAC = 1.0F;

    // Unsorted tree splitting fraction.
    static constexpr float UNSORTED_TREE_SPLIT_FRAC = 0.5F;

};

template <typename _key, typename _value, typename _dual_tree_knobs=DUAL_TREE_KNOBS<_key, _value>,
            typename _betree_knobs = BeTree_Default_Knobs<_key, _value>, 
            typename _compare=std::less<_key>>
class dual_tree
{
    // Left tree to accept unsorted input data.
    BeTree<_key, _value, _betree_knobs, _compare> *unsorted_tree;
    // Right tree to accept sorted input data.
    BeTree<_key, _value, _betree_knobs, _compare> *sorted_tree;

    uint sorted_size;

    uint unsorted_size;

public:

    // Default constructor, disable the buffer.
    dual_tree(std::string tree_path_1, std::string tree_path_2)
    {   
        unsorted_tree = new BeTree<_key, _value, _betree_knobs, _compare>("manager", tree_path_1, 
    _betree_knobs::BLOCK_SIZE, _betree_knobs::BLOCKS_IN_MEMORY, _dual_tree_knobs::UNSORTED_TREE_SPLIT_FRAC);
        sorted_tree = new BeTree<_key, _value, _betree_knobs, _compare>("manager", tree_path_2, 
    _betree_knobs::BLOCK_SIZE, _betree_knobs::BLOCKS_IN_MEMORY, _dual_tree_knobs::SORTED_TREE_SPLIT_FRAC);
        sorted_size = 0;
        unsorted_size = 0;

    }

    // Deconstructor
    ~dual_tree()
    {
        delete sorted_tree;
        delete unsorted_tree;
    }

    uint sorted_tree_size() { return sorted_size;}

    uint unsorted_tree_size() { return unsorted_size;}

    bool insert(_key key, _value value)
    {
        _key inserted_key = key;
        _value inserted_value = value;
        bool no_lower_bound = false;
        _key lower_bound = _get_required_minimum_inserted_key(no_lower_bound);
        bool less_than_lower_bound = !no_lower_bound && inserted_key < lower_bound; 
        if(less_than_lower_bound)
        {
            unsorted_tree->insert(inserted_key, inserted_value);
            unsorted_size += 1;
        }
        else
        {
            // When _dual_tree_knobs::ALLOW_SORTED_TREE_INSERTION is false, @append is always true.
            bool append = inserted_key >= sorted_tree->getMaximumKey();
            sorted_tree->insert_to_tail_leaf(inserted_key, inserted_value, append);
            sorted_size += 1;
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
