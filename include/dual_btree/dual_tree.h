#ifndef DUAL_TREE_H
#define DUAL_TREE_H

#include <iostream>
#include <queue>

#include <collection.h>
#include <dual_btree/dual_tree_knob.h>
#include "../../src/betree.h"
#include "../../src/outlier_detector.h"

template<typename key_type, typename value_type>
class Heap {
    std::unordered_map<key_type, value_type> map;
    std::priority_queue<key_type, std::vector<key_type>, std::greater<key_type>> pq;
public:
    bool contains(const key_type &key) const {
        return map.find(key) != map.end();
    }

    std::pair<key_type, value_type> pop() {
        key_type key = pq.top();
        value_type value = map.at(key);
        pq.pop();
        map.erase(key);
        return {key, value};
    }

    std::pair<key_type, value_type> top() const {
        key_type key = pq.top();
        value_type value = map.at(key);
        return {key, value};
    }

    void push(key_type key, value_type value) {
        pq.push(key);
        map.insert({key, value});
    }

    size_t size() const {
        return map.size();
    }
};

template<typename key_type, typename value_type, typename _betree_knobs = BeTree_Default_Knobs<key_type, value_type>, typename _compare=std::less<key_type>>
class dual_tree : public collection<key_type, value_type> {
    // tree to accept unsorted input data.
    BeTree<key_type, value_type, _betree_knobs, _compare> *unsorted_tree;
    // tree to accept sorted input data.
    BeTree<key_type, value_type, _betree_knobs, _compare> *sorted_tree;

    uint sorted_size;

    uint unsorted_size;

    Heap<key_type, value_type> *heap_buffer;

    OutlierDetector<key_type, value_type> *outlier_detector;

#ifdef COUNTER
    int call_counter;
#endif

public:
    void add(const key_type &key, const value_type &value) override {
        insert(key, value);
    }

    bool contains(const key_type &key) override {
        return query(key);
    }

    // Default constructor, disable the buffer.
    dual_tree(const std::string &unsorted_tree_path, const std::string &sorted_tree_path, const std::string &root_dir,
              const std::string &config_path = "./config.toml") {
        DUAL_TREE_KNOBS::CONFIG_FILE_PATH = config_path;
        unsorted_tree = new BeTree<key_type, value_type, _betree_knobs, _compare>(unsorted_tree_path, root_dir,
                                                                                  _betree_knobs::BLOCK_SIZE,
                                                                                  _betree_knobs::BLOCKS_IN_MEMORY,
                                                                                  DUAL_TREE_KNOBS::UNSORTED_TREE_SPLIT_FRAC());
        sorted_tree = new BeTree<key_type, value_type, _betree_knobs, _compare>(sorted_tree_path, root_dir,
                                                                                _betree_knobs::BLOCK_SIZE,
                                                                                _betree_knobs::BLOCKS_IN_MEMORY,
                                                                                DUAL_TREE_KNOBS::SORTED_TREE_SPLIT_FRAC());
        sorted_size = 0;
        unsorted_size = 0;

        if (DUAL_TREE_KNOBS::HEAP_SIZE() > 0) {
            // initialize heap buffer if the tuning knob::heap size > 0
            heap_buffer = new Heap<key_type, value_type>();
        } else {
            heap_buffer = nullptr;
        }
        outlier_detector = DUAL_TREE_KNOBS::get_detector<key_type, value_type>();
    }

    ~dual_tree() {
        delete sorted_tree;
        delete unsorted_tree;
        delete heap_buffer;
        delete outlier_detector;
    }

    bool insert(key_type key, value_type value) {
        if (heap_buffer) {
            // push to heap buffer (if it is enabled)
            if (heap_buffer->size() < DUAL_TREE_KNOBS::HEAP_SIZE()) {
                heap_buffer->push(key, value);
                return true;
            } else {
                // if buffer is full, pop the key of min heap
                std::pair<key_type, value_type> heap_min = heap_buffer->top();
                if (key > heap_min.first) {
                    heap_buffer->pop();
                    heap_buffer->push(key, value);
                    key = heap_min.first;
                    value = heap_min.second;
                }
            }
        }

        if (sorted_tree->more_than_one_leaf()) {
            key_type tail_min;
            if (sorted_tree->is_tail_leaf_empty()) {
                tail_min = sorted_tree->get_prev_tail_maximum_key();
            } else {
                tail_min = sorted_tree->get_minimum_key_of_tail_leaf();
            }
            if (key < tail_min) {
                // when key is smaller than tail_min, insert directly to unsorted tree
                unsorted_tree->insert(key, value);
                unsorted_size++;
                return true;
            }
        }

        // insert current key to sorted tree if it passes outlier check
        // note that we only set outlier check for key >= tail_max

        if (outlier_detector && outlier_detector->is_outlier(key)) {
            // insert outlier key to unsorted tree
            unsorted_tree->insert(key, value);
            unsorted_size++;
            return true;
        }

        bool append = key >= sorted_tree->getMaximumKey();
        if (append || !DUAL_TREE_KNOBS::ENABLE_LAZY_MOVE()) {
            // If the new key will be appended or lazy move is disabled, we use the insert method.
            bool split;
            sorted_tree->insert_to_tail_leaf(key, value, append, split);
            if (split && outlier_detector) {
                outlier_detector->update(sorted_tree);
            }
            sorted_size++;
        } else {
#ifdef COUNTER
            call_counter++;
#endif
            std::pair<key_type, value_type> swapped = sorted_tree->swap_in_tail_leaf(key, value);
            unsorted_tree->insert(swapped.first, swapped.second);
            unsorted_size++;
        }
        return true;
    }

    bool query(key_type key) const {
        // search the buffer
        if (heap_buffer && heap_buffer->contains(key)) {
            return true;
        }
        // Search the one with more tuples at first.
        if (sorted_size > unsorted_size) {
            return sorted_tree->query(key) || unsorted_tree->query(key);
        } else {
            return unsorted_tree->query(key) || sorted_tree->query(key);
        }
    }

    std::ostream &get_stats(std::ostream &os) const override {
        os << sorted_size << ", " << unsorted_size << ", " << sorted_tree->depth() << ", " << unsorted_tree->depth()
           << ", " << sorted_tree->getNumWrites() << ", " << unsorted_tree->getNumWrites()
           #ifdef COUNTER
           << ", " << call_counter
#endif
                ;
        return os;
    }

};

#endif