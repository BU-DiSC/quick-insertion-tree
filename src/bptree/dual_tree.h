#ifndef DUAL_TREE_H
#define DUAL_TREE_H

#include <iostream>
#include <queue>
#include <unordered_map>

#ifdef DUAL_FILTERS
#include "bamboofilter.h"
#endif
#include "config.h"
#include "fast_append_tree.h"

template<typename key_type, typename value_type>
class dual_tree : public FastAppendTree<key_type, value_type> {
    using super = FastAppendTree<key_type, value_type>;
    using node_t = bp_node<key_type, value_type>;

    // tree to accept outlier data.
    FastAppendTree<key_type, value_type> outlier_tree;
#ifdef DUAL_FILTERS
    BambooFilter bf1;
    BambooFilter bf2;
#endif
    OutlierDetector<key_type> *outlier_detector;
    Heap<key_type, value_type> *heap_buffer;
    bool lazy_move;

protected:
    void update_stats(const node_t &leaf) override {
        if (outlier_detector) {
            outlier_detector->update(leaf.keys, leaf.info->size);
        }
    }

public:
    // Default constructor, disable the buffer.
    dual_tree(const char *sorted_file, const char *outlier_file, const Config &config) :
            super(sorted_file, config.blocks_in_memory / 2, config.sorted_tree_split_frac),
            outlier_tree(outlier_file, config.blocks_in_memory / 2, config.unsorted_tree_split_frac)
#ifdef DUAL_FILTERS
            , bf1(65536, 2), bf2(65536, 2)
#endif
    {
        heap_buffer = config.get_heap_buffer<key_type, value_type>();
        outlier_detector = config.get_detector<key_type>();
        lazy_move = config.enable_lazy_move;
#ifdef DUAL_FILTERS
        std::cout << "DUAL FILTERS STRATEGY " << DUAL_FILTERS << std::endl;
#endif
    }

    ~dual_tree() {
        delete heap_buffer;
        delete outlier_detector;
    }

    std::ostream &get_stats(std::ostream &os) const override {
        os << "DUAL" << ", " << super::size << ", " << super::depth << ", " << super::manager.getNumWrites()
           << ", " << outlier_tree.size << ", " << outlier_tree.depth << ", " << outlier_tree.manager.getNumWrites();
        return os;
    }

    /**
     * @param key
     * @param value
     * @return true
     */
    bool insert(const key_type &key, const value_type &value) override {
#if DUAL_FILTERS == 1
        if (bf1.Lookup(&key, sizeof(key_type)) || bf2.Lookup(&key, sizeof(key_type))) {
            bf2.Insert(&key, sizeof(key_type));
            outlier_tree.insert(key, value);
            return true;
        }
#elif DUAL_FILTERS == 2
        if (bf1.Lookup(&key, sizeof(key_type)) && super::update(key, value)) {
            return true;
        }
        if (bf2.Lookup(&key, sizeof(key_type)) && outlier_tree.update(key, value)) {
            return true;
        }
#endif
        sortedness_insert(key, value);
        return true;
    }

    void sortedness_insert(key_type key, value_type value) {
        if (heap_buffer) {
            // push to heap buffer (if it is enabled)
            if (heap_buffer->size() < heap_buffer->max_size) {
                heap_buffer->push(key, value);
                return;
            }
            // if buffer is full, pop the key of min heap
            std::pair<key_type, value_type> heap_min = heap_buffer->top();
            if (key > heap_min.first) {
                heap_buffer->pop();
                heap_buffer->push(key, value);
                key = heap_min.first;
                value = heap_min.second;
            }
        }

        if (super::size == 0) {
            // if the tree is empty, insert directly to sorted tree
#ifdef DUAL_FILTERS
            bf1.Insert(&key, sizeof(key_type));
#endif
            super::insert(key, value);
            if (outlier_detector) {
                outlier_detector->init(key);
            }
            return;
        }

        if (key < super::tail_min) {
            // when key is smaller than tail_min, insert directly to unsorted tree
#ifdef DUAL_FILTERS
            bf2.Insert(&key, sizeof(key_type));
#endif
            outlier_tree.insert(key, value);
            return;
        }

        // insert current key to sorted tree if it passes outlier check
        // note that we only set outlier check for key > tail_max
        if (outlier_detector && key > super::tail_max && outlier_detector->is_outlier(key)) {
            // insert outlier key to unsorted tree
#ifdef DUAL_FILTERS
            bf2.Insert(&key, sizeof(key_type));
#endif
            outlier_tree.insert(key, value);
            return;
        }

        if (lazy_move && key < super::tail_max && super::size == node_t::leaf_capacity) {
            std::pair<key_type, value_type> max_kv = swap_max(key, value);
#ifdef DUAL_FILTERS
            bf1.Insert(&key, sizeof(key_type));
            bf1.Delete(&max_kv.first, sizeof(key_type));
            bf2.Insert(&max_kv.first, sizeof(key_type));
#endif
            outlier_tree.insert(max_kv.first, max_kv.second);
            return;
        }

#ifdef DUAL_FILTERS
        bf1.Insert(&key, sizeof(key_type));
#endif
        super::insert(key, value);
    }

    std::pair<key_type, value_type> swap_max(const key_type &key, const value_type &value) {
        node_t tail(super::manager.open_block(super::tail_id));
        super::size--;
        tail.info->size--;
        assert(tail.keys[tail.info->size] == super::tail_max);
        super::tail_max = tail.keys[tail.info->size - 1];
        const std::pair<key_type, value_type> &max_kv = {tail.keys[tail.info->size], tail.values[tail.info->size]};
        super::leaf_insert(tail, key, value);
        return max_kv;
    }

    bool update(const key_type &key, const value_type &value) override {
        throw std::runtime_error("Not implemented");
    }

    std::optional<value_type> get(const key_type &key) override {
#ifdef DUAL_FILTERS
        if (bf2.Lookup(&key, sizeof(key_type))) {
            std::optional<value_type> res = outlier_tree.get(key);
            if (res.has_value()) {
                return res;
            }
        }
        if (bf1.Lookup(&key, sizeof(key_type))) {
            return super::get(key);
        }
        return std::nullopt;
#else
        if (super::size > outlier_tree.size) {
            std::optional<value_type> result = super::get(key);
            if (result.has_value()) {
                return result;
            }
            return outlier_tree.get(key);
        }
        std::optional<value_type> result = outlier_tree.get(key);
        if (result.has_value()) {
            return result;
        }
        return super::get(key);
#endif
    }
};

#endif
