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

template <typename key_type, typename value_type>
class dual_tree : public FastAppendTree<key_type, value_type>
{
    using super = FastAppendTree<key_type, value_type>;
    using node_t = bp_node<key_type, value_type>;

    // tree to accept outlier data.
    FastAppendTree<key_type, value_type> outlier_tree;
#ifdef DUAL_FILTERS
    BambooFilter bf1;
    BambooFilter bf2;
#endif
    OutlierDetector<key_type> *outlier_detector;
    OutlierDetector<key_type> *obvious_detector;
#ifdef ENABLE_HEAP
    Heap<key_type, value_type> *heap_buffer;
#endif
    bool lazy_move;

    // metadata for keeping counters of lazy moves and delta redirects
    uint32_t ctr_lazy_move;
    uint32_t ctr_obvious; // counter for the outer outlier detector
    uint32_t ctr_outlier;
    uint32_t ctr_direct; // counter for direct insert to outlier tree
    uint32_t ctr_outlier_tree_update;
    uint32_t ctr_sorted_tree_update;

public:
    // Default constructor, disable the buffer.
    dual_tree(const char *sorted_file, const char *outlier_file, const Config &config) : super(sorted_file, config.blocks_in_memory / 2, config.sorted_tree_split_frac),
                                                                                         outlier_tree(outlier_file, config.blocks_in_memory / 2, config.unsorted_tree_split_frac)
#ifdef DUAL_FILTERS
                                                                                         , bf1(65536, 2), bf2(65536, 2)
#endif
    {
#ifdef ENABLE_HEAP
        heap_buffer = config.get_heap_buffer<key_type, value_type>();
#endif
        obvious_detector = config.get_obvious_detector<key_type>();
        outlier_detector = config.get_outlier_detector<key_type>();
        lazy_move = config.enable_lazy_move; // right now we want the outlier detector always if we are using lazy move (which is actually lazy swap)
#ifdef DUAL_FILTERS
        std::cout << "DUAL FILTERS STRATEGY " << DUAL_FILTERS << std::endl;
#endif

        // initialize counters for lazy move and outlier detector
        ctr_direct = 0;
        ctr_obvious = 0;
        ctr_lazy_move = 0;
        ctr_outlier = 0;
        ctr_outlier_tree_update = 0;
        ctr_sorted_tree_update = 0;
    }

    ~dual_tree()
    {
#ifdef ENABLE_HEAP
        delete heap_buffer;
#endif
        delete outlier_detector;
        delete obvious_detector;
    }

    std::ostream &get_stats(std::ostream &os) const override
    {
        os << "DUAL"
           << ", " << super::size << ", " << super::depth << ", " << super::manager.getNumWrites() << ", " << super::ctr_fast_inserts
           << ", " << outlier_tree.size << ", " << outlier_tree.depth << ", " << outlier_tree.manager.getNumWrites() << ", " << outlier_tree.ctr_fast_inserts
           << ", " << ctr_lazy_move << ", " << ctr_obvious << ", " << ctr_outlier << ", " << ctr_direct
           << ", " << ctr_sorted_tree_update << ", " << ctr_outlier_tree_update;
        return os;
    }

    /**
     * @param key
     * @param value
     * @return true
     */
    bool insert(const key_type &key, const value_type &value) override
    {
#if DUAL_FILTERS == 1
        if (bf1.Lookup(&key, sizeof(key_type)) || bf2.Lookup(&key, sizeof(key_type)))
        {
            bf2.Insert(&key, sizeof(key_type));
            outlier_tree.insert(key, value);
            ctr_outlier_tree_update++;
            return true;
        }
#elif DUAL_FILTERS == 2
        if (super::size > 0 && super::tree_min <= key && key <= super::tree_max &&
            bf1.Lookup(&key, sizeof(key_type)) && super::update(key, value))
        {
            ctr_sorted_tree_update++;
            return true;
        }
        if (outlier_tree.size > 0 && outlier_tree.tree_min <= key && key <= outlier_tree.tree_max &&
            bf2.Lookup(&key, sizeof(key_type)) && outlier_tree.update(key, value))
        {
            ctr_outlier_tree_update++;
            return true;
        }
#endif
        sortedness_insert(key, value);
        return true;
    }

    void sortedness_insert(key_type key, value_type value)
    {
#ifdef ENABLE_HEAP
        if (heap_buffer)
        {
            // push to heap buffer (if it is enabled)
            if (heap_buffer->size() < heap_buffer->max_size)
            {
                heap_buffer->push(key, value);
                return;
            }
            // if buffer is full, pop the key of min heap
            std::pair<key_type, value_type> heap_min = heap_buffer->top();
            if (key > heap_min.first)
            {
                heap_buffer->pop();
                heap_buffer->push(key, value);
                key = heap_min.first;
                value = heap_min.second;
            }
        }
#endif
        if (super::size == 0)
        {
            // if the tree is empty, insert directly to sorted tree
#ifdef DUAL_FILTERS
            bf1.Insert(&key, sizeof(key_type));
#endif
            super::insert(key, value);
            return;
        }

        if (super::root_id != super::tail_id && key <= super::tail_greater_than)
        {
            // when key is smaller than tail_greater_than, insert directly to unsorted tree
#ifdef DUAL_FILTERS
            bf2.Insert(&key, sizeof(key_type));
#endif
            outlier_tree.insert(key, value);
            ctr_direct++;
            return;
        }

        if (obvious_detector && key > super::tree_max && obvious_detector->is_outlier(key, super::tail_greater_than))
        {
            // insert outlier key to unsorted tree
#ifdef DUAL_FILTERS
            bf2.Insert(&key, sizeof(key_type));
#endif
            outlier_tree.insert(key, value);

            // we caught an outlier using the global outlier detector, so increment its counter
            ctr_obvious++;
            return;
        }

        node_t tail(super::manager.open_block(super::tail_id));
        if (lazy_move && tail.info->size == node_t::leaf_capacity)
        {
            if (key < super::tree_max)
            {
                super::size--;
                tail.info->size--;
                assert(tail.keys[tail.info->size] == super::tree_max);
                assert(tail.info->size > 0);
                super::tree_max = tail.keys[tail.info->size - 1];
                key_type max_key = tail.keys[tail.info->size];
                value_type max_value =  tail.values[tail.info->size];
                super::leaf_insert(tail, key, value);
#ifdef DUAL_FILTERS
                bf1.Delete(&max_key, sizeof(key_type));
                bf1.Insert(&key, sizeof(key_type));
#endif
                // make life easy by re-assigning key to the max swapped
                key = max_key;
                value = max_value;

                // this was a lazy swap so increment that counter; this counter also signifies number of local outlier detector catches
                ctr_lazy_move++;
            }
            if (outlier_detector && outlier_detector->has_outlier(tail.keys, tail.info->size))
            {
#ifdef DUAL_FILTERS
                bf2.Insert(&key, sizeof(key_type));
#endif
                outlier_tree.insert(key, value);

                // this was a lazy swap so increment that counter; this counter also signifies number of local outlier detector catches
                ctr_outlier++;
                return;
            }
            // we know if we reach here, there will be a split
            if (obvious_detector) {
                obvious_detector->update(super::tree_max - tail.keys[0]);
            }
        }

#ifdef DUAL_FILTERS
        bf1.Insert(&key, sizeof(key_type));
#endif
        super::insert(key, value);
    }

    bool update(const key_type &key, const value_type &value) override
    {
        throw std::runtime_error("Dual update not implemented");
    }

    std::optional<value_type> get(const key_type &key) override
    {
#ifdef DUAL_FILTERS
        if (bf2.Lookup(&key, sizeof(key_type)))
        {
            std::optional<value_type> res = outlier_tree.get(key);
            if (res.has_value())
            {
                return res;
            }
        }
        if (bf1.Lookup(&key, sizeof(key_type)))
        {
            return super::get(key);
        }
        return std::nullopt;
#else
        if (super::size > outlier_tree.size)
        {
            std::optional<value_type> result = super::get(key);
            if (result.has_value())
            {
                return result;
            }
            return outlier_tree.get(key);
        }
        std::optional<value_type> result = outlier_tree.get(key);
        if (result.has_value())
        {
            return result;
        }
        return super::get(key);
#endif
    }
};

#endif
