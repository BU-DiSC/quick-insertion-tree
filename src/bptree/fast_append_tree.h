#ifndef FAST_APPEND_TREE_H
#define FAST_APPEND_TREE_H
#define LIL_FAT
#include "bp_tree.h"

template <typename key_type, typename value_type>
class FastAppendTree : public bp_tree<key_type, value_type>
{
    using super = bp_tree<key_type, value_type>;
    using node_t = bp_node<key_type, value_type>;

public:
    uint32_t ctr_tail_appends;
    FastAppendTree(const char *filepath, uint32_t blocks_in_memory, float split_frac = 0.8) : super(filepath, blocks_in_memory, split_frac)
    {
        ctr_tail_appends = 0;
    }

    std::ostream &get_stats(std::ostream &os) const override
    {
        os << "FAST, " << super::size << ", " << super::depth << ", " << super::manager.getNumWrites() << ", " << ctr_tail_appends;
        return os;
    }

    bool insert(const key_type &key, const value_type &value) override
    {
        if ((super::leaf_min.has_value() && super::leaf_min > key) || (super::leaf_max.has_value() && key >= super::leaf_max))
        {
            return super::insert(key, value);
        }
        node_t last_insert_leaf(super::manager.open_block(super::last_insert_id));
        ctr_tail_appends++;
        return super::leaf_insert(last_insert_leaf, key, value);
    }

    bool update(const key_type &key, const value_type &value) override
    {
        if (super::leaf_min.has_value() && super::leaf_min > key || super::leaf_max.has_value() && key > super::leaf_max)
        {
            return super::update(key, value);
        }
        node_t last_insert_leaf(super::manager.open_block(super::last_insert_id));
        return super::leaf_update(last_insert_leaf, key, value);
    }
};

#undef LIL_FAT
#endif
