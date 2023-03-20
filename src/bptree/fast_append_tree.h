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
        if (super::size > 0)
        {
            node_t last_insert_leaf(super::manager.open_block(super::last_insert_id));
            if (last_insert_leaf.keys[0] <= key && key <= last_insert_leaf.keys[last_insert_leaf.info->size - 1])
            {
                ctr_tail_appends++;
                return super::leaf_insert(last_insert_leaf, key, value);
            }
        }
        return super::insert(key, value);
    }

    bool update(const key_type &key, const value_type &value) override
    {
        if (super::size > 0)
        {
            node_t last_insert_leaf(super::manager.open_block(super::last_insert_id));
            if (last_insert_leaf.keys[0] <= key && key <= last_insert_leaf.keys[last_insert_leaf.info->size - 1])
            {
                return super::leaf_update(last_insert_leaf, key, value);
            }
        }
        return super::update(key, value);
    }
};
#undef LIL_FAT
#endif
