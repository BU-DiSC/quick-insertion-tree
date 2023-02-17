#ifndef FAST_APPEND_TREE_H
#define FAST_APPEND_TREE_H

#include "bp_tree.h"

template <typename key_type, typename value_type>
class FastAppendTree : public bp_tree<key_type, value_type>
{
    using super = bp_tree<key_type, value_type>;
    using node_t = bp_node<key_type, value_type>;

public:
    FastAppendTree(const char *filepath, uint32_t blocks_in_memory, float split_frac = 0.8) : super(filepath, blocks_in_memory, split_frac) {}

    std::ostream &get_stats(std::ostream &os) const override
    {
        os << "FAST, " << super::size << ", " << super::depth << ", " << super::manager.getNumWrites();
        return os;
    }

    bool insert(const key_type &key, const value_type &value) override
    {
        if (super::root_id != super::tail_id && key >= super::tail_atleast)
        {
            node_t tail(super::manager.open_block(super::tail_id));
            return super::leaf_insert(tail, key, value);
        }
        return super::insert(key, value);
    }

    bool update(const key_type &key, const value_type &value) override
    {
        if (super::root_id != super::tail_id && key >= super::tail_atleast)
        {
            node_t tail(super::manager.open_block(super::tail_id));
            return super::leaf_update(tail, key, value);
        }
        return super::update(key, value);
    }

    size_t get_tail_leaf_size()
    {
        node_t tail(super::manager.open_block(super::tail_id));
        return tail.info->size;
    }
};

#endif
