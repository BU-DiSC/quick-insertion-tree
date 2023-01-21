#ifndef DUAL_BTREE_FASTAPPENDBETREE_H
#define DUAL_BTREE_FASTAPPENDBETREE_H

#include "betree.h"

template<typename key_type, typename value_type>
class FastAppendBeTree : public BeTree<key_type, value_type> {
    using super = BeTree<key_type, value_type>;
public:
    FastAppendBeTree(const std::string &_name, const std::string &_rootDir, unsigned long long _size_of_each_block,
                     uint _blocks_in_memory, float split_frac = 0.5) : super(_name, _rootDir, _size_of_each_block,
                                                                             _blocks_in_memory, split_frac) {}

    std::ostream &get_stats(std::ostream &os) const override {
        os << "FAST, " << super::depth() << ", " << super::getNumWrites();
        return os;
    }

    void add(const key_type &key, const value_type &value) override {
        if (super::more_than_one_leaf()) {
            key_type tail_min;
            if (super::is_tail_leaf_empty()) {
                tail_min = super::get_prev_tail_maximum_key();
            } else {
                tail_min = super::get_minimum_key_of_tail_leaf();
            }
            if (key < tail_min) {
                // when key is smaller than tail_min, insert normally
                super::insert(key, value);
                return;
            }
        }
        bool append = key >= super::getMaximumKey();
        super::insert_to_tail_leaf(key, value, append);
    }
};

#endif
