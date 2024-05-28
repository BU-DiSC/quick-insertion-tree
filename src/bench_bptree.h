#ifndef BENCH_BPTREE_H
#define BENCH_BPTREE_H

#include "bptree/bp_tree.h"
#include "index_bench.h"

namespace index_bench {
    template<typename KEY_TYPE, typename VALUE_TYPE>
    class BPTreeIndex final : public Index<KEY_TYPE, VALUE_TYPE> {
        using dist_f = std::size_t (*)(const KEY_TYPE &, const KEY_TYPE &);

    public:
        bp_tree<KEY_TYPE, VALUE_TYPE> _tree;

        BPTreeIndex(dist_f cmp, BlockManager &m) : _tree(cmp, m) {
        }

        bool contains(KEY_TYPE key) override { return _tree.contains(key); }

        void insert(KEY_TYPE key, VALUE_TYPE value) override {
            _tree.insert(key, value);
        }

        size_t select_k(size_t count, const KEY_TYPE &min_key) override {
            return _tree.select_k(count, min_key);
        }

        void print(std::ostream &os) const override { os << _tree; }
    };
}

#endif
