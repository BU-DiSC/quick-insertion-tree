#ifndef BENCH_SABTREE_H
#define BENCH_SABTREE_H
#include "sware/satree.h"
#include "sware/swareBuffer.h"

namespace index_bench {
template <typename KEY_TYPE, typename VALUE_TYPE>
class SATreeIndex final : public Index<KEY_TYPE, VALUE_TYPE> {
    using dist_f = std::size_t (*)(const KEY_TYPE &, const KEY_TYPE &);

   public:
    OsmTree<KEY_TYPE, VALUE_TYPE> _tree;

    SATreeIndex(dist_f cmp, BlockManager &m, uint num_buffer_entries,
                double fill_factor)
        : _tree(cmp, m, num_buffer_entries, fill_factor) {}

    bool contains(KEY_TYPE key) override { return _tree.osmQuery(key); }

    void insert(KEY_TYPE key, VALUE_TYPE value) override {
        _tree.osmInsert(key, value);
    }

    size_t top_k(size_t count, const KEY_TYPE &min_key) override { return 0; }

    void print(std::ostream &os) const override { os << _tree; }
};
}  // namespace index_bench
#endif