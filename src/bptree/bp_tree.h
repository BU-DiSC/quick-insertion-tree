#ifndef BP_TREE_H
#define BP_TREE_H

#include <optional>
#include <shared_mutex>
#include <mutex>
#include <vector>
#include <cassert>

#ifdef LOL_FAT
#ifdef FAST_PATH
#error "FAST_PATH already defined"
#endif
#define FAST_PATH
#include "outlier_detector.h"
#endif

#ifdef LIL_FAT
#ifdef FAST_PATH
#error "FAST_PATH already defined"
#endif
#define FAST_PATH
#endif

#ifdef TAIL_FAT
#ifdef FAST_PATH
#error "FAST_PATH already defined"
#endif
#define FAST_PATH
#endif

#ifdef INMEMORY

#include "memory_block_manager.h"

using BlockManager = InMemoryBlockManager;

#else

#include "disk_block_manager.h"

using BlockManager = DiskBlockManager;

#endif

#include "bp_node.h"

#ifndef MAX_DEPTH
#define MAX_DEPTH 10
#endif

struct reset_stats {
    std::atomic<uint8_t> fails;
    const uint8_t threshold;

    explicit reset_stats(uint8_t t) : threshold(t) {
        fails = 0;
    }

    void success() { fails = 0; }

    bool failure() {
        fails++;
        if (fails >= threshold) {
            return true;
        }
        return false;
    }

    void reset() { fails = 0; }
};

template <typename key_type, typename value_type>
class bp_tree {
    friend std::ostream &operator<<(std::ostream &os, const bp_tree &tree) {
        os << tree.ctr_size << ", " << +tree.ctr_depth << ", " << tree.manager
           << ", " << tree.ctr_internal << ", " << tree.ctr_leaves << ", "
           #ifdef REDISTRIBUTE
           << tree.ctr_redistribute
           #endif
           << ", "
           #ifdef LOL_FAT
           << tree.ctr_split
           #endif
           << ", "
           #ifdef LOL_FAT
           << tree.ctr_iqr
           #endif
           << ", "
           #ifdef LOL_FAT
           << tree.ctr_soft
           #endif
           << ", "
           #ifdef LOL_RESET
           << tree.ctr_hard
           #endif
           << ", "
           #ifdef FAST_PATH
           << tree.ctr_fp
#endif
                ;
        return os;
    }

    using node_id_t = uint32_t;
    using node_t = bp_node<node_id_t, key_type, value_type>;
    using dist_f = std::size_t (*)(const key_type &, const key_type &);
    // starts from leaf -> root and empty slots at the end for the tree to grow
    struct step {
        node_id_t id;
        uint16_t size;
    };
    using path_t = std::array<step, MAX_DEPTH>;

    static constexpr uint16_t SPLIT_INTERNAL_POS = node_t::internal_capacity / 2;
    static constexpr uint16_t SPLIT_LEAF_POS = (node_t::leaf_capacity + 1) / 2;
    static constexpr uint16_t IQR_SIZE_THRESH = SPLIT_LEAF_POS;
    static constexpr node_id_t INVALID_NODE_ID = -1;

    dist_f dist;

    BlockManager &manager;
    mutable std::vector<std::shared_mutex> mutexes;
    const node_id_t root_id;
    node_id_t head_id;
    node_id_t tail_id;
#ifdef FAST_PATH
    mutable std::shared_mutex fp_mutex;
    node_id_t fp_id;
    key_type fp_min;
    key_type fp_max;
    path_t fp_path;
#ifdef LOL_FAT
    node_id_t fp_prev_id;
#ifdef LOL_RESET
    reset_stats life;
#endif
    // iqr
    key_type fp_prev_min;
    uint16_t fp_prev_size;
    uint16_t fp_size;
#endif
#endif

    // stats
    std::atomic<uint32_t> ctr_size;
    std::atomic<uint8_t> ctr_depth;  // path[ctr_depth - 1] is the root
    std::atomic<uint32_t> ctr_internal;
    std::atomic<uint32_t> ctr_leaves;
#ifdef FAST_PATH
    std::atomic<uint32_t> ctr_fp;
#ifdef LOL_FAT
    std::atomic<uint32_t> ctr_split;
    std::atomic<uint32_t> ctr_iqr;
    std::atomic<uint32_t> ctr_soft;
#endif
#ifdef LOL_RESET
    std::atomic<uint32_t> ctr_hard;
#endif
#ifdef REDISTRIBUTE
    std::atomic<uint32_t> ctr_redistribute;
#endif
#endif

    void create_new_root(const key_type &key, node_id_t node_id) {
        node_id_t left_node_id = manager.allocate();
        node_t root;
        root.load(manager.open_block(root_id));
        node_t left_node;
        left_node.load(manager.open_block(left_node_id));
        std::memcpy(left_node.info, root.info, BLOCK_SIZE_BYTES);
        left_node.info->id = left_node_id;
        manager.mark_dirty(left_node_id);

        if (root.info->type == LEAF) {
            root.to_internal();
        }
        manager.mark_dirty(root_id);
        root.info->size = 1;
        root.keys[0] = key;
        root.children[0] = left_node_id;
        root.children[1] = node_id;
        if (root_id == head_id) {
            head_id = left_node_id;
        }
        uint8_t depth = ctr_depth;
#ifdef FAST_PATH
        if (fp_path[depth-1].id == root_id) {
            if (fp_id == root_id) {
                fp_id = left_node_id;
            }
            fp_path[depth-1].id = left_node_id;
        }
        fp_path[depth] = {root_id, 1};
#endif
        assert(depth + 1 < MAX_DEPTH);
        ctr_depth.fetch_add(1);
        ctr_internal.fetch_add(1, std::memory_order_relaxed);
        mutexes[root_id].unlock();
    }

    void find_leaf_shared(node_t &node, const key_type &key) const {
        node_id_t node_id = root_id;
        mutexes[root_id].lock_shared();
        node.load(manager.open_block(node_id));
        while (node.info->type == INTERNAL) {
            const node_id_t parent_id = node_id;
            const uint16_t slot = node.child_slot(key);
            node_id = node.children[slot];
            mutexes[node_id].lock_shared();
            mutexes[parent_id].unlock_shared();
            node.load(manager.open_block(node_id));
        }
    }

    void find_leaf_exclusive(node_t &node, path_t &path, const key_type &key, key_type &leaf_max) const {
        mutexes[root_id].lock();
        node.load(manager.open_block(root_id));

        uint8_t i = ctr_depth - 1;
        uint8_t locked = i;
        path[i] = {root_id, node.info->size};
        path[ctr_depth] = {INVALID_NODE_ID, 0};
        while (node.info->type == bp_node_type::INTERNAL) {
            uint16_t slot = node.child_slot(key);
            if (slot != node.info->size) {
                leaf_max = node.keys[slot];
            }
            node_id_t node_id = node.children[slot];
            mutexes[node_id].lock();
            node.load(manager.open_block(node_id));
            if (node.info->size < node_t::internal_capacity) {
                for (uint8_t j = locked; j > i - 1; --j) {
                    auto temp_id = path[j].id;
                    mutexes[temp_id].unlock();
                }
                locked = i - 1;
            }
            path[--i] = {node_id, node.info->size};
        }
    }

#ifdef FAST_PATH
    void lock_path() const {
        uint8_t locked = ctr_depth - 1;
        for (uint8_t i = locked; i > 0; --i) {
            auto node_id = fp_path[i].id;
            auto size = fp_path[i].size;
            mutexes[node_id].lock();
            if (size < node_t::internal_capacity) {
                for (uint8_t j = locked; j > i; --j) {
                    auto temp_id = fp_path[j].id;
                    mutexes[temp_id].unlock();
                }
                locked = i;
            }
        }
    }
#endif

#ifdef REDISTRIBUTE
    void update_internal(const path_t &path, const key_type &old_key,
                         const key_type &new_key) {
        node_t node;
        uint8_t depth = ctr_depth;
        for (uint8_t i = 1; i < depth; i++) {
            node_id_t node_id = path[i].id;
            node.load(manager.open_block(node_id));
            assert(node.info->id == node_id);
            assert(node.info->type == bp_node_type::INTERNAL);
            uint16_t index = node.child_slot(old_key) - 1;
            if (index < node.info->size && node.keys[index] == old_key) {
                manager.mark_dirty(node_id);
                node.keys[index] = new_key;
                return;
            }
        }
        assert(false);
    }
#endif

    void internal_insert(const path_t &path, key_type key, node_id_t child_id, uint16_t split_pos) {
        node_t node;
        uint8_t depth = ctr_depth;
        for (uint8_t i = 1; i < depth; i++) {
            node_id_t node_id = path[i].id;
            node.load(manager.open_block(node_id));
            assert(node.info->id == node_id);
            assert(node.info->type == bp_node_type::INTERNAL);
            uint16_t index = node.child_slot(key);
            assert(index == node.info->size || node.keys[index] != key);
            manager.mark_dirty(node_id);
            if (node.info->size < node_t::internal_capacity) {
                // insert new key
                std::memmove(node.keys + index + 1, node.keys + index, (node.info->size - index) * sizeof(key_type));
                std::memmove(node.children + index + 2, node.children + index + 1,
                             (node.info->size - index) * sizeof(uint32_t));
                node.keys[index] = key;
                node.children[index + 1] = child_id;
                ++node.info->size;
#ifdef FAST_PATH
                // update_paths
                if (fp_path[i].id == node_id) {
                    ++fp_path[i].size;
                }
#endif
                mutexes[node_id].unlock();
                return;
            }

            // split the node
            node_id_t new_node_id = manager.allocate();
            node_t new_node;
            new_node.init(manager.open_block(new_node_id), INTERNAL);
            manager.mark_dirty(new_node_id);
            ctr_internal.fetch_add(1, std::memory_order_relaxed);

            node.info->size = split_pos;
            new_node.info->id = new_node_id;
            new_node.info->size = node_t::internal_capacity - node.info->size;

            if (index < node.info->size) {
                std::memcpy(new_node.keys, node.keys + node.info->size,
                            new_node.info->size * sizeof(key_type));
                std::memmove(node.keys + index + 1, node.keys + index,
                             (node.info->size - index) * sizeof(key_type));
                node.keys[index] = key;
                std::memcpy(new_node.children, node.children + node.info->size,
                            (new_node.info->size + 1) * sizeof(uint32_t));
                std::memmove(node.children + index + 2,
                             node.children + index + 1,
                             (node.info->size - index + 1) * sizeof(uint32_t));
                node.children[index + 1] = child_id;

                key = node.keys[node.info->size];
                // key = new_node.keys[0];
            } else if (index == node.info->size) {
                std::memcpy(new_node.keys, node.keys + node.info->size,
                            new_node.info->size * sizeof(key_type));
                std::memcpy(new_node.children + 1,
                            node.children + 1 + node.info->size,
                            new_node.info->size * sizeof(uint32_t));
                new_node.children[0] = child_id;
            } else {
                std::memcpy(new_node.keys, node.keys + node.info->size + 1,
                            (index - node.info->size - 1) * sizeof(key_type));
                std::memcpy(
                        new_node.keys + index - node.info->size, node.keys + index,
                        (node_t::internal_capacity - index) * sizeof(key_type));
                new_node.keys[index - node.info->size - 1] = key;
                std::memcpy(new_node.children,
                            node.children + 1 + node.info->size,
                            (index - node.info->size) * sizeof(uint32_t));
                std::memcpy(new_node.children + 1 + index - node.info->size,
                            node.children + 1 + index,
                            new_node.info->size * sizeof(uint32_t));
                new_node.children[index - node.info->size] = child_id;

                key = node.keys[node.info->size];
            }
#ifdef FAST_PATH
            // update_paths
            if (fp_path[i].id == node_id && fp_id != head_id && key <= fp_min) {
                fp_path[i] = {new_node_id, new_node.info->size};
            }
#endif
            if (node_id != root_id) {
                mutexes[node_id].unlock();
            }
            child_id = new_node_id;
        }
        create_new_root(key, child_id);
    }

#ifdef REDISTRIBUTE
    void redistribute(const node_t &leaf, uint16_t index, const key_type &key,
                      const value_type &value) {
        assert(fp_prev_id != INVALID_NODE_ID);
        ctr_redistribute.fetch_add(1, std::memory_order_relaxed);
        // move values from leaf to leaf prev
        uint16_t items =
                IQR_SIZE_THRESH - fp_prev_size;  // items to be moved to fp prev
        manager.mark_dirty(fp_prev_id);
        node_t fp_prev;
        fp_prev.load(manager.open_block(fp_prev_id));
        assert(fp_prev_id == fp_prev.info->id);
        assert(fp_prev.info->type == bp_node_type::LEAF);
        if (index < items) {
            --items;
            std::memcpy(fp_prev.keys + fp_prev_size, leaf.keys,
                        index * sizeof(key_type));
            std::memcpy(fp_prev.keys + fp_prev_size + index + 1,
                        leaf.keys + index, (items - index) * sizeof(key_type));
            fp_prev.keys[fp_prev_size + index] = key;
            std::memcpy(fp_prev.values + fp_prev_size, leaf.values,
                        index * sizeof(value_type));
            std::memcpy(fp_prev.values + fp_prev_size + index + 1,
                        leaf.values + index,
                        (items - index) * sizeof(value_type));
            fp_prev.values[fp_prev_size + index] = value;

            std::memmove(leaf.keys, leaf.keys + items,
                         (fp_size - items) * sizeof(key_type));
            std::memmove(leaf.values, leaf.values + items,
                         (fp_size - items) * sizeof(value_type));
            ++items;
        } else {
            std::memcpy(fp_prev.keys + fp_prev_size, leaf.keys,
                        items * sizeof(key_type));
            std::memcpy(fp_prev.values + fp_prev_size, leaf.values,
                        items * sizeof(key_type));

            // move leaf.keys and leaf.values items positions to the left
            uint16_t new_index = index - items;
            std::memmove(leaf.keys, leaf.keys + items,
                         new_index * sizeof(key_type));
            std::memmove(leaf.keys + new_index + 1, leaf.keys + index,
                         (fp_size - index) * sizeof(key_type));
            leaf.keys[new_index] = key;
            std::memmove(leaf.values, leaf.values + items,
                         new_index * sizeof(value_type));
            std::memmove(leaf.values + new_index + 1, leaf.values + index,
                         (fp_size - index) * sizeof(value_type));
            leaf.values[new_index] = value;
        }

        // update parent for current leaf min
        update_internal(fp_path, fp_min, leaf.keys[0]);
        // update fp_min, fp_size
        fp_min = leaf.keys[0];
        fp_size = fp_size - items + 1;
        fp_prev_size = IQR_SIZE_THRESH;
        leaf.info->size = fp_size;
        fp_prev.info->size = IQR_SIZE_THRESH;
    }
#endif

    bool leaf_insert(node_t &leaf, const path_t &path, const key_type &key,
                     const value_type &value
#ifdef FAST_PATH
    , bool fast = false
#endif
                     ) {
        std::unique_lock leaf_lock(mutexes[leaf.info->id], std::adopt_lock);
        manager.mark_dirty(leaf.info->id);
        uint16_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key) {
            // update value
            leaf.values[index] = value;
            return false;
        }

        ctr_size.fetch_add(1, std::memory_order_relaxed);
        if (leaf.info->size < node_t::leaf_capacity) {
            // insert new key
            std::memmove(leaf.keys + index + 1, leaf.keys + index,
                         (leaf.info->size - index) * sizeof(key_type));
            std::memmove(leaf.values + index + 1, leaf.values + index,
                         (leaf.info->size - index) * sizeof(value_type));
            leaf.keys[index] = key;
            leaf.values[index] = value;
            ++leaf.info->size;
#ifdef LOL_FAT
            if (leaf.info->id == fp_id) {
                ++fp_size;
                ++fp_path[0].size;
            } else if (leaf.info->next_id == fp_id) {
                fp_prev_id = leaf.info->id;
                fp_prev_min = leaf.keys[0];
                fp_prev_size = leaf.info->size;
            }
#endif
            return true;
        }

        // how many elements should be on the left side after split
        uint16_t split_leaf_pos = SPLIT_LEAF_POS;
#ifdef LOL_FAT
#ifdef VARIABLE_SPLIT
        bool fp_move = false;
        if (leaf.info->id == fp_id) {
            if (fast) {
                lock_path();
            }
            // when splitting leaf, normally we would do it in the middle
            // but for fp we want to split it where IQR suggests
            if (fp_prev_id == INVALID_NODE_ID) {
                fp_move = true;  // move from head
            } else if (fp_prev_size >= IQR_SIZE_THRESH) {
                // If IQR has enough information
                size_t max_distance = IKR::upper_bound(
                        dist(fp_min, fp_prev_min), fp_prev_size, fp_size);
                uint16_t outlier_pos = leaf.value_slot2(fp_min + max_distance);
                if (outlier_pos <= SPLIT_LEAF_POS) {
                    split_leaf_pos =
                            outlier_pos;  // keep these good values on current fp
                    // and do not move
                } else {
                    // most of the values are certainly good
                    if (outlier_pos - 10 < SPLIT_LEAF_POS)
                        split_leaf_pos = SPLIT_LEAF_POS;
                    else
                        split_leaf_pos = outlier_pos - 10;
                    fp_move = true;  // also move fp
                }
                if (index < outlier_pos) {
                    split_leaf_pos++;  // this key will be also in the current
                    // leaf
                }
            } else {
#ifdef REDISTRIBUTE
                redistribute(leaf, index, key, value);
                return true;
#else
                fp_move = true;
#endif
            }
        }
#endif
#endif
        // split the leaf
        node_id_t new_leaf_id = manager.allocate();
        node_t new_leaf;
        new_leaf.init(manager.open_block(new_leaf_id), LEAF);
        manager.mark_dirty(new_leaf_id);
        ctr_leaves.fetch_add(1, std::memory_order_relaxed);

        assert(1 <= split_leaf_pos && split_leaf_pos <= node_t::leaf_capacity);
        leaf.info->size = split_leaf_pos;
        new_leaf.info->id = new_leaf_id;
        new_leaf.info->next_id = leaf.info->next_id;
        leaf.info->next_id = new_leaf_id;
        new_leaf.info->size = node_t::leaf_capacity + 1 - leaf.info->size;

        if (index < leaf.info->size) {
            // get one more since the new key goes left
            std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size - 1,
                        new_leaf.info->size * sizeof(key_type));
            std::memmove(leaf.keys + index + 1, leaf.keys + index,
                         (leaf.info->size - index - 1) * sizeof(key_type));
            leaf.keys[index] = key;
            std::memcpy(new_leaf.values, leaf.values + leaf.info->size - 1,
                        new_leaf.info->size * sizeof(value_type));
            std::memmove(leaf.values + index + 1, leaf.values + index,
                         (leaf.info->size - index - 1) * sizeof(value_type));
            leaf.values[index] = value;

#ifdef LIL_FAT
            // if we insert to left node of split, we set the lil max
            if (leaf.info->id == fp_id) {
                fp_max = new_leaf.keys[0];
            }
#endif
        } else {
            uint16_t new_index = index - leaf.info->size;
            std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size,
                        new_index * sizeof(key_type));
            new_leaf.keys[new_index] = key;
            std::memcpy(new_leaf.keys + new_index + 1, leaf.keys + index,
                        (node_t::leaf_capacity - index) * sizeof(key_type));
            std::memcpy(new_leaf.values, leaf.values + leaf.info->size,
                        new_index * sizeof(value_type));
            new_leaf.values[new_index] = value;
            std::memcpy(new_leaf.values + new_index + 1, leaf.values + index,
                        (node_t::leaf_capacity - index) * sizeof(value_type));
#ifdef LIL_FAT
            if (leaf.info->id == fp_id) {
                fp_id = new_leaf.info->id;
                // if we insert to right split node, we set leaf min
                fp_min = new_leaf.keys[0];
                fp_path[0] = {fp_id, new_leaf.info->size};
            }
#endif
        }
        if (leaf.info->id == tail_id) {
            tail_id = new_leaf_id;
#ifdef TAIL_FAT
            fp_min = new_leaf.keys[0];
            fp_path[0] = {new_leaf_id, new_leaf.info->size};
            fp_id = new_leaf_id;
#endif
        }
#ifdef LOL_FAT
        if (leaf.info->id == fp_id) {
            ctr_split.fetch_add(1, std::memory_order_relaxed);
#ifndef VARIABLE_SPLIT
            bool fp_move =
                fp_id == head_id ||  // move fp from head
                (fp_prev_size >= IQR_SIZE_THRESH &&
                 dist(new_leaf.keys[0], fp_min) <
                     IKR::upper_bound(dist(fp_min, fp_prev_min), fp_prev_size,
                                      leaf.info->size));
#endif
            if (fp_move) {
                ctr_iqr.fetch_add(1, std::memory_order_relaxed);
                // fp believes that the new leaf is not an outlier
                fp_prev_min = fp_min;
                fp_prev_size = leaf.info->size;
                fp_prev_id = fp_id;
                fp_id = new_leaf_id;
                fp_min = new_leaf.keys[0];
                fp_size = new_leaf.info->size;
                fp_path[0] = {fp_id, fp_size};
            } else {
                fp_max = new_leaf.keys[0];
                fp_size = leaf.info->size;
            }
        } else if (new_leaf.info->next_id == fp_id) {
            fp_prev_id = new_leaf_id;
            fp_prev_min = new_leaf.keys[0];
            fp_prev_size = new_leaf.info->size;
        }
#endif
        if (leaf.info->id == root_id) {
            leaf_lock.release();
        } else {
            leaf_lock.unlock();
        }
        // insert new key to parent
        internal_insert(path, new_leaf.keys[0], new_leaf_id, SPLIT_INTERNAL_POS);
        return true;
    }

public:
    bp_tree(dist_f cmp, BlockManager &m)
            : manager(m), mutexes(m.get_capacity()), root_id(m.allocate())
#ifdef LOL_RESET
            ,
              life(sqrt(node_t::leaf_capacity)),
              ctr_hard(0)
#endif
    {
        dist = cmp;
        head_id = tail_id = root_id;
#ifdef FAST_PATH
        fp_id = root_id;
        fp_path[0] = {fp_id, 0};
        fp_min = {};
        fp_max = {};
        ctr_fp = 0;
#endif
#ifdef LOL_FAT
        fp_prev_id = INVALID_NODE_ID;
        fp_prev_min = {};
        fp_prev_size = 0;
        fp_size = 0;
        ctr_split = 0;
        ctr_iqr = 0;
        ctr_soft = 0;
#endif
        node_t root;
        root.init(manager.open_block(root_id), LEAF);
        manager.mark_dirty(root_id);
        root.info->id = root_id;
        root.info->next_id = root_id;
        root.info->size = 0;

        ctr_size = 0;
        ctr_depth = 1;
        ctr_internal = 0;
        ctr_leaves = 1;
#ifdef REDISTRIBUTE
        ctr_redistribute = 0;
#endif
    }

    bool top_insert(const key_type &key, const value_type &value) {
        node_t leaf;
        path_t path;
        key_type leaf_max;
        find_leaf_exclusive(leaf, path, key, leaf_max);
        return leaf_insert(leaf, path, key, value);
    }

    std::string status() const {
        std::string res;
        for (int i = 0; i < ctr_leaves + ctr_internal; i++) {
            if (mutexes[i].try_lock()) {
                mutexes[i].unlock();
            } else {
                res += std::to_string(i) + " ";
            }
        }
        return res;
    }

    struct Status {
        const bp_tree *tree;
        Status(const bp_tree *tree) : tree(tree) {}
        ~Status() {
            std::string res = tree->status();
            if (!res.empty()) {
                std::cout << "Error: " << res << std::endl;
            }
        }
    };

    bool insert(const key_type &key, const value_type &value) {
//        Status s(this);
        node_t leaf;
#ifdef FAST_PATH
#ifdef PLOT_FAST
        std::cout << key << ',' << ctr_fp << std::endl;
#endif
        std::unique_lock fp_lock(fp_mutex);
        if ((fp_id == head_id || fp_min <= key) &&
            (fp_id == tail_id || key < fp_max)) {
            ctr_fp.fetch_add(1, std::memory_order_relaxed);
            mutexes[fp_id].lock();
            leaf.load(manager.open_block(fp_id));
            assert(fp_id == leaf.info->id);
            assert(leaf.info->type == bp_node_type::LEAF);
#ifdef LOL_RESET
            life.success();
#endif
            return leaf_insert(leaf, fp_path, key, value, true);
        }
#endif
#ifdef LIL_FAT
        path_t &path = fp_path;  // update fp_path
#else
        path_t path;
#endif
        key_type leaf_max;
        find_leaf_exclusive(leaf, path, key, leaf_max);
#ifdef LIL_FAT
        // update rest of lil
        fp_id = leaf.info->id;
        if (fp_id != head_id) fp_min = leaf.keys[0];
        if (fp_id != tail_id) fp_max = leaf_max;
#endif
#ifdef LOL_FAT
        // if the new inserted key goes to fp->next, check if fp->next is not
        // an outlier it might be the case that fp reached the previous
        // outliers.
        if (fp_prev_id != INVALID_NODE_ID &&  // fp->prev info exist
            //            fp_id != head_id &&
            //            // fp_min is valid
            fp_id != tail_id &&                // fp_max is valid
            //            leaf.info->id != tail_id && // don't go to tail
            fp_max == leaf.keys[0] &&  // leaf is fp->next
            // TODO: IQR doesn't have enough values but this kinda works
            // fp_prev_size >= IQR_SIZE_THRESH && fp_size >= IQR_SIZE_THRESH &&
            dist(fp_max, fp_min) < IKR::upper_bound(dist(fp_min, fp_prev_min), fp_prev_size, fp_size)) {
            // move fp to fp->next = leaf
            fp_prev_min = fp_min;
            fp_prev_size = fp_size;
            fp_prev_id = fp_id;
            fp_id = leaf.info->id;
            fp_min = fp_max;
            fp_max = leaf_max;
            fp_size = leaf.info->size;
            fp_path = path;
            ctr_soft.fetch_add(1, std::memory_order_relaxed);
#ifdef LOL_RESET
            life.reset();
        } else if (life.failure()) {
            ctr_hard.fetch_add(1, std::memory_order_relaxed);
            fp_prev_id = INVALID_NODE_ID;
            fp_id = leaf.info->id;
            fp_min = leaf.keys[0];
            fp_max = leaf_max;
            fp_size = leaf.info->size;
            fp_path = path;
            life.reset();
#endif
        }
#endif
#ifdef FAST_PATH
        fp_lock.unlock();
#endif
        return leaf_insert(leaf, path, key, value);
    }

    size_t select_k(size_t count, const key_type &min_key) const {
        assert(false);
        node_t leaf;
        find_leaf_shared(leaf, min_key);
        uint16_t index = leaf.value_slot(min_key);
        size_t loads = 1;
        uint16_t curr_size = leaf.info->size - index;
        while (count > curr_size) {
            count -= curr_size;
            if (leaf.info->id == tail_id) {
                break;
            }
            node_id_t next_id = leaf.info->next_id;
            mutexes[next_id].lock_shared();
            mutexes[leaf.info->id].unlock_shared();
            leaf.load(manager.open_block(next_id));
            assert(next_id == leaf.info->id);
            assert(leaf.info->type == bp_node_type::LEAF);
            curr_size = leaf.info->size;
            ++loads;
        }
        mutexes[leaf.info->id].unlock_shared();
        return loads;
    }

    size_t range(const key_type &min_key, const key_type &max_key) const {
        assert(false);
        size_t loads = 1;
        node_t leaf;
        find_leaf_shared(leaf, min_key);
        while (leaf.keys[leaf.info->size - 1] < max_key) {
            if (leaf.info->id == tail_id) {
                break;
            }
            node_id_t next_id = leaf.info->next_id;
            mutexes[next_id].lock_shared();
            mutexes[leaf.info->id].unlock_shared();
            leaf.load(manager.open_block(next_id));
            assert(next_id == leaf.info->id);
            assert(leaf.info->type == bp_node_type::LEAF);
            ++loads;
        }
        mutexes[leaf.info->id].unlock_shared();
        return loads;
    }

    std::optional<value_type> get(const key_type &key) const {
        node_t leaf;
        find_leaf_shared(leaf, key);
        uint16_t index = leaf.value_slot(key);
        std::optional<value_type> res = index < leaf.info->size && leaf.keys[index] == key
                                        ? std::make_optional(leaf.values[index])
                                        : std::nullopt;
        mutexes[leaf.info->id].unlock_shared();
        return res;
    }

    bool contains(const key_type &key) const { return get(key).has_value(); }
};

#endif
