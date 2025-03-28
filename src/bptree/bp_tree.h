#ifndef BP_TREE_H
#define BP_TREE_H

#include <algorithm>
#include <optional>
#include <cstring>

#ifdef LOL_FAT
#ifdef REDISTRIBUTE
#ifdef VARIABLE_SPLIT
#ifdef LOL_RESET
#define QUIT_FAT
#endif
#endif
#endif

#include <cmath>
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

#define MAX_DEPTH 10

struct reset_stats {
    uint8_t fails;
    uint8_t threshold;

    explicit reset_stats(uint8_t t) {
        fails = 0;
        threshold = t;
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

template<typename key_type, typename value_type>
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
        return ctr::log(os);
    }

    using node_id_t = uint32_t;
    using node_t = bp_node<node_id_t, key_type, value_type>;
    using dist_f = std::size_t (*)(const key_type &, const key_type &);
    // starts from leaf -> root and empty slots at the end for the tree to grow
    using path_t = std::array<node_id_t, MAX_DEPTH>;

    static constexpr uint16_t SPLIT_INTERNAL_POS = node_t::internal_capacity / 2;
    static constexpr uint16_t SPLIT_LEAF_POS = (node_t::leaf_capacity + 1) / 2;
    static constexpr uint16_t IQR_SIZE_THRESH = SPLIT_LEAF_POS;
    static constexpr node_id_t INVALID_NODE_ID = -1;

    BlockManager &manager;
    const node_id_t root_id;
    node_id_t head_id;
    node_id_t tail_id;
#ifdef FAST_PATH
    node_id_t fp_id;
    key_type fp_min;
    key_type fp_max;
    path_t fp_path;
#ifdef LOL_FAT
    dist_f dist;
    node_id_t lol_prev_id;
#ifdef LOL_RESET
    reset_stats life;
#endif
    // iqr
    key_type lol_prev_min;
    uint16_t lol_prev_size;
    uint16_t lol_size;
#endif
#endif

    // stats
    uint32_t ctr_size;
    uint8_t ctr_depth;  // path[ctr_depth - 1] is the root
    uint32_t ctr_internal;
    uint32_t ctr_leaves;
#ifdef FAST_PATH
    uint32_t ctr_fp;
#ifdef LOL_FAT
    uint32_t ctr_split;
    uint32_t ctr_iqr;
    uint32_t ctr_soft;
#endif
#ifdef LOL_RESET
    uint32_t ctr_hard;
#endif
#ifdef REDISTRIBUTE
    uint32_t ctr_redistribute;
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
#ifdef FAST_PATH
        if (fp_path[ctr_depth-1] == root_id) {
            if (fp_id == root_id) {
                fp_id = left_node_id;
            }
            fp_path[ctr_depth-1] = left_node_id;
        }
        fp_path[ctr_depth] = root_id;
#endif
        ctr_depth++;
        assert(ctr_depth < MAX_DEPTH);
        ctr_internal++;
    }

    key_type find_leaf(node_t &node, path_t &path, const key_type &key) const {
        key_type leaf_max = {};
        node_id_t child_id = root_id;
        for (uint8_t i = ctr_depth - 1; i > 0; --i) {
            // from root to last internal level
            path[i] = child_id;
            node.load(manager.open_block(child_id));
            assert(child_id == node.info->id);
            assert(node.info->type == bp_node_type::INTERNAL);

            uint16_t slot = node.child_slot(key);
            if (slot != node.info->size) {
                leaf_max = node.keys[slot];
            }
            child_id = node.children[slot];
        }
        path[0] = child_id;
        node.load(manager.open_block(child_id));
        assert(child_id == node.info->id);
        assert(node.info->type == bp_node_type::LEAF);

        return leaf_max;
    }

#ifdef REDISTRIBUTE
    void update_internal(const path_t &path, const key_type &old_key,
                         const key_type &new_key) {
        node_t node;
        for (uint8_t i = 1; i < ctr_depth; i++) {
            node_id_t node_id = path[i];
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
        for (uint8_t i = 1; i < ctr_depth; i++) {
            node_id_t node_id = path[i];
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
                return;
            }

            // split the node
            node_id_t new_node_id = manager.allocate();
            node_t new_node;
            new_node.init(manager.open_block(new_node_id), INTERNAL);
            manager.mark_dirty(new_node_id);
            ctr_internal++;

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
            if (fp_path[i] == node_id && fp_id != head_id && key <= fp_min) {
                fp_path[i] = new_node_id;
            }
#endif
            child_id = new_node_id;
        }
        create_new_root(key, child_id);
    }

#ifdef REDISTRIBUTE
    void redistribute(const node_t &leaf, uint16_t index, const key_type &key,
                      const value_type &value) {
        assert(lol_prev_id != INVALID_NODE_ID);
        ctr_redistribute++;
        // move values from leaf to leaf prev
        uint16_t items =
            IQR_SIZE_THRESH - lol_prev_size;  // items to be moved to lol prev
        manager.mark_dirty(lol_prev_id);
        node_t lol_prev;
        lol_prev.load(manager.open_block(lol_prev_id));
        assert(lol_prev_id == lol_prev.info->id);
        assert(lol_prev.info->type == bp_node_type::LEAF);
        if (index < items) {
            --items;
            std::memcpy(lol_prev.keys + lol_prev_size, leaf.keys,
                        index * sizeof(key_type));
            std::memcpy(lol_prev.keys + lol_prev_size + index + 1,
                        leaf.keys + index, (items - index) * sizeof(key_type));
            lol_prev.keys[lol_prev_size + index] = key;
            std::memcpy(lol_prev.values + lol_prev_size, leaf.values,
                        index * sizeof(value_type));
            std::memcpy(lol_prev.values + lol_prev_size + index + 1,
                        leaf.values + index,
                        (items - index) * sizeof(value_type));
            lol_prev.values[lol_prev_size + index] = value;

            std::memmove(leaf.keys, leaf.keys + items,
                         (lol_size - items) * sizeof(key_type));
            std::memmove(leaf.values, leaf.values + items,
                         (lol_size - items) * sizeof(value_type));
            ++items;
        } else {
            std::memcpy(lol_prev.keys + lol_prev_size, leaf.keys,
                        items * sizeof(key_type));
            std::memcpy(lol_prev.values + lol_prev_size, leaf.values,
                        items * sizeof(key_type));

            // move leaf.keys and leaf.values items positions to the left
            uint16_t new_index = index - items;
            std::memmove(leaf.keys, leaf.keys + items,
                         new_index * sizeof(key_type));
            std::memmove(leaf.keys + new_index + 1, leaf.keys + index,
                         (lol_size - index) * sizeof(key_type));
            leaf.keys[new_index] = key;
            std::memmove(leaf.values, leaf.values + items,
                         new_index * sizeof(value_type));
            std::memmove(leaf.values + new_index + 1, leaf.values + index,
                         (lol_size - index) * sizeof(value_type));
            leaf.values[new_index] = value;
        }

        // update parent for current leaf min
        update_internal(fp_path, fp_min, leaf.keys[0]);
        // update fp_min, lol_size
        fp_min = leaf.keys[0];
        lol_size = lol_size - items + 1;
        lol_prev_size = IQR_SIZE_THRESH;
        leaf.info->size = lol_size;
        lol_prev.info->size = IQR_SIZE_THRESH;
    }
#endif

    bool leaf_insert(node_t &leaf, const path_t &path, const key_type &key,
                     const value_type &value) {
        manager.mark_dirty(leaf.info->id);
        uint16_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key) {
            // update value
            leaf.values[index] = value;
            return false;
        }

        ctr_size++;
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
                lol_size++;
            } else if (leaf.info->next_id == fp_id) {
                lol_prev_id = leaf.info->id;
                lol_prev_min = leaf.keys[0];
                lol_prev_size = leaf.info->size;
            }
#endif
            return true;
        }

        // how many elements should be on the left side after split
        uint16_t split_leaf_pos = SPLIT_LEAF_POS;
#ifdef LOL_FAT
#ifdef VARIABLE_SPLIT
        bool lol_move = false;
        if (leaf.info->id == fp_id) {
            // when splitting leaf, normally we would do it in the middle
            // but for lol we want to split it where IQR suggests
            if (lol_prev_id == INVALID_NODE_ID) {
                lol_move = true;  // move from head
            } else if (lol_prev_size >= IQR_SIZE_THRESH) {
                // If IQR has enough information
                size_t max_distance = IKR::upper_bound(
                    dist(fp_min, lol_prev_min), lol_prev_size, lol_size);
                uint16_t outlier_pos = leaf.value_slot2(fp_min + max_distance);
                if (outlier_pos <= SPLIT_LEAF_POS) {
                    split_leaf_pos =
                        outlier_pos;  // keep these good values on current lol
                                      // and do not move
                } else {
                    // most of the values are certainly good
                    if (outlier_pos - 10 < SPLIT_LEAF_POS)
                        split_leaf_pos = SPLIT_LEAF_POS;
                    else
                        split_leaf_pos = outlier_pos - 10;
                    lol_move = true;  // also move lol
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
                lol_move = true;
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
        ctr_leaves++;

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
                fp_path[0] = fp_id;
            }
#endif
        }
        if (leaf.info->id == tail_id) {
            tail_id = new_leaf_id;
#ifdef TAIL_FAT
            fp_min = new_leaf.keys[0];
            fp_path[0] = new_leaf_id;
            fp_id = new_leaf_id;
#endif
        }
#ifdef LOL_FAT
        if (leaf.info->id == fp_id) {
            ctr_split++;
#ifndef VARIABLE_SPLIT
            bool lol_move =
                fp_id == head_id ||  // move lol from head
                (lol_prev_size >= IQR_SIZE_THRESH &&
                 dist(new_leaf.keys[0], fp_min) <
                     IKR::upper_bound(dist(fp_min, lol_prev_min), lol_prev_size,
                                      leaf.info->size));
#endif
            if (lol_move) {
                ctr_iqr++;
                // lol believes that the new leaf is not an outlier
                lol_prev_min = fp_min;
                lol_prev_size = leaf.info->size;
                lol_prev_id = fp_id;
                fp_id = new_leaf_id;
                fp_min = new_leaf.keys[0];
                lol_size = new_leaf.info->size;
                fp_path[0] = fp_id;
            } else {
                fp_max = new_leaf.keys[0];
                lol_size = leaf.info->size;
            }
        } else if (new_leaf.info->next_id == fp_id) {
            lol_prev_id = new_leaf_id;
            lol_prev_min = new_leaf.keys[0];
            lol_prev_size = new_leaf.info->size;
        }
#endif

        // insert new key to parent
        internal_insert(path, new_leaf.keys[0], new_leaf_id,
                        SPLIT_INTERNAL_POS);
        return true;
    }

#ifdef LOL_FAT
    static std::size_t cmp(const key_type &max, const key_type &min) { return max - min; }
#endif

public:
    explicit bp_tree(BlockManager &m) : manager(m), root_id(m.allocate())
#ifdef LOL_RESET
          ,
          life(sqrt(node_t::leaf_capacity)),
          ctr_hard(0)
#endif
    {
        head_id = tail_id = root_id;
#ifdef FAST_PATH
        fp_id = root_id;
        fp_path[0] = fp_id;
        fp_min = {};
        fp_max = {};
        ctr_fp = 0;
#ifdef LOL_FAT
        dist = cmp;
        lol_prev_id = INVALID_NODE_ID;
        lol_prev_min = {};
        lol_prev_size = 0;
        lol_size = 0;
        ctr_split = 0;
        ctr_iqr = 0;
        ctr_soft = 0;
#endif
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
        key_type leaf_max = find_leaf(leaf, path, key);
        return leaf_insert(leaf, path, key, value);
    }

    bool insert(const key_type &key, const value_type &value) {
        node_t leaf;
#ifdef FAST_PATH
#ifdef PLOT_FAST
        std::cout << key << ',' << ctr_fp << std::endl;
#endif
        if ((fp_id == head_id || fp_min <= key) &&
            (fp_id == tail_id || key < fp_max)) {
            ctr_fp++;
            leaf.load(manager.open_block(fp_id));
            assert(fp_id == leaf.info->id);
            assert(leaf.info->type == bp_node_type::LEAF);
#ifdef LOL_RESET
            life.success();
#endif
            return leaf_insert(leaf, fp_path, key, value);
        }
#endif
#ifdef LIL_FAT
        path_t &path = fp_path;  // update fp_path
#else
        path_t path;
#endif
        key_type leaf_max = find_leaf(leaf, path, key);
#ifdef LIL_FAT
        // update rest of lil
        fp_id = leaf.info->id;
        if (fp_id != head_id) fp_min = leaf.keys[0];
        if (fp_id != tail_id) fp_max = leaf_max;
#endif
#ifdef LOL_FAT
        // if the new inserted key goes to lol->next, check if lol->next is not
        // an outlier it might be the case that lol reached the previous
        // outliers.
        if (lol_prev_id != INVALID_NODE_ID &&  // lol->prev info exist
                                               //            fp_id != head_id &&
                                               //            // fp_min is valid
            fp_id != tail_id &&                // fp_max is valid
            //            leaf.info->id != tail_id && // don't go to tail
            fp_max == leaf.keys[0] &&  // leaf is lol->next
            // TODO: IQR doesn't have enough values but this kinda works
            // lol_prev_size >= IQR_SIZE_THRESH && lol_size >= IQR_SIZE_THRESH &&
            dist(fp_max, fp_min) < IKR::upper_bound(dist(fp_min, lol_prev_min), lol_prev_size, lol_size)) {
            // move lol to lol->next = leaf
            lol_prev_min = fp_min;
            lol_prev_size = lol_size;
            lol_prev_id = fp_id;
            fp_id = leaf.info->id;
            fp_min = fp_max;
            fp_max = leaf_max;
            lol_size = leaf.info->size;
            fp_path = path;
            ctr_soft++;
#ifdef LOL_RESET
            life.reset();
        } else if (life.failure()) {
            ++ctr_hard;
            lol_prev_id = INVALID_NODE_ID;
            fp_id = leaf.info->id;
            fp_min = leaf.keys[0];
            fp_max = leaf_max;
            lol_size = leaf.info->size;
            fp_path = path;
            life.reset();
#endif
        }
#endif
        return leaf_insert(leaf, path, key, value);
    }

    size_t top_k(size_t count, const key_type &min_key) const {
        node_t leaf;
        path_t path;
        find_leaf(leaf, path, min_key);
        uint16_t index = leaf.value_slot(min_key);
        size_t loads = 1;
        uint16_t curr_size = leaf.info->size - index;
        while (count > curr_size) {
            count -= curr_size;
            if (leaf.info->id == tail_id) {
                break;
            }
            node_id_t next_id = leaf.info->next_id;
            leaf.load(manager.open_block(next_id));
            assert(next_id == leaf.info->id);
            assert(leaf.info->type == bp_node_type::LEAF);
            curr_size = leaf.info->size;
            ++loads;
        }
        return loads;
    }

    size_t range(const key_type &min_key, const key_type &max_key) const {
        size_t loads = 1;
        node_t leaf;
        path_t path;
        find_leaf(leaf, path, min_key);
        while (leaf.keys[leaf.info->size - 1] < max_key) {
            if (leaf.info->id == tail_id) {
                break;
            }
            node_id_t next_id = leaf.info->next_id;
            leaf.load(manager.open_block(next_id));
            assert(next_id == leaf.info->id);
            assert(leaf.info->type == bp_node_type::LEAF);
            ++loads;
        }
        return loads;
    }

    std::optional<value_type> get(const key_type &key) const {
        node_t leaf;
        path_t path;
        find_leaf(leaf, path, key);
        uint16_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key) {
            return leaf.values[index];
        }
        return std::nullopt;
    }

    bool contains(const key_type &key) const { return get(key).has_value(); }
};

#endif
