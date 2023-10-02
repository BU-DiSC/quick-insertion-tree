#ifndef BP_TREE_H
#define BP_TREE_H

#include <optional>

#ifdef INMEMORY

#include "memory_block_manager.h"

using BlockManager = InMemoryBlockManager;

#else

#include "disk_block_manager.h"

using BlockManager = DiskBlockManager;

#endif

#include "bp_node.h"

#ifdef LOL_FAT

#include "outlier_detector.h"

#endif

#define MAX_DEPTH 10

template<typename key_type, typename value_type>
class bp_tree {
    friend std::ostream &operator<<(std::ostream &os, const bp_tree<key_type, value_type> &tree) {
        os << tree.ctr_size << ", " << +tree.ctr_depth << ", " << tree.manager << ", "
           << tree.ctr_internal << ", " << tree.ctr_leaves << ", "
           #ifdef REDISTRIBUTE
           << tree.ctr_redistribute
           #endif
           << ", "
           #ifdef LOL_FAT
           << tree.ctr_lol_split
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
           #ifdef LOL_FAT
           << tree.ctr_1
           #endif
           << ", "
           #ifdef LOL_FAT
           << tree.ctr_2
           #endif
           << ", "
           #ifdef LOL_FAT
           << tree.ctr_3
           #endif
           << ", "
           #ifdef TAIL_FAT
           << tree.ctr_tail
           #endif
           << ", "
           #ifdef LIL_FAT
           << tree.ctr_lil
           #endif
           << ", "
           #ifdef LOL_FAT
           << tree.ctr_lol
           #endif
                ;
        return os;
    }

    using node_t = bp_node<key_type, value_type>;
    using dist_f = std::size_t (*)(const key_type &, const key_type &);
    using path_t = std::array<key_type, MAX_DEPTH>;  // starts from leaf -> root and empty slots at the end for the tree to grow

    static constexpr uint16_t SPLIT_INTERNAL_POS = node_t::internal_capacity / 2;
    static constexpr uint16_t SPLIT_LEAF_POS = (node_t::leaf_capacity + 1) / 2;
    static constexpr uint16_t IQR_SIZE_THRESH = 0;

    dist_f dist;

    BlockManager manager;
    uint32_t root_id;
    uint32_t head_id;  // not really needed
    uint32_t tail_id;  // not really needed
#ifdef TAIL_FAT
    key_type tail_min;
    path_t tail_path;
    uint32_t ctr_tail;
#endif
#ifdef LIL_FAT
    uint32_t lil_id;
    key_type lil_min;
    key_type lil_max;
    path_t lil_path;
    uint32_t ctr_lil;
#endif
#ifdef LOL_FAT
    uint32_t lol_id;
    key_type lol_min;
    key_type lol_max;
    path_t lol_path;
    uint32_t ctr_lol;
    // iqr
    key_type lol_prev_min;
    uint16_t lol_prev_size;
    uint16_t lol_size;
    uint32_t ctr_lol_split;
    uint32_t ctr_iqr;
    uint32_t ctr_soft;
#endif
    uint32_t ctr_size;
    uint8_t ctr_depth;  // path[ctr_depth - 1] is the root
    uint32_t ctr_internal;
    uint32_t ctr_leaves;
    uint32_t ctr_1;
    uint32_t ctr_2;
    uint32_t ctr_3;
#ifdef REDISTRIBUTE
    uint32_t ctr_redistribute;
#endif

    void update_paths(uint8_t depth, const key_type &key, uint32_t node_id, uint32_t new_node_id) {
#ifdef LOL_FAT
        if (lol_path[depth] == node_id && lol_id != head_id && key <= lol_min) {
            lol_path[depth] = new_node_id;
        }
#endif
#ifdef LIL_FAT
        if (lil_path[depth] == node_id && lil_id != head_id && key <= lil_min) {
            lil_path[depth] = new_node_id;
        }
#endif
#ifdef TAIL_FAT
        if (tail_path[depth] == node_id && tail_id != head_id && key <= tail_min) {
            tail_path[depth] = new_node_id;
        }
#endif
    }

    void create_new_root(const key_type &key, uint32_t node_id) {
        uint32_t old_root_id = root_id;
        root_id = manager.allocate();
        node_t root(manager.open_block(root_id), bp_node_info::INTERNAL);
        // It is expected that the root is already marked dirty, so we don't need to mark it again
        // manager.mark_dirty(root_id);
        root.info->id = root_id;
        root.info->size = 1;
        root.keys[0] = key;
        root.children[0] = old_root_id;
        root.children[1] = node_id;
#ifdef LOL_FAT
        lol_path[ctr_depth] = root_id;
#endif
#ifdef LIL_FAT
        lil_path[ctr_depth] = root_id;
#endif
#ifdef TAIL_FAT
        tail_path[ctr_depth] = root_id;
#endif
        ctr_depth++;
        assert(ctr_depth < MAX_DEPTH);
        ctr_internal++;
    }

    key_type find_leaf(node_t &node, path_t &path, const key_type &key) {
        key_type leaf_max = {};
        uint32_t child_id = root_id;
        for (int i = ctr_depth - 1; i > 0; --i) {  // from root to last internal level
            path[i] = child_id;
            node.load(manager.open_block(child_id));
            assert(child_id == node.info->id);

            uint16_t slot = node.child_slot(key);
            if (slot != node.info->size) {
                leaf_max = node.keys[slot];
            }
            child_id = node.children[slot];
        }
        path[0] = child_id;
        node.load(manager.open_block(child_id));
        assert(child_id == node.info->id);
        assert(node.info->type == bp_node_info::LEAF);

        return leaf_max;
    }

    void internal_insert(path_t &path, key_type key, uint32_t child_id, uint16_t split_pos) {
        for (uint8_t i = 1; i < ctr_depth; i++) {
            uint32_t node_id = path[i];
            node_t node(manager.open_block(node_id));
            assert(node.info->id == node_id);
            assert(node.info->type == bp_node_info::INTERNAL);
            uint16_t index = node.child_slot(key);
            assert(index == node_t::internal_capacity || node.keys[index] != key);
            manager.mark_dirty(node_id);
            if (node.info->size < node_t::internal_capacity) {
                // insert new key
                std::memmove(node.keys + index + 1, node.keys + index, (node.info->size - index) * sizeof(key_type));
                std::memmove(node.children + index + 2, node.children + index + 1,
                             (node.info->size - index) * sizeof(uint32_t));
                node.keys[index] = key;
                node.children[index + 1] = child_id;
                node.info->size++;
                return;
            }

            // split the node
            uint32_t new_node_id = manager.allocate();
            node_t new_node(manager.open_block(new_node_id), bp_node_info::INTERNAL);
            manager.mark_dirty(new_node_id);
            ctr_internal++;

            node.info->size = split_pos;
            new_node.info->id = new_node_id;
            new_node.info->size = node_t::internal_capacity - node.info->size;

            if (index < node.info->size) {
                std::memcpy(new_node.keys, node.keys + node.info->size, new_node.info->size * sizeof(key_type));
                std::memmove(node.keys + index + 1, node.keys + index, (node.info->size - index) * sizeof(key_type));
                node.keys[index] = key;
                std::memcpy(new_node.children, node.children + node.info->size,
                            (new_node.info->size + 1) * sizeof(uint32_t));
                std::memmove(node.children + index + 2, node.children + index + 1,
                             (node.info->size - index + 1) * sizeof(uint32_t));
                node.children[index + 1] = child_id;

                key = node.keys[node.info->size];
                // key = new_node.keys[0];
            } else if (index == node.info->size) {
                std::memcpy(new_node.keys, node.keys + node.info->size, new_node.info->size * sizeof(key_type));
                std::memcpy(new_node.children + 1, node.children + 1 + node.info->size,
                            new_node.info->size * sizeof(uint32_t));
                new_node.children[0] = child_id;
            } else {
                std::memcpy(new_node.keys, node.keys + node.info->size + 1,
                            (index - node.info->size - 1) * sizeof(key_type));
                std::memcpy(new_node.keys + index - node.info->size, node.keys + index,
                            (node_t::internal_capacity - index) * sizeof(key_type));
                new_node.keys[index - node.info->size - 1] = key;
                std::memcpy(new_node.children, node.children + 1 + node.info->size,
                            (index - node.info->size) * sizeof(uint32_t));
                std::memcpy(new_node.children + 1 + index - node.info->size, node.children + 1 + index,
                            new_node.info->size * sizeof(uint32_t));
                new_node.children[index - node.info->size] = child_id;

                key = node.keys[node.info->size];
            }

            update_paths(i, key, node_id, new_node_id);

            child_id = new_node_id;
        }
        create_new_root(key, child_id);
    }

    bool leaf_insert(node_t &leaf, path_t &path, const key_type &key, const value_type &value) {
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
            std::memmove(leaf.keys + index + 1, leaf.keys + index, (leaf.info->size - index) * sizeof(key_type));
            std::memmove(leaf.values + index + 1, leaf.values + index, (leaf.info->size - index) * sizeof(value_type));
            leaf.keys[index] = key;
            leaf.values[index] = value;
            leaf.info->size++;
#ifdef LOL_FAT
            if (leaf.info->id == lol_id) {
                lol_size++;
            } else if (leaf.info->id != tail_id && leaf.info->next_id == lol_id) {
                lol_prev_size++;
            }
#endif
            return true;
        }

#ifndef LOL_FAT
#undef VARIABLE_SPLIT
#endif
        // how many elements should be on the left side after split
        uint16_t split_leaf_pos = SPLIT_LEAF_POS;
#ifdef VARIABLE_SPLIT
        bool lol_move = false;
        if (leaf.info->id == lol_id) {
            // when splitting leaf, normally we would do it in the middle
            // but for lol we want to split it where IQR suggests
            if (lol_id == head_id) {
                lol_move = true; // move from head
            } else if (lol_prev_size >= IQR_SIZE_THRESH) {
                // If IQR has enough information
                size_t d = dist(lol_min, lol_prev_min);
                size_t lower = IQRDetector::lower_bound(d, lol_prev_size, lol_size);
                uint16_t lower_pos = leaf.value_slot(lol_min + lower); // 0 < split_leaf_pos <= node_t::leaf_capacity
//                if (key < leaf.keys[split_leaf_pos]) {
//                    ++split_leaf_pos;
//                }
                if (lower_pos > SPLIT_LEAF_POS) {  // most of the values are certainly good
                    split_leaf_pos = lower_pos - 1;  // take one to the new leaf
                    lol_move = true; // also move lol
                    ctr_1++;
                } else {
                    size_t upper = IQRDetector::upper_bound(d, lol_prev_size, lol_size);
                    uint16_t upper_pos = leaf.value_slot(lol_min + upper); // 0 < split_leaf_pos <= node_t::leaf_capacity
                    if (upper_pos < SPLIT_LEAF_POS) {  // most of the values are certainly bad
                        split_leaf_pos = index <= upper_pos ? upper_pos + 1 : upper_pos;
                        ctr_2++;
                    } else {
                        split_leaf_pos = SPLIT_LEAF_POS;
                        lol_move = upper_pos - SPLIT_LEAF_POS > SPLIT_LEAF_POS - lower_pos;
                        ctr_3++;
                    }
                }
//                split_leaf_pos = SPLIT_LEAF_POS;
#ifdef REDISTRIBUTE
            } else {
                ctr_redistribute++;
                lol_move = true;
//                lol_move = lol_id == head_id || // move lol from head
//                            dist(leaf.keys[SPLIT_LEAF_POS], lol_min) <
//                            IQRDetector::upper_bound(dist(lol_min, lol_prev_min), lol_prev_size, SPLIT_LEAF_POS);
//              move values from leaf to leaf prev
//              update parent for current leaf min
//              update lol_min, lol_size
//              lol_prev_size = SPLIT_LEAF_POS;
//              ctr_lol_split++;
//              return true;
#else
#endif
            }
        }
#endif
        // split the leaf
        uint32_t new_leaf_id = manager.allocate();
        node_t new_leaf(manager.open_block(new_leaf_id), bp_node_info::LEAF);
        manager.mark_dirty(new_leaf_id);
        ctr_leaves++;

        assert(1 < split_leaf_pos && split_leaf_pos <= node_t::leaf_capacity);
        leaf.info->size = split_leaf_pos;
        new_leaf.info->id = new_leaf_id;
        new_leaf.info->next_id = leaf.info->next_id;
        leaf.info->next_id = new_leaf_id;
        new_leaf.info->size = node_t::leaf_capacity + 1 - leaf.info->size;

        if (index < leaf.info->size) {
            // get one more since the new key goes left
            std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size - 1, new_leaf.info->size * sizeof(key_type));
            std::memmove(leaf.keys + index + 1, leaf.keys + index, (leaf.info->size - index - 1) * sizeof(key_type));
            leaf.keys[index] = key;
            std::memcpy(new_leaf.values, leaf.values + leaf.info->size - 1, new_leaf.info->size * sizeof(value_type));
            std::memmove(leaf.values + index + 1, leaf.values + index,
                         (leaf.info->size - index - 1) * sizeof(value_type));
            leaf.values[index] = value;

#ifdef LIL_FAT
            // if we insert to left node of split, we set the leaf max
            if (leaf.info->id == lil_id) {
                lil_max = new_leaf.keys[0];
            }
#endif
        } else {
            uint16_t new_index = index - leaf.info->size;
            std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size, new_index * sizeof(key_type));
            new_leaf.keys[new_index] = key;
            std::memcpy(new_leaf.keys + new_index + 1, leaf.keys + index,
                        (node_t::leaf_capacity - index) * sizeof(key_type));
            std::memcpy(new_leaf.values, leaf.values + leaf.info->size, new_index * sizeof(value_type));
            new_leaf.values[new_index] = value;
            std::memcpy(new_leaf.values + new_index + 1, leaf.values + index,
                        (node_t::leaf_capacity - index) * sizeof(value_type));
#ifdef LIL_FAT
            if (leaf.info->id == lil_id) {
                lil_id = new_leaf.info->id;
                // if we insert to right split node, we set leaf min
                lil_min = new_leaf.keys[0];
                lil_path[0] = lil_id;
            }
#endif
        }
        if (leaf.info->id == tail_id) {
            tail_id = new_leaf_id;
#ifdef TAIL_FAT
            tail_min = new_leaf.keys[0];
            tail_path[0] = tail_id;
#endif
        }
#ifdef LOL_FAT
        if (leaf.info->id == lol_id) {
            ctr_lol_split++;
#ifndef VARIABLE_SPLIT
            bool lol_move = lol_id == head_id || // move lol from head
                            (lol_prev_size >= IQR_SIZE_THRESH &&
                             dist(new_leaf.keys[0], lol_min) <
                             IQRDetector::upper_bound(dist(lol_min, lol_prev_min), lol_prev_size, leaf.info->size));
#endif
            if (lol_move) {
                ctr_iqr++;
                // lol believes that the new leaf is not an outlier
                lol_prev_min = lol_min;
                lol_prev_size = leaf.info->size;
                lol_id = new_leaf_id;
                lol_min = new_leaf.keys[0];
                lol_size = new_leaf.info->size;
                lol_path[0] = lol_id;
            } else {
                lol_max = new_leaf.keys[0];
                lol_size = leaf.info->size;
            }
        } else if (new_leaf_id != tail_id && new_leaf.info->next_id == lol_id) {
            lol_prev_min = new_leaf.keys[0];
            lol_prev_size = new_leaf.info->size;
        }
#endif

        // insert new key to parent
        internal_insert(path, new_leaf.keys[0], new_leaf_id, SPLIT_INTERNAL_POS);
        return true;
    }

public:
    bp_tree(const char *filepath, uint32_t blocks_in_memory, dist_f cmp) : manager(filepath, blocks_in_memory) {
        dist = cmp;
        root_id = manager.allocate();
        head_id = tail_id = root_id;
#ifdef TAIL_FAT
        tail_path[0] = tail_id;
        tail_min = 0;
        ctr_tail = 0;
#endif
#ifdef LIL_FAT
        lil_id = root_id;
        lil_path[0] = lil_id;
        lil_min = 0;
        lil_max = 0;
        ctr_lil = 0;
#endif
#ifdef LOL_FAT
        lol_id = root_id;
        lol_path[0] = lol_id;
        lol_prev_min = 0;
        lol_prev_size = 0;
        lol_min = 0;
        lol_max = 0;
        lol_size = 0;
        ctr_lol = 0;
        ctr_lol_split = 0;
        ctr_iqr = 0;
        ctr_soft = 0;
#endif
        node_t root(manager.open_block(root_id), bp_node_info::LEAF);
        manager.mark_dirty(root_id);
        root.info->id = root_id;
        root.info->next_id = root_id;
        root.info->size = 0;

        ctr_size = 0;
        ctr_depth = 1;
        ctr_internal = 0;
        ctr_leaves = 1;
        ctr_1 = 0;
        ctr_2 = 0;
        ctr_3 = 0;
#ifdef REDISTRIBUTE
        ctr_redistribute = 0;
#endif
    }

    bool insert(const key_type &key, const value_type &value) {
#ifdef LOL_FAT
#ifdef LIL_FAT
        // sanity check
        assert(lil_id != lol_id ||
               ((lil_id == head_id || lil_min == lol_min) && (lil_id == tail_id || lil_max == lol_max)));
#endif
#endif
        node_t leaf;
#ifdef LOL_FAT
        if ((lol_id == head_id || lol_min <= key) && (lol_id == tail_id || key < lol_max)) {
            ctr_lol++;
            leaf.load(manager.open_block(lol_id));
            return leaf_insert(leaf, lol_path, key, value);
        }
#endif
#ifdef LIL_FAT
        if ((lil_id == head_id || lil_min <= key) && (lil_id == tail_id || key < lil_max)) {
            ctr_lil++;
            leaf.load(manager.open_block(lil_id));
            return leaf_insert(leaf, lil_path, key, value);
        }
#endif
#ifdef TAIL_FAT
        if (tail_id == head_id || tail_min <= key) {
            ctr_tail++;
            leaf.load(manager.open_block(tail_id));
            return leaf_insert(leaf, tail_path, key, value);
        }
#endif
#ifdef LIL_FAT
        path_t &path = lil_path;  // update lil_path
#else
        path_t path;
#endif
        key_type leaf_max = find_leaf(leaf, path, key);
#ifdef LIL_FAT
        // update rest of lil
        lil_id = leaf.info->id;
        if (lil_id != head_id)
            lil_min = leaf.keys[0];
        if (lil_id != tail_id)
            lil_max = leaf_max;
#endif
#ifdef LOL_FAT
        // if the new inserted key goes to lol->next, check if lol->next is not an outlier
        // it might be the case that lol reached the previous outliers.
        if (lol_id != head_id && // lol->prev and lol_min exist
            lol_id != tail_id && // lol_max exists
//            leaf.info->id != tail_id && // don't go to tail
            lol_max == leaf.keys[0] && // leaf is lol->next
//            lol_prev_size >= IQR_SIZE_THRESH && lol_size >= IQR_SIZE_THRESH &&  // TODO: IQR doesn't have enough values but this kinda works
            dist(lol_max, lol_min) < IQRDetector::upper_bound(dist(lol_min, lol_prev_min), lol_prev_size, lol_size)) {
            // move lol to lol->next = leaf
            lol_prev_min = lol_min;
            lol_prev_size = lol_size;
            lol_id = leaf.info->id;
            lol_min = lol_max;
            lol_max = leaf_max;
            lol_size = leaf.info->size;
            lol_path = path;
            ctr_soft++;
        }
#endif
        return leaf_insert(leaf, path, key, value);
    }

    std::optional<value_type> get(const key_type &key) {
        node_t leaf;
        path_t path;
        find_leaf(leaf, path, key);
        uint16_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key) {
            return leaf.values[index];
        }
        return std::nullopt;
    }

    bool contains(const key_type &key) {
        return get(key).has_value();
    }
};

#endif
