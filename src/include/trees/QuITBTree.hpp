#pragma once

#include <outlier_detector.h>

#include <cstring>
#include <limits>
#include <optional>
#include <ranges>
#include <vector>

#include "../BTreeNode.hpp"
#include "../MemoryBlockManager.hpp"

namespace QuITBTree {

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

template <typename key_type, typename value_type>
class BTree {
   public:
    using node_id_t = uint32_t;
    using BlockManager = InMemoryBlockManager<node_id_t>;
    using node_t =
        BTreeNode<node_id_t, key_type, value_type, BlockManager::block_size>;
    using step = node_id_t;
    using path_t = std::array<node_id_t, 10>;

    static constexpr const char *name = "QuITBTree";
    static constexpr const bool concurrent = false;
    static constexpr uint16_t SPLIT_INTERNAL_POS =
        node_t::internal_capacity / 2;
    static constexpr uint16_t SPLIT_LEAF_POS = (node_t::leaf_capacity + 1) / 2;
    static constexpr uint16_t IQR_SIZE_THRESH = SPLIT_LEAF_POS;
    static constexpr node_id_t INVALID_NODE_ID =
        std::numeric_limits<node_id_t>::max();
    using dist_f = std::size_t (*)(const key_type &, const key_type &);

    explicit BTree(BlockManager &m)
        : manager(m),
          root_id(m.allocate()),
          life(sqrt(node_t::leaf_capacity)),
          ctr_hard(0) {
        head_id = tail_id = root_id;

        fp_id = root_id;
        fp_path[0] = fp_id;
        fp_min = {};
        fp_max = {};
        ctr_fp = 0;

        dist = cmp;
        lol_prev_id = INVALID_NODE_ID;
        lol_prev_min = {};
        lol_prev_size = 0;
        lol_size = 0;
        ctr_split = 0;
        ctr_iqr = 0;
        ctr_soft = 0;

        node_t root(manager.open_block(root_id), LEAF);
        manager.mark_dirty(root_id);
        root.info->id = root_id;
        root.info->next_id = root_id;
        root.info->size = 0;

        ctr_size = 0;
        ctr_depth = 1;
        ctr_internal = 0;
        ctr_leaves = 1;

        ctr_redistribute = 0;
    }

    bool update(const key_type &key, const value_type &value) {
        node_t leaf;
        path_t path;
        find_leaf(leaf, path, key);
        uint16_t index = leaf.value_slot(key);
        if (index >= leaf.info->size || leaf.keys[index] != key) {
            return false;
        }
        manager.mark_dirty(leaf.info->id);
        leaf.values[index] = value;
        return true;
    }

    bool insert(const key_type &key, const value_type &value) {
        node_t leaf;

        if ((fp_id == head_id || fp_min <= key) &&
            (fp_id == tail_id || key < fp_max)) {
            ctr_fp++;
            leaf.load(manager.open_block(fp_id));

            life.success();

            return leaf_insert(leaf, fp_path, key, value);
        }

        path_t path;

        key_type leaf_max = find_leaf(leaf, path, key);
        if (lol_prev_id != INVALID_NODE_ID &&

            fp_id != tail_id &&

            fp_max == leaf.keys[0] &&

            dist(fp_max, fp_min) < IKR::upper_bound(dist(fp_min, lol_prev_min),
                                                    lol_prev_size, lol_size)) {
            lol_prev_min = fp_min;
            lol_prev_size = lol_size;
            lol_prev_id = fp_id;
            fp_id = leaf.info->id;
            fp_min = fp_max;
            fp_max = leaf_max;
            lol_size = leaf.info->size;
            fp_path = path;
            ctr_soft++;

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
        }

        return leaf_insert(leaf, path, key, value);
    }

    size_t select_k(size_t count, const key_type &min_key) const {
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

    friend std::ostream &operator<<(std::ostream &os, const BTree &tree) {
        os << tree.ctr_size << ", " << +tree.ctr_depth << ", "
           << tree.ctr_internal << ", " << tree.ctr_leaves << ", "
           << tree.ctr_redistribute << ", " << tree.ctr_split << ", "
           << tree.ctr_iqr << ", " << tree.ctr_soft << ", " << tree.ctr_hard
           << ", " << tree.ctr_fp;
        return os;
    }

    bool top_insert(const key_type &key, const value_type &value) {
        node_t leaf;
        path_t path;
        key_type leaf_max = find_leaf(leaf, path, key);
        return leaf_insert(leaf, path, key, value);
    }

   private:
    void create_new_root(const key_type &key, node_id_t node_id) {
        node_id_t left_node_id = manager.allocate();
        node_t root;
        root.load(manager.open_block(root_id));
        node_t left_node;
        left_node.load(manager.open_block(left_node_id));
        std::memcpy(left_node.info, root.info, 4096);
        left_node.info->id = left_node_id;
        manager.mark_dirty(left_node_id);

        if (root.info->type == LEAF) {
            root.info->type = INTERNAL;
            root.children = reinterpret_cast<node_id_t *>(
                root.keys + root.internal_capacity);
        }
        manager.mark_dirty(root_id);
        root.info->size = 1;
        root.keys[0] = key;
        root.children[0] = left_node_id;
        root.children[1] = node_id;
        if (root_id == head_id) {
            head_id = left_node_id;
        }

        if (fp_path[ctr_depth - 1] == root_id) {
            if (fp_id == root_id) {
                fp_id = left_node_id;
            }
            fp_path[ctr_depth - 1] = left_node_id;
        }
        fp_path[ctr_depth] = root_id;

        ctr_depth++;

        ctr_internal++;
    }

    key_type find_leaf(node_t &node, path_t &path, const key_type &key) const {
        key_type leaf_max = {};
        node_id_t child_id = root_id;
        for (uint8_t i = ctr_depth - 1; i > 0; --i) {
            path[i] = child_id;
            node.load(manager.open_block(child_id));

            uint16_t slot = node.child_slot(key);
            if (slot != node.info->size) {
                leaf_max = node.keys[slot];
            }
            child_id = node.children[slot];
        }
        path[0] = child_id;
        node.load(manager.open_block(child_id));

        return leaf_max;
    }

    void update_internal(const path_t &path, const key_type &old_key,
                         const key_type &new_key) {
        node_t node;
        for (uint8_t i = 1; i < ctr_depth; i++) {
            node_id_t node_id = path[i];
            node.load(manager.open_block(node_id));

            uint16_t index = node.child_slot(old_key) - 1;
            if (index < node.info->size && node.keys[index] == old_key) {
                manager.mark_dirty(node_id);
                node.keys[index] = new_key;
                return;
            }
        }
    }

    void internal_insert(const path_t &path, key_type key, node_id_t child_id,
                         uint16_t split_pos) {
        node_t node;
        for (uint8_t i = 1; i < ctr_depth; i++) {
            node_id_t node_id = path[i];
            node.load(manager.open_block(node_id));

            uint16_t index = node.child_slot(key);
            manager.mark_dirty(node_id);
            if (node.info->size < node_t::internal_capacity) {
                std::memmove(node.keys + index + 1, node.keys + index,
                             (node.info->size - index) * sizeof(key_type));
                std::memmove(node.children + index + 2,
                             node.children + index + 1,
                             (node.info->size - index) * sizeof(uint32_t));
                node.keys[index] = key;
                node.children[index + 1] = child_id;
                ++node.info->size;
                return;
            }

            node_id_t new_node_id = manager.allocate();
            node_t new_node(manager.open_block(new_node_id), INTERNAL);
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

            if (fp_path[i] == node_id && fp_id != head_id && key <= fp_min) {
                fp_path[i] = new_node_id;
            }

            child_id = new_node_id;
        }
        create_new_root(key, child_id);
    }

    void redistribute(const node_t &leaf, uint16_t index, const key_type &key,
                      const value_type &value) {
        ctr_redistribute++;

        uint16_t items = IQR_SIZE_THRESH - lol_prev_size;
        manager.mark_dirty(lol_prev_id);
        node_t lol_prev;
        lol_prev.load(manager.open_block(lol_prev_id));
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

        update_internal(fp_path, fp_min, leaf.keys[0]);

        fp_min = leaf.keys[0];
        lol_size = lol_size - items + 1;
        lol_prev_size = IQR_SIZE_THRESH;
        leaf.info->size = lol_size;
        lol_prev.info->size = IQR_SIZE_THRESH;
    }

    bool leaf_insert(node_t &leaf, const path_t &path, const key_type &key,
                     const value_type &value) {
        manager.mark_dirty(leaf.info->id);
        uint16_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key) {
            leaf.values[index] = value;
            return false;
        }

        ctr_size++;
        if (leaf.info->size < node_t::leaf_capacity) {
            std::memmove(leaf.keys + index + 1, leaf.keys + index,
                         (leaf.info->size - index) * sizeof(key_type));
            std::memmove(leaf.values + index + 1, leaf.values + index,
                         (leaf.info->size - index) * sizeof(value_type));
            leaf.keys[index] = key;
            leaf.values[index] = value;
            ++leaf.info->size;

            if (leaf.info->id == fp_id) {
                lol_size++;
            } else if (leaf.info->next_id == fp_id) {
                lol_prev_id = leaf.info->id;
                lol_prev_min = leaf.keys[0];
                lol_prev_size = leaf.info->size;
            }

            return true;
        }

        uint16_t split_leaf_pos = SPLIT_LEAF_POS;

        bool lol_move = false;
        if (leaf.info->id == fp_id) {
            if (lol_prev_id == INVALID_NODE_ID) {
                lol_move = true;
            } else if (lol_prev_size >= IQR_SIZE_THRESH) {
                size_t max_distance = IKR::upper_bound(
                    dist(fp_min, lol_prev_min), lol_prev_size, lol_size);
                uint16_t outlier_pos = leaf.value_slot2(fp_min + max_distance);
                if (outlier_pos <= SPLIT_LEAF_POS) {
                    split_leaf_pos = outlier_pos;

                } else {
                    if (outlier_pos - 10 < SPLIT_LEAF_POS)
                        split_leaf_pos = SPLIT_LEAF_POS;
                    else
                        split_leaf_pos = outlier_pos - 10;
                    lol_move = true;
                }
                if (index < outlier_pos) {
                    split_leaf_pos++;
                }
            } else {
                redistribute(leaf, index, key, value);
                return true;
            }
        }

        node_id_t new_leaf_id = manager.allocate();
        node_t new_leaf(manager.open_block(new_leaf_id), LEAF);
        manager.mark_dirty(new_leaf_id);
        ctr_leaves++;
        leaf.info->size = split_leaf_pos;
        new_leaf.info->id = new_leaf_id;
        new_leaf.info->next_id = leaf.info->next_id;
        leaf.info->next_id = new_leaf_id;
        new_leaf.info->size = node_t::leaf_capacity + 1 - leaf.info->size;

        if (index < leaf.info->size) {
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
        }
        if (leaf.info->id == tail_id) {
            tail_id = new_leaf_id;
        }

        if (leaf.info->id == fp_id) {
            ctr_split++;
            if (lol_move) {
                ctr_iqr++;

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

        internal_insert(path, new_leaf.keys[0], new_leaf_id,
                        SPLIT_INTERNAL_POS);
        return true;
    }

    static std::size_t cmp(const key_type &max, const key_type &min) {
        return max - min;
    }

    BlockManager &manager;
    const node_id_t root_id;
    node_id_t head_id;
    node_id_t tail_id;

    node_id_t fp_id;
    key_type fp_min;
    key_type fp_max;
    path_t fp_path;

    dist_f dist;
    node_id_t lol_prev_id;

    reset_stats life;

    key_type lol_prev_min;
    uint16_t lol_prev_size;
    uint16_t lol_size;

    uint32_t ctr_size;
    uint8_t ctr_depth;
    uint32_t ctr_internal;
    uint32_t ctr_leaves;

    uint32_t ctr_fp;

    uint32_t ctr_split;
    uint32_t ctr_iqr;
    uint32_t ctr_soft;

    uint32_t ctr_hard;

    uint32_t ctr_redistribute;
};
}  // namespace QuITBTree
