#ifndef BP_TREE_H
#define BP_TREE_H

#include <optional>
#include "block_manager.h"
#include "bp_node.h"
#ifdef LOL_FAT
#include "iqr_detector.h"
#endif

template <typename key_type, typename value_type>
class bp_tree
{
    friend std::ostream &operator<<(std::ostream &os, const bp_tree<key_type, value_type> &tree)
    {
        os << tree.size << ", " << tree.depth << ", " <<
           tree.manager.getNumWrites() << ", " << tree.manager.getMarkDirty() << ", " <<
           tree.num_internal << ", " << tree.num_leaves << ", "
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

protected:
    using node_t = bp_node<key_type, value_type>;

    uint32_t root_id;
    uint32_t head_id;
#ifdef LIL_FAT
    uint32_t lil_id;
    std::optional<key_type> lil_min;
    std::optional<key_type> lil_max;
#endif
#ifdef LOL_FAT
    uint32_t lol_id;
    std::optional<key_type> lol_min;
    std::optional<key_type> lol_max;
#endif
    uint32_t size;
    uint32_t depth;
    BlockManager manager;

    uint32_t num_internal;
    uint32_t num_leaves;
#ifdef LIL_FAT
    uint32_t ctr_lil;
#endif
#ifdef LOL_FAT
    uint32_t ctr_lol;
#endif

    void create_new_root(key_type key, node_t &node, node_t &new_node)
    {
        assert(root_id == node.info->id);
        depth++;
        root_id = manager.allocate();
        node_t root(manager.open_block(root_id), bp_node_info::INTERNAL);
        // It is expected that the root is already marked dirty, so we don't need to mark it again
        // manager.mark_dirty(root_id);
        root.info->id = root_id;
        root.info->size = 1;
        root.keys[0] = key;
        root.children[0] = node.info->id;
        root.children[1] = new_node.info->id;
        node.info->parent_id = root_id;
        new_node.info->parent_id = root_id;
        num_internal++;
    }

    std::optional<key_type> find_leaf(node_t &node, const key_type &key)
    {
        std::optional<key_type> current_max = std::nullopt;
        uint32_t child_id = root_id;
        while (true)
        {
            node.load(manager.open_block(child_id));
            assert(child_id == node.info->id);
            if (node.info->type == bp_node_info::LEAF)
                break;

            uint32_t slot = node.child_slot(key);
            if (slot != node.info->size)
            {
                current_max = node.keys[slot];
            }
            child_id = node.children[slot];
        }

        return current_max;
    }

    void internal_insert(uint32_t node_id, key_type key, uint32_t child_id, float split_ratio)
    {
        while (true)
        {
            node_t node(manager.open_block(node_id));
            assert(node.info->id == node_id);
            assert(node.info->type == bp_node_info::INTERNAL);
            uint32_t index = node.child_slot(key);
            assert(index == node_t::internal_capacity || node.keys[index] != key);
            manager.mark_dirty(node_id);
            if (node.info->size < node_t::internal_capacity)
            {
                // insert new key
#ifdef COPY
                std::copy_backward(node.keys + index, node.keys + node.info->size, node.keys + node.info->size + 1);
                std::copy_backward(node.children + index + 1, node.children + node.info->size + 1,
                                   node.children + node.info->size + 2);
#else
                std::memmove(node.keys + index + 1, node.keys + index, (node.info->size - index) * sizeof(key_type));
                std::memmove(node.children + index + 2, node.children + index + 1,
                             (node.info->size - index) * sizeof(uint32_t));
#endif
                node.keys[index] = key;
                node.children[index + 1] = child_id;
                node.info->size++;
                return;
            }

            // split the node
            uint32_t new_node_id = manager.allocate();
            node_t new_node(manager.open_block(new_node_id), bp_node_info::INTERNAL);
            manager.mark_dirty(new_node_id);
            num_internal++;

            node.info->size = node_t::internal_capacity * split_ratio;
            new_node.info->id = new_node_id;
            new_node.info->parent_id = node.info->parent_id; // same parent?
            new_node.info->size = node_t::internal_capacity - node.info->size;

            if (index < node.info->size)
            {
#ifdef COPY
                std::copy(node.keys + node.info->size, node.keys + node_t::internal_capacity, new_node.keys);
                std::copy_backward(node.keys + index, node.keys + node.info->size, node.keys + node.info->size + 1);
#else
                std::memcpy(new_node.keys, node.keys + node.info->size,
                            (node_t::internal_capacity - node.info->size) * sizeof(key_type));
                std::memmove(node.keys + index + 1, node.keys + index, (node.info->size - index) * sizeof(key_type));
#endif
                node.keys[index] = key;

#ifdef COPY
                std::copy(node.children + node.info->size, node.children + 1 + node_t::internal_capacity,
                          new_node.children);
                std::copy_backward(node.children + index + 1, node.children + node.info->size + 1,
                                   node.children + node.info->size + 2);
#else
                std::memcpy(new_node.children, node.children + node.info->size,
                            (1 + node_t::internal_capacity - node.info->size) * sizeof(uint32_t));
                std::memmove(node.children + index + 2, node.children + index + 1,
                             (node.info->size - index + 1) * sizeof(uint32_t));
#endif
                node.children[index + 1] = child_id;

                key = node.keys[node.info->size];
                // key = new_node.keys[0];
            }
            else if (index == node.info->size)
            {
#ifdef COPY
                std::copy(node.keys + node.info->size, node.keys + node_t::internal_capacity, new_node.keys);
                std::copy(node.children + 1 + node.info->size, node.children + 1 + node_t::internal_capacity,
                          new_node.children + 1);
#else
                std::memcpy(new_node.keys, node.keys + node.info->size,
                            (node_t::internal_capacity - node.info->size) * sizeof(key_type));
                std::memcpy(new_node.children + 1, node.children + 1 + node.info->size,
                            (node_t::internal_capacity - node.info->size) * sizeof(uint32_t));
#endif
                new_node.children[0] = child_id;
            }
            else
            {
#ifdef COPY
                std::copy(node.keys + node.info->size + 1, node.keys + index, new_node.keys);
                std::copy(node.keys + index, node.keys + node_t::internal_capacity,
                          new_node.keys + index - node.info->size);
#else
                std::memcpy(new_node.keys, node.keys + node.info->size + 1,
                            (index - node.info->size - 1) * sizeof(key_type));
                std::memcpy(new_node.keys + index - node.info->size, node.keys + index,
                            (node_t::internal_capacity - index) * sizeof(key_type));
#endif
                new_node.keys[index - node.info->size - 1] = key;

#ifdef COPY
                std::copy(node.children + 1 + node.info->size, node.children + 1 + index, new_node.children);
                std::copy(node.children + 1 + index, node.children + 1 + node_t::internal_capacity,
                          new_node.children + 1 + index - node.info->size);
#else
                std::memcpy(new_node.children, node.children + 1 + node.info->size,
                            (index - node.info->size) * sizeof(uint32_t));
                std::memcpy(new_node.children + 1 + index - node.info->size, node.children + 1 + index,
                            (node_t::internal_capacity - index) * sizeof(uint32_t));
#endif
                new_node.children[index - node.info->size] = child_id;

                key = node.keys[node.info->size];
            }

            for (int i = 0; i <= new_node.info->size; ++i)
            {
                node_t child(manager.open_block(new_node.children[i]));
                assert(child.info->id == new_node.children[i]);
                manager.mark_dirty(child.info->id);
                child.info->parent_id = new_node_id;
            }

            if (node.info->id == root_id)
            {
                create_new_root(key, node, new_node);
                return;
            }

            node_id = node.info->parent_id;
            child_id = new_node_id;
        }
    }

    bool leaf_insert(node_t &leaf, const key_type &key, const value_type &value)
    {
        manager.mark_dirty(leaf.info->id);
        uint32_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key)
        {
            // update value
            leaf.values[index] = value;
            return false;
        }
        size++;
        if (leaf.info->size < node_t::leaf_capacity)
        {
            // insert new key
#ifdef COPY
            std::copy_backward(leaf.keys + index, leaf.keys + leaf.info->size, leaf.keys + leaf.info->size + 1);
            std::copy_backward(leaf.values + index, leaf.values + leaf.info->size, leaf.values + leaf.info->size + 1);
#else
            std::memmove(leaf.keys + index + 1, leaf.keys + index, (leaf.info->size - index) * sizeof(key_type));
            std::memmove(leaf.values + index + 1, leaf.values + index, (leaf.info->size - index) * sizeof(value_type));
#endif
            leaf.keys[index] = key;
            leaf.values[index] = value;
            leaf.info->size++;
            return true;
        }
        // split the leaf
        uint32_t new_leaf_id = manager.allocate();
        node_t new_leaf(manager.open_block(new_leaf_id), bp_node_info::LEAF);
        manager.mark_dirty(new_leaf_id);
        num_leaves++;

        float split_ratio = 0.5;
        leaf.info->size = (node_t::leaf_capacity + 1) * split_ratio;
        new_leaf.info->id = new_leaf_id;
        new_leaf.info->parent_id = leaf.info->parent_id; // same parent?
        new_leaf.info->next_id = leaf.info->next_id;
        leaf.info->next_id = new_leaf_id;
        new_leaf.info->size = node_t::leaf_capacity + 1 - leaf.info->size;

        if (index < leaf.info->size)
        {
#ifdef COPY
            std::copy(leaf.keys + leaf.info->size - 1, leaf.keys + node_t::leaf_capacity, new_leaf.keys);
            std::copy_backward(leaf.keys + index, leaf.keys + leaf.info->size, leaf.keys + leaf.info->size + 1);
#else
            std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size - 1,
                        (node_t::leaf_capacity - leaf.info->size + 1) * sizeof(key_type));
            std::memmove(leaf.keys + index + 1, leaf.keys + index, (leaf.info->size - index) * sizeof(key_type));
#endif
            leaf.keys[index] = key;
#ifdef COPY
            std::copy(leaf.values + leaf.info->size - 1, leaf.values + node_t::leaf_capacity, new_leaf.values);
            std::copy_backward(leaf.values + index, leaf.values + leaf.info->size, leaf.values + leaf.info->size + 1);
#else
            std::memcpy(new_leaf.values, leaf.values + leaf.info->size - 1,
                        (node_t::leaf_capacity - leaf.info->size + 1) * sizeof(value_type));
            std::memmove(leaf.values + index + 1, leaf.values + index, (leaf.info->size - index) * sizeof(value_type));
#endif
            leaf.values[index] = value;

#ifdef LIL_FAT
            // if we insert to left node of split, we set the leaf max
#ifdef LOL_FAT
            if (lil_id == leaf.info->id)
#endif
                lil_max = new_leaf.keys[0];
#endif
        }
        else
        {
#ifdef COPY
            std::copy(leaf.keys + leaf.info->size, leaf.keys + index, new_leaf.keys);
            std::copy(leaf.keys + index, leaf.keys + node_t::leaf_capacity,
                      new_leaf.keys + index - leaf.info->size + 1);
#else
            std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size, (index - leaf.info->size) * sizeof(key_type));
            std::memcpy(new_leaf.keys + index - leaf.info->size + 1, leaf.keys + index,
                        (node_t::leaf_capacity - index) * sizeof(key_type));
#endif
            new_leaf.keys[index - leaf.info->size] = key;
#ifdef COPY
            std::copy(leaf.values + leaf.info->size, leaf.values + index, new_leaf.values);
            std::copy(leaf.values + index, leaf.values + node_t::leaf_capacity,
                      new_leaf.values + index - leaf.info->size + 1);
#else
            std::memcpy(new_leaf.values, leaf.values + leaf.info->size, (index - leaf.info->size) * sizeof(value_type));
            std::memcpy(new_leaf.values + index - leaf.info->size + 1, leaf.values + index,
                        (node_t::leaf_capacity - index) * sizeof(value_type));
#endif
            new_leaf.values[index - leaf.info->size] = value;
#ifdef LIL_FAT
#ifdef LOL_FAT
            if (lil_id == leaf.info->id)
#endif
            {
                lil_id = new_leaf.info->id;
                // if we insert to right split node, we set leaf min
                lil_min = new_leaf.keys[0];
            }
#endif
        }
#ifdef LOL_FAT
        if (lol_id == leaf.info->id) {
            if (IQRDetector<key_type>::is_outlier(leaf.keys, leaf.info->size, new_leaf.keys[0])) {
                lol_max = new_leaf.keys[0];
            } else {
                lol_id = new_leaf.info->id;
                lol_min = new_leaf.keys[0];
            }
        }
#endif

        if (leaf.info->id == root_id)
        {
            create_new_root(new_leaf.keys[0], leaf, new_leaf);
            return true;
        }

        // insert new key to parent
        internal_insert(new_leaf.info->parent_id, new_leaf.keys[0], new_leaf_id, split_ratio);
        return true;
    }

public:
    bp_tree(const char *filepath, uint32_t blocks_in_memory, float split_ratio = 0.8) : manager(filepath, blocks_in_memory), size(0), depth(1)
    {
        assert((node_t::leaf_capacity + 1) * split_ratio < node_t::leaf_capacity + 1);
        assert(node_t::internal_capacity * split_ratio < node_t::internal_capacity);
        head_id = root_id = manager.allocate();
#ifdef LIL_FAT
        lil_id = head_id;
        lil_min = std::nullopt;
        lil_max = std::nullopt;
        ctr_lil = 0;
#endif
#ifdef LOL_FAT
        lol_id = head_id;
        lol_min = std::nullopt;
        lol_max = std::nullopt;
        ctr_lol = 0;
#endif
        node_t root(manager.open_block(root_id), bp_node_info::LEAF);
        manager.mark_dirty(root_id);
        root.info->id = root_id;
        root.info->size = 0;

        num_internal = 0;
        num_leaves = 1;
    }

    bool insert(const key_type &key, const value_type &value)
    {
#ifdef LOL_FAT
#ifdef LIL_FAT
        assert(lil_id != lol_id || (lil_min == lol_min && lil_max == lol_max));
#endif
#endif
        node_t leaf;
#ifdef LOL_FAT
        if ((!lol_min.has_value() || lol_min <= key) && (!lol_max.has_value() || key < lol_max)) {
            ctr_lol++;
            leaf.load(manager.open_block(lol_id));
            return leaf_insert(leaf, key, value);
        }
#endif
#ifdef LIL_FAT
        if ((!lil_min.has_value() || lil_min <= key) && (!lil_max.has_value() || key < lil_max)) {
            ctr_lil++;
            leaf.load(manager.open_block(lil_id));
            return leaf_insert(leaf, key, value);
        }
        std::optional<key_type> current_max =
#endif
        find_leaf(leaf, key);
#ifdef LIL_FAT
        lil_id = leaf.info->id;
        lil_max = current_max;
        if (head_id != leaf.info->id)
            lil_min = leaf.keys[0];
        else
            lil_min = std::nullopt;
#endif
        return leaf_insert(leaf, key, value);
    }

    std::optional<value_type> get(const key_type &key)
    {
        node_t leaf;
        find_leaf(leaf, key);
        uint32_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key)
        {
            return leaf.values[index];
        }
        return std::nullopt;
    }

    bool contains(const key_type &key) {
        return get(key).has_value();
    }
};

#endif
