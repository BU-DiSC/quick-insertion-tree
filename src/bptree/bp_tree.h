#ifndef BP_TREE_H
#define BP_TREE_H

#include "kv_store.h"
#include "block_manager.h"
#include "bp_node.h"

template <typename key_type, typename value_type>
class dual_tree;

template <typename key_type, typename value_type>
class bp_tree : public kv_store<key_type, value_type>
{

    std::ostream &get_stats(std::ostream &os) const override
    {
        os << "SIMPLE, " << size << ", " << depth << ", " << manager.getNumWrites() << ", " << num_internal << ", " << num_leaves;
        return os;
    }

protected:
    using node_t = bp_node<key_type, value_type>;

    const float tail_split_ratio;

    uint32_t root_id;
    uint32_t head_id;
    uint32_t tail_id;
#ifdef LIL_FAT
    uint32_t last_insert_id;
    std::optional<key_type> leaf_min;
    std::optional<key_type>
        leaf_max;
#endif
    uint32_t size;
    uint32_t depth;
    key_type tree_min;
    key_type tail_min;
    key_type tree_max;
    BlockManager manager;

    uint32_t num_internal;
    uint32_t num_leaves;

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

    void copy_backward_keys(key_type* first, key_type* last, key_type* d_last){
        while (first != last){
            *(--d_last) = *(--last);
        }
    }

    void copy_backward_children(uint32_t* first, uint32_t* last, uint32_t* d_last){
        while (first != last){
            *(--d_last) = *(--last);
        }
    }

    void copy_backward_values(value_type* first, value_type* last, value_type* d_last){
        while (first != last){
            *(--d_last) = *(--last);
        }
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
                copy_backward_keys(node.keys + index, node.keys + node.info->size, node.keys + node.info->size + 1);
                copy_backward_children(node.children + index + 1, node.children + node.info->size + 1,
                                   node.children + node.info->size + 2);
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
                std::copy(node.keys + node.info->size, node.keys + node_t::internal_capacity, new_node.keys);
                copy_backward_keys(node.keys + index, node.keys + node.info->size, node.keys + node.info->size + 1);
                node.keys[index] = key;

                std::copy(node.children + node.info->size, node.children + 1 + node_t::internal_capacity,
                          new_node.children);
                copy_backward_children(node.children + index + 1, node.children + node.info->size + 1,
                                   node.children + node.info->size + 2);
                node.children[index + 1] = child_id;

                key = node.keys[node.info->size];
                // key = new_node.keys[0];
            }
            else if (index == node.info->size)
            {
                std::copy(node.keys + node.info->size, node.keys + node_t::internal_capacity, new_node.keys);
                std::copy(node.children + 1 + node.info->size, node.children + 1 + node_t::internal_capacity,
                          new_node.children + 1);
                new_node.children[0] = child_id;
            }
            else
            {
                std::copy(node.keys + node.info->size + 1, node.keys + index, new_node.keys);
                std::copy(node.keys + index, node.keys + node_t::internal_capacity,
                          new_node.keys + index - node.info->size);
                new_node.keys[index - node.info->size - 1] = key;

                std::copy(node.children + 1 + node.info->size, node.children + 1 + index, new_node.children);
                std::copy(node.children + 1 + index, node.children + 1 + node_t::internal_capacity,
                          new_node.children + 1 + index - node.info->size);
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
#ifdef LIL_FAT
        last_insert_id = leaf.info->id;
#endif
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
            copy_backward_keys(leaf.keys + index, leaf.keys + leaf.info->size, leaf.keys + leaf.info->size + 1);
            copy_backward_values(leaf.values + index, leaf.values + leaf.info->size, leaf.values + leaf.info->size + 1);
            leaf.keys[index] = key;
            leaf.values[index] = value;
            leaf.info->size++;
            if (head_id == leaf.info->id)
            { // && index == 0
                tree_min = leaf.keys[0];
            }
            if (tail_id == leaf.info->id)
            { // && index == leaf.info->size - 1
                tree_max = leaf.keys[leaf.info->size - 1];
            }
            return true;
        }
        // split the leaf
        uint32_t new_leaf_id = manager.allocate();
        node_t new_leaf(manager.open_block(new_leaf_id), bp_node_info::LEAF);
        manager.mark_dirty(new_leaf_id);
        num_leaves++;

        float split_ratio = tail_id == leaf.info->id ? tail_split_ratio : 0.5;
        leaf.info->size = (node_t::leaf_capacity + 1) * split_ratio;
        new_leaf.info->id = new_leaf_id;
        new_leaf.info->parent_id = leaf.info->parent_id; // same parent?
        new_leaf.info->next_id = leaf.info->next_id;
        leaf.info->next_id = new_leaf_id;
        new_leaf.info->size = node_t::leaf_capacity + 1 - leaf.info->size;

        if (index < leaf.info->size)
        {
            std::copy(leaf.keys + leaf.info->size - 1, leaf.keys + node_t::leaf_capacity, new_leaf.keys);
            copy_backward_keys(leaf.keys + index, leaf.keys + leaf.info->size, leaf.keys + leaf.info->size + 1);
            leaf.keys[index] = key;
            std::copy(leaf.values + leaf.info->size - 1, leaf.values + node_t::leaf_capacity, new_leaf.values);
            copy_backward_values(leaf.values + index, leaf.values + leaf.info->size, leaf.values + leaf.info->size + 1);
            leaf.values[index] = value;

#ifdef LIL_FAT
            // if we insert to left node of split, we set the leaf max
            leaf_max = new_leaf.keys[0];
            if (leaf.info->id == head_id)
            {
                leaf_min = std::nullopt;
            }
#endif
        }
        else
        {
            std::copy(leaf.keys + leaf.info->size, leaf.keys + index, new_leaf.keys);
            std::copy(leaf.keys + index, leaf.keys + node_t::leaf_capacity,
                      new_leaf.keys + index - leaf.info->size + 1);
            new_leaf.keys[index - leaf.info->size] = key;
            std::copy(leaf.values + leaf.info->size, leaf.values + index, new_leaf.values);
            std::copy(leaf.values + index, leaf.values + node_t::leaf_capacity,
                      new_leaf.values + index - leaf.info->size + 1);
            new_leaf.values[index - leaf.info->size] = value;
#ifdef LIL_FAT
            last_insert_id = new_leaf.info->id;
            // if we insert to right split node, we set leaf min
            leaf_min = new_leaf.keys[0];
#endif
        }

        if (head_id == leaf.info->id)
        { // && index == 0
            tree_min = leaf.keys[0];
        }
        if (tail_id == leaf.info->id)
        {
            tail_id = new_leaf_id;
            tail_min = new_leaf.keys[0];
            tree_max = new_leaf.keys[new_leaf.info->size - 1];
        }

        if (leaf.info->id == root_id)
        {
            create_new_root(new_leaf.keys[0], leaf, new_leaf);
            return true;
        }

        // insert new key to parent
        // internal_insert(new_leaf.info->parent_id, leaf.keys[leaf.info->size - 1], new_leaf_id, split_ratio);
        internal_insert(new_leaf.info->parent_id, new_leaf.keys[0], new_leaf_id, split_ratio);
        return true;
    }

    bool leaf_update(node_t &leaf, const key_type &key, const value_type &value)
    {
        uint32_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key)
        {
#ifdef LIL_FAT
            last_insert_id = leaf.info->id;
#endif
            // update value
            manager.mark_dirty(leaf.info->id);
            leaf.values[index] = value;
            return true;
        }
        return false;
    }

    friend class dual_tree<key_type, value_type>;

public:
    bp_tree(const char *filepath, uint32_t blocks_in_memory, float split_ratio = 0.8) : manager(filepath, blocks_in_memory), size(0), depth(1), tail_split_ratio(split_ratio)
    {
        assert((node_t::leaf_capacity + 1) * split_ratio < node_t::leaf_capacity + 1);
        assert(node_t::internal_capacity * split_ratio < node_t::internal_capacity);
        head_id = tail_id = root_id = manager.allocate();
#ifdef LIL_FAT
        last_insert_id = head_id;
        leaf_min = std::nullopt;
        leaf_max = std::nullopt;
#endif
        node_t root(manager.open_block(root_id), bp_node_info::LEAF);
        manager.mark_dirty(root_id);
        root.info->id = root_id;
        root.info->size = 0;

        num_internal = 0;
        num_leaves = 1;
    }

    bool update(const key_type &key, const value_type &value) override
    {
        node_t leaf;
        find_leaf(leaf, key);
        return leaf_update(leaf, key, value);
    }

    bool insert(const key_type &key, const value_type &value) override
    {
        node_t leaf;
        std::optional<key_type> current_max;
        current_max = find_leaf(leaf, key);
#ifdef LIL_FAT
        if (size == 0)
            leaf_max = std::nullopt;
        else
            leaf_max = current_max;

        if (head_id != leaf.info->id)
            leaf_min = leaf.keys[0];
        else
            leaf_min = std::nullopt;
#endif
        // leaf_min = head_id == leaf.info->id ? std::nullopt : leaf.keys[0];
        return leaf_insert(leaf, key, value);
    }

    std::optional<value_type> get(const key_type &key) override
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
};

#endif
