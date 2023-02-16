#ifndef BP_TREE_H
#define BP_TREE_H

#include "kv_store.h"
#include "block_manager.h"
#include "bp_node.h"

template<typename key_type, typename value_type>
class dual_tree;

template<typename key_type, typename value_type>
class bp_tree : public kv_store<key_type, value_type> {

    std::ostream &get_stats(std::ostream &os) const override {
        os << "SIMPLE, " << size << ", " << depth << ", " << manager.getNumWrites();
        return os;
    }

protected:
    using node_t = bp_node<key_type, value_type>;

    const uint32_t split_internal_pos;
    const uint32_t split_leaf_pos;

    uint32_t root_id;
    uint32_t head_id;
    uint32_t tail_id;
    uint32_t size;
    uint32_t depth;
    key_type tree_min;
    key_type tail_min;
    key_type tail_max;
    BlockManager manager;

    void create_new_root(key_type key, node_t &node, node_t &new_node) {
        assert(root_id == node.info->id);
        depth++;
        root_id = manager.allocate();
        node_t root(manager.open_block(root_id), bp_node_info::INTERNAL);
//        It is expected that the root is already marked dirty, so we don't need to mark it again
//        manager.mark_dirty(root_id);
        root.info->id = root_id;
        root.info->size = 1;
        root.keys[0] = key;
        root.children[0] = node.info->id;
        root.children[1] = new_node.info->id;
        node.info->parent_id = root_id;
        new_node.info->parent_id = root_id;
    }

    void find_leaf(node_t &node, const key_type &key) {
        uint32_t child_id = root_id;
        while (true) {
            node.load(manager.open_block(child_id));
            assert(child_id == node.info->id);
            if (node.info->type == bp_node_info::LEAF) break;
            child_id = node.children[node.child_slot(key)];
        }
    }

    void internal_insert(uint32_t node_id, key_type key, uint32_t child_id) {
        while (true) {
            node_t node(manager.open_block(node_id));
            assert(node.info->id == node_id);
            assert(node.info->type == bp_node_info::INTERNAL);
            uint32_t index = node.child_slot(key);
            assert(index == node_t::internal_capacity || node.keys[index] != key);
            manager.mark_dirty(node_id);
            if (node.info->size < node_t::internal_capacity) {
                // insert new key
                std::copy_backward(node.keys + index, node.keys + node.info->size, node.keys + node.info->size + 1);
                std::copy_backward(node.children + index + 1, node.children + node.info->size + 1,
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

            node.info->size = split_internal_pos;
            new_node.info->id = new_node_id;
            new_node.info->parent_id = node.info->parent_id; // same parent?
            new_node.info->size = node_t::internal_capacity - node.info->size;

            if (index < node.info->size) {
                std::copy(node.keys + node.info->size, node.keys + node_t::internal_capacity, new_node.keys);
                std::copy_backward(node.keys + index, node.keys + node.info->size, node.keys + node.info->size + 1);
                node.keys[index] = key;

                std::copy(node.children + node.info->size, node.children + 1 + node_t::internal_capacity,
                          new_node.children);
                std::copy_backward(node.children + index + 1, node.children + node.info->size + 1,
                                   node.children + node.info->size + 2);
                node.children[index + 1] = child_id;

                key = node.keys[node.info->size];
            } else if (index == node.info->size) {
                std::copy(node.keys + node.info->size, node.keys + node_t::internal_capacity, new_node.keys);
                std::copy(node.children + 1 + node.info->size, node.children + 1 + node_t::internal_capacity,
                          new_node.children + 1);
                new_node.children[0] = child_id;
            } else {
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

            for (int i = 0; i <= new_node.info->size; ++i) {
                node_t child(manager.open_block(new_node.children[i]));
                assert(child.info->id == new_node.children[i]);
                manager.mark_dirty(child.info->id);
                child.info->parent_id = new_node_id;
            }

            if (node.info->id == root_id) {
                create_new_root(key, node, new_node);
                return;
            }

            node_id = node.info->parent_id;
            child_id = new_node_id;
        }
    }

    virtual void update_stats(const node_t &leaf) {
        throw std::runtime_error("not implemented");
    }

    bool leaf_insert(node_t &leaf, const key_type &key, const value_type &value) {
        manager.mark_dirty(leaf.info->id);
        uint32_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key) {
            // update value
            leaf.values[index] = value;
            return false;
        }
        size++;
        if (leaf.info->size < node_t::leaf_capacity) {
            // insert new key
            std::copy_backward(leaf.keys + index, leaf.keys + leaf.info->size, leaf.keys + leaf.info->size + 1);
            std::copy_backward(leaf.values + index, leaf.values + leaf.info->size, leaf.values + leaf.info->size + 1);
            leaf.keys[index] = key;
            leaf.values[index] = value;
            leaf.info->size++;
            if (head_id == leaf.info->id) { // && index == 0
                tree_min = leaf.keys[0];
            }
            if (tail_id == leaf.info->id) { // && index == 0
                tail_min = leaf.keys[0];
                tail_max = leaf.keys[leaf.info->size - 1];
            }
            return true;
        }
        // split the leaf
        uint32_t new_leaf_id = manager.allocate();
        node_t new_leaf(manager.open_block(new_leaf_id), bp_node_info::LEAF);
        manager.mark_dirty(new_leaf_id);

        leaf.info->size = split_leaf_pos;
        new_leaf.info->id = new_leaf_id;
        new_leaf.info->parent_id = leaf.info->parent_id; // same parent?
        new_leaf.info->next_id = leaf.info->next_id;
        leaf.info->next_id = new_leaf_id;
        new_leaf.info->size = node_t::leaf_capacity + 1 - leaf.info->size;

        if (index < leaf.info->size) {
            std::copy(leaf.keys + leaf.info->size - 1, leaf.keys + node_t::leaf_capacity, new_leaf.keys);
            std::copy_backward(leaf.keys + index, leaf.keys + leaf.info->size, leaf.keys + leaf.info->size + 1);
            leaf.keys[index] = key;
            std::copy(leaf.values + leaf.info->size - 1, leaf.values + node_t::leaf_capacity, new_leaf.values);
            std::copy_backward(leaf.values + index, leaf.values + leaf.info->size, leaf.values + leaf.info->size + 1);
            leaf.values[index] = value;
        } else {
            std::copy(leaf.keys + leaf.info->size, leaf.keys + index, new_leaf.keys);
            std::copy(leaf.keys + index, leaf.keys + node_t::leaf_capacity,
                      new_leaf.keys + index - leaf.info->size + 1);
            new_leaf.keys[index - leaf.info->size] = key;
            std::copy(leaf.values + leaf.info->size, leaf.values + index, new_leaf.values);
            std::copy(leaf.values + index, leaf.values + node_t::leaf_capacity,
                      new_leaf.values + index - leaf.info->size + 1);
            new_leaf.values[index - leaf.info->size] = value;
        }

        if (head_id == leaf.info->id) { // && index == 0
            tree_min = leaf.keys[0];
        }
        if (tail_id == leaf.info->id) {
            tail_id = new_leaf_id;
            tail_min = new_leaf.keys[0];
            tail_max = new_leaf.keys[new_leaf.info->size - 1];
            update_stats(leaf);
        }

        if (leaf.info->id == root_id) {
            create_new_root(new_leaf.keys[0], leaf, new_leaf);
            return true;
        }

        // insert new key to parent
        internal_insert(new_leaf.info->parent_id, new_leaf.keys[0], new_leaf_id);
        return true;
    }

    bool leaf_update(node_t &leaf, const key_type &key, const value_type &value) {
        uint32_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key) {
            // update value
            manager.mark_dirty(leaf.info->id);
            leaf.values[index] = value;
            return true;
        }
        return false;
    }

    friend class dual_tree<key_type, value_type>;

public:
    bp_tree(const char *filepath, uint32_t blocks_in_memory, float split_frac = 0.8) :
            manager(filepath, blocks_in_memory), size(0), depth(1),
            split_internal_pos(node_t::internal_capacity * split_frac),
            split_leaf_pos((node_t::leaf_capacity + 1) * split_frac) {
        assert(split_leaf_pos < node_t::leaf_capacity + 1);
        assert(split_internal_pos < node_t::internal_capacity);
        head_id = tail_id = root_id = manager.allocate();
        node_t root(manager.open_block(root_id), bp_node_info::LEAF);
        manager.mark_dirty(root_id);
        root.info->id = root_id;
        root.info->size = 0;
    }

    bool update(const key_type &key, const value_type &value) override {
        node_t leaf(manager.open_block(root_id));
        assert(leaf.info->id == root_id);
        find_leaf(leaf, key);
        return leaf_update(leaf, key, value);
    }

    bool insert(const key_type &key, const value_type &value) override {
        node_t leaf;
        find_leaf(leaf, key);
        return leaf_insert(leaf, key, value);
    }

    std::optional<value_type> get(const key_type &key) override {
        node_t leaf(manager.open_block(root_id));
        assert(leaf.info->id == root_id);
        find_leaf(leaf, key);
        uint32_t index = leaf.value_slot(key);
        if (index < leaf.info->size && leaf.keys[index] == key) {
            return leaf.values[index];
        }
        return std::nullopt;
    }
};

#endif
