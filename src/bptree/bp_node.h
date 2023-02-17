#ifndef BP_NODE_H
#define BP_NODE_H

#include <algorithm>
#include <ostream>

#include "block_manager.h"
#include "kv_store.h"

struct bp_node_info {
    uint32_t id;
    uint32_t parent_id;
    uint32_t next_id;
    uint32_t size;
    enum bp_node_type {
        LEAF, INTERNAL
    } type;
};

template<typename key_type, typename value_type>
class bp_node {
public:
    static constexpr uint32_t leaf_capacity =
            (BlockManager::block_size - sizeof(bp_node_info)) / (sizeof(key_type) + sizeof(value_type));
    static constexpr uint32_t internal_capacity = (BlockManager::block_size - sizeof(bp_node_info) - sizeof(uint32_t)) /
                                                  (sizeof(key_type) + sizeof(uint32_t));
    bp_node_info *info;
    key_type *keys;
    union {
        uint32_t *children;
        value_type *values;
    };

    bp_node() = default;

    explicit bp_node(void *buf) {
        load(buf);
    }

    bp_node(void *buf, const bp_node_info::bp_node_type &type) {
        load(buf, type);
    }

    void load(void *buf) {
        info = static_cast<bp_node_info *>(buf);
        keys = reinterpret_cast<key_type *>(info + 1);
        if (info->type == bp_node_info::LEAF) {
            values = reinterpret_cast<value_type *>(keys + leaf_capacity);
        } else {
            children = reinterpret_cast<uint32_t *>(keys + internal_capacity);
        }
    }

    void load(void *buf, const bp_node_info::bp_node_type &type) {
        info = static_cast<bp_node_info *>(buf);
        keys = reinterpret_cast<key_type *>(info + 1);
        info->type = type;
        if (info->type == bp_node_info::LEAF) {
            values = reinterpret_cast<value_type *>(keys + leaf_capacity);
        } else {
            children = reinterpret_cast<uint32_t *>(keys + internal_capacity);
        }
    }

    /**
     * Function finds suitable index where key can be placed in the current node
     * @param key
     * @return index of key slot
     */
    uint32_t value_slot(const key_type &key) const {
        assert(info->type == bp_node_info::LEAF);
        return std::lower_bound(keys, keys + info->size, key) - keys;
    }

    uint32_t child_slot(const key_type &key) const {
        assert(info->type == bp_node_info::INTERNAL);
        return std::lower_bound(keys, keys + info->size, key) - keys;
    }

};

#endif
