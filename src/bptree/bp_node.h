#ifndef BP_NODE_H
#define BP_NODE_H

#include <algorithm>

enum bp_node_type {
    LEAF, INTERNAL
};

template<typename node_id_type, typename key_type, typename value_type>
class bp_node {
    struct node_info {
        node_id_type id;
        node_id_type next_id;
        uint16_t size;
        bp_node_type type; // not needed. use 1 bit from id?
    };
public:
    static constexpr uint16_t leaf_capacity = (BlockManager::block_size - sizeof(node_info)) /
                                              (sizeof(key_type) + sizeof(value_type));
    static constexpr uint16_t internal_capacity = (BlockManager::block_size - sizeof(node_info) - sizeof(node_id_type)) /
                                                  (sizeof(key_type) + sizeof(node_id_type));
    node_info *info;
    key_type *keys;
    union {
        node_id_type *children;
        value_type *values;
    };

    bp_node() = default;

    void load(void *buf) {
        info = static_cast<node_info *>(buf);
        keys = reinterpret_cast<key_type *>(info + 1);
        if (info->type == bp_node_type::LEAF) {
            values = reinterpret_cast<value_type *>(keys + leaf_capacity);
        } else {
            children = reinterpret_cast<node_id_type *>(keys + internal_capacity);
        }
    }

    void init(void *buf, const bp_node_type &type) {
        info = static_cast<node_info *>(buf);
        keys = reinterpret_cast<key_type *>(info + 1);
        info->type = type;
        if (info->type == bp_node_type::LEAF) {
            values = reinterpret_cast<value_type *>(keys + leaf_capacity);
        } else {
            children = reinterpret_cast<node_id_type *>(keys + internal_capacity);
        }
    }

    /**
     * Function finds suitable index where key can be placed in the current node
     * @param key
     * @return index of key slot
     */
    uint16_t value_slot(const key_type &key) const {
        assert(info->type == bp_node_type::LEAF);
        auto it = std::lower_bound(keys, keys + info->size, key);
//       assert(it == std::lower_bound(keys, keys + info->size, key));
        return std::distance(keys, it);
    }

    uint16_t child_slot(const key_type &key) const {
        assert(info->type == bp_node_type::INTERNAL);
        auto it = std::upper_bound(keys, keys + info->size, key);
//        assert(it == std::upper_bound(keys, keys + info->size, key));
        return std::distance(keys, it);
    }

};

#endif
