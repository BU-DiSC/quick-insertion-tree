#pragma once

#include <algorithm>

enum bp_node_type {
  LEAF,
  INTERNAL
};

template <typename node_id_type, typename key_type, typename value_type, size_t block_size>
class BTreeNode {
  struct node_info {
    node_id_type id;
    node_id_type next_id;
    uint16_t size;
    uint16_t type;
  };

public:
  static constexpr uint16_t leaf_capacity = (block_size - sizeof(node_info)) / (sizeof(key_type) + sizeof(value_type));
  static constexpr uint16_t internal_capacity = (block_size - sizeof(node_info) - sizeof(node_id_type)) / (sizeof(key_type) + sizeof(node_id_type));

  node_info *info;
  key_type *keys;
  union {
    node_id_type *children;
    value_type *values;
  };

  BTreeNode() = default;

  explicit BTreeNode(void *buf) {
    load(buf);
  }

  void load(void *buf) {
    info = static_cast<node_info *>(buf);
    keys = reinterpret_cast<key_type *>(info + 1);
    if (info->type == bp_node_type::LEAF) {
      values = reinterpret_cast<value_type *>(keys + leaf_capacity);
    } else {
      children = reinterpret_cast<node_id_type *>(keys + internal_capacity);
    }
  }

  BTreeNode(void *buf, const bp_node_type &type) {
    info = static_cast<node_info *>(buf);
    keys = reinterpret_cast<key_type *>(info + 1);
    info->type = type;
    if (info->type == bp_node_type::LEAF) {
      values = reinterpret_cast<value_type *>(keys + leaf_capacity);
    } else {
      children = reinterpret_cast<node_id_type *>(keys + internal_capacity);
    }
  }

  uint16_t value_slot(const key_type &key) const {
    auto it = std::lower_bound(keys, keys + info->size, key);
    return std::distance(keys, it);
  }

  uint16_t value_slot2(const key_type &key) const {
    auto it = std::upper_bound(keys, keys + info->size, key);
    return std::distance(keys, it);
  }

  uint16_t child_slot(const key_type &key) const {
    auto it = std::upper_bound(keys, keys + info->size, key);
    return std::distance(keys, it);
  }
};
