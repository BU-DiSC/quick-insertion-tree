#pragma once

#include <cstring>
#include <limits>
#include <mutex>
#include <optional>
#include <ranges>
#include <vector>

#include "BTreeNode.hpp"
#include "MemoryBlockManager.hpp"
#include "util.hpp"

// #include <shared_mutex>
// using std::shared_mutex;
#include "mtx.hpp"
using atm::shared_mutex;
// using srv::shared_mutex;
// using flg::shared_mutex;
// using spn::shared_mutex;
// using rwl::shared_mutex;
// using mtx::shared_mutex;

namespace ConcurrentSimpleBTree {
template <typename key_type, typename value_type>
class BTree {
public:
  using node_id_t = uint32_t;
  using BlockManager = InMemoryBlockManager<node_id_t>;
  using node_t = BTreeNode<node_id_t, key_type, value_type, BlockManager::block_size>;
  using step = node_id_t;
  using path_t = std::vector<step>;

  static constexpr const char *name = "ConcurrentSimpleBTree";
  static constexpr const bool concurrent = true;
  static constexpr uint16_t SPLIT_INTERNAL_POS = node_t::internal_capacity / 2;
  static constexpr uint16_t SPLIT_LEAF_POS = (node_t::leaf_capacity + 1) / 2;
  static constexpr node_id_t INVALID_NODE_ID = std::numeric_limits<node_id_t>::max();

  // not necessary but good to have
  std::atomic<uint32_t> size;

  // stats for benchmarking purposes
  mutable std::atomic<uint32_t> ctr_root_shared{};
  mutable uint32_t ctr_root_exclusive{};
  uint32_t ctr_root{};

  void reset_ctr() {
    ctr_root_shared = 0;
    ctr_root_exclusive = 0;
    ctr_root = 0;
  }

  explicit BTree(BlockManager &m) : manager(m), mutexes(m.get_capacity()), root_id(m.allocate()), head_id(m.allocate()), height(1) {
    node_t leaf(manager.open_block(head_id), bp_node_type::LEAF);
    manager.mark_dirty(head_id);
    leaf.info->id = head_id;
    leaf.info->next_id = INVALID_NODE_ID;
    leaf.info->size = 0;
    node_t root(manager.open_block(root_id), bp_node_type::INTERNAL);
    manager.mark_dirty(root_id);
    root.info->id = root_id;
    root.info->next_id = INVALID_NODE_ID;
    root.info->size = 0;
    root.children[0] = head_id;
  }

  bool update(const key_type &key, const value_type &value) {
    node_t leaf;
    find_leaf_exclusive(leaf, key);
    uint16_t index = leaf.value_slot(key);
    if (index >= leaf.info->size || leaf.keys[index] != key) {
      mutexes[leaf.info->id].unlock();
      return false;
    }
    manager.mark_dirty(leaf.info->id);
    leaf.values[index] = value;
    mutexes[leaf.info->id].unlock();
    return true;
  }

  static constexpr size_t OPT_RETRIES = 4;
  void insert(const key_type &key, const value_type &value) {
    node_t leaf;
    for (size_t i = 0; i < OPT_RETRIES; ++i) {
      find_leaf_exclusive(leaf, key);
      uint16_t index = leaf.value_slot(key);
      if (leaf_insert(leaf, index, key, value)) {
        return;
      }
      mutexes[leaf.info->id].unlock();
    }
    insert_pessimistic(key, value);
  }

  void insert_pessimistic(const key_type &key, const value_type &value) {
    node_t leaf;
    path_t path;
    find_leaf_exclusive(leaf, path, key);
    uint16_t index = leaf.value_slot(key);
    if (leaf_insert(leaf, index, key, value)) {
      // if the leaf is full but the key already exists we need to release the path
      for (const auto &parent_id : path) {
        mutexes[parent_id].unlock();
      }
      return;
    }
    split_insert(leaf, index, path, key, value);
  }

  uint32_t select_k(size_t count, const key_type &min_key) const {
    node_t leaf;
    find_leaf_shared(leaf, min_key);
    uint16_t index = leaf.value_slot(min_key);
    uint32_t loads = 1;
    uint16_t curr_size = leaf.info->size - index;
    while (count > curr_size) {
      count -= curr_size;
      node_id_t next_id = leaf.info->next_id;
      if (next_id == INVALID_NODE_ID) {
        break;
      }
      mutexes[next_id].lock_shared();
      mutexes[leaf.info->id].unlock_shared();
      leaf.load(manager.open_block(next_id));
      curr_size = leaf.info->size;
      ++loads;
    }
    mutexes[leaf.info->id].unlock_shared();
    return loads;
  }

  uint32_t range(const key_type &min_key, const key_type &max_key) const {
    uint32_t loads = 1;
    node_t leaf;
    find_leaf_shared(leaf, min_key);
    while (leaf.keys[leaf.info->size - 1] < max_key) {
      node_id_t next_id = leaf.info->next_id;
      if (next_id == INVALID_NODE_ID) {
        break;
      }
      mutexes[next_id].lock_shared();
      mutexes[leaf.info->id].unlock_shared();
      leaf.load(manager.open_block(next_id));
      ++loads;
    }
    mutexes[leaf.info->id].unlock_shared();
    return loads;
  }

  std::optional<value_type> get(const key_type &key) const {
    node_t leaf;
    find_leaf_shared(leaf, key);
    uint16_t index = leaf.value_slot(key);
    bool result = index < leaf.info->size && (leaf.keys[index] == key ? std::make_optional(leaf.values[index]) : std::nullopt);
    mutexes[leaf.info->id].unlock_shared();
    return result;
  }

  bool contains(const key_type &key) const {
    node_t leaf;
    find_leaf_shared(leaf, key);
    uint16_t index = leaf.value_slot(key);
    bool result = index < leaf.info->size && (leaf.keys[index] == key);
    mutexes[leaf.info->id].unlock_shared();
    return result;
  }

private:
  void create_new_root(const key_type &key, node_id_t right_node_id) {
    ++ctr_root;
    node_id_t left_node_id = manager.allocate();
    node_t root(manager.open_block(root_id));
    node_t left_node(manager.open_block(left_node_id));
    std::memcpy(left_node.info, root.info, BlockManager::block_size);
    left_node.info->id = left_node_id;
    manager.mark_dirty(left_node_id);
    manager.mark_dirty(root_id);
    root.info->size = 1;
    root.keys[0] = key;
    root.children[0] = left_node_id;
    root.children[1] = right_node_id;
    ++height;
  }

  void find_leaf_shared(node_t &node, const key_type &key) const {
    node_id_t parent_id = root_id;
    mutexes[parent_id].lock_shared();
    ctr_root_shared.fetch_add(1, std::memory_order_relaxed);
    node.load(manager.open_block(parent_id));
    do {
      const uint16_t slot = node.child_slot(key);
      const node_id_t child_id = node.children[slot];
      mutexes[child_id].lock_shared();
      mutexes[parent_id].unlock_shared();
      node.load(manager.open_block(child_id));
      parent_id = child_id;
    } while (node.info->type == bp_node_type::INTERNAL);
  }

  void find_leaf_exclusive(node_t &node, path_t &path, const key_type &key) const {
    node_id_t node_id = root_id;
    mutexes[node_id].lock();
    ++ctr_root_exclusive;
    path.reserve(height);
    node.load(manager.open_block(node_id));
    do {
      if (node.info->size < node_t::internal_capacity) {
        for (const auto &parent_id : path) {
          mutexes[parent_id].unlock();
        }
        path.clear();
      }
      path.push_back(node_id);
      uint16_t slot = node.child_slot(key);
      node_id = node.children[slot];
      mutexes[node_id].lock();
      node.load(manager.open_block(node_id));
    } while (node.info->type == bp_node_type::INTERNAL);
    if (node.info->size < node_t::leaf_capacity) {
      for (const auto &parent_id : path) {
        mutexes[parent_id].unlock();
      }
      path.clear();
    }
  }

  void find_leaf_exclusive(node_t &node, const key_type &key) const {
    node_id_t parent_id = root_id;
    mutexes[root_id].lock_shared();
    ctr_root_shared.fetch_add(1, std::memory_order_relaxed);
    uint8_t i = height;
    node.load(manager.open_block(parent_id));
    while (--i > 0) {
      const uint16_t slot = node.child_slot(key);
      const node_id_t child_id = node.children[slot];
      mutexes[child_id].lock_shared();
      mutexes[parent_id].unlock_shared();
      node.load(manager.open_block(child_id));
      parent_id = child_id;
    }
    const uint16_t slot = node.child_slot(key);
    const node_id_t leaf_id = node.children[slot];
    mutexes[leaf_id].lock();
    mutexes[parent_id].unlock_shared();
    node.load(manager.open_block(leaf_id));
  }

  void internal_insert(const path_t &path, key_type key, node_id_t child_id) {
    for (node_id_t node_id : std::ranges::reverse_view(path)) {
      node_t node(manager.open_block(node_id));
      uint16_t index = node.child_slot(key);
      manager.mark_dirty(node_id);
      if (node.info->size < node_t::internal_capacity) {
        std::memmove(node.keys + index + 1, node.keys + index, (node.info->size - index) * sizeof(key_type));
        std::memmove(node.children + index + 2, node.children + index + 1, (node.info->size - index) * sizeof(uint32_t));
        node.keys[index] = key;
        node.children[index + 1] = child_id;
        ++node.info->size;
        mutexes[node_id].unlock();
        return;
      }
      node_id_t new_node_id = manager.allocate();
      node_t new_node(manager.open_block(new_node_id), bp_node_type::INTERNAL);
      manager.mark_dirty(new_node_id);
      node.info->size = SPLIT_INTERNAL_POS;
      new_node.info->id = new_node_id;
      new_node.info->next_id = node.info->next_id;
      new_node.info->size = node_t::internal_capacity - node.info->size;
      node.info->next_id = new_node_id;
      if (index < node.info->size) {
        std::memcpy(new_node.keys, node.keys + node.info->size, new_node.info->size * sizeof(key_type));
        std::memmove(node.keys + index + 1, node.keys + index, (node.info->size - index) * sizeof(key_type));
        node.keys[index] = key;
        std::memcpy(new_node.children, node.children + node.info->size, (new_node.info->size + 1) * sizeof(uint32_t));
        std::memmove(node.children + index + 2, node.children + index + 1, (node.info->size - index + 1) * sizeof(uint32_t));
        node.children[index + 1] = child_id;
        key = node.keys[node.info->size];
      } else if (index == node.info->size) {
        std::memcpy(new_node.keys, node.keys + node.info->size, new_node.info->size * sizeof(key_type));
        std::memcpy(new_node.children + 1, node.children + 1 + node.info->size, new_node.info->size * sizeof(uint32_t));
        new_node.children[0] = child_id;
      } else {
        std::memcpy(new_node.keys, node.keys + node.info->size + 1, (index - node.info->size - 1) * sizeof(key_type));
        std::memcpy(new_node.keys + index - node.info->size, node.keys + index, (node_t::internal_capacity - index) * sizeof(key_type));
        new_node.keys[index - node.info->size - 1] = key;
        std::memcpy(new_node.children, node.children + 1 + node.info->size, (index - node.info->size) * sizeof(uint32_t));
        std::memcpy(new_node.children + 1 + index - node.info->size, node.children + 1 + index, new_node.info->size * sizeof(uint32_t));
        new_node.children[index - node.info->size] = child_id;
        key = node.keys[node.info->size];
      }
      child_id = new_node_id;
      if (node_id != root_id) {
        mutexes[node_id].unlock();
      }
    }
    create_new_root(key, child_id);
    mutexes[root_id].unlock();
  }

  bool leaf_insert(node_t &leaf, uint16_t index, const key_type &key, const value_type &value) {
    if (index < leaf.info->size && leaf.keys[index] == key) {
      manager.mark_dirty(leaf.info->id);
      leaf.values[index] = value;
      mutexes[leaf.info->id].unlock();
      return true;
    }
    if (leaf.info->size >= node_t::leaf_capacity) {
      return false;
    }
    size.fetch_add(1, std::memory_order_relaxed);
    manager.mark_dirty(leaf.info->id);
    std::memmove(leaf.keys + index + 1, leaf.keys + index, (leaf.info->size - index) * sizeof(key_type));
    std::memmove(leaf.values + index + 1, leaf.values + index, (leaf.info->size - index) * sizeof(value_type));
    leaf.keys[index] = key;
    leaf.values[index] = value;
    ++leaf.info->size;
    mutexes[leaf.info->id].unlock();
    return true;
  }

  void split_insert(node_t &leaf, uint16_t index, const path_t &path, const key_type &key, const value_type &value) {
    size.fetch_add(1, std::memory_order_relaxed);
    uint16_t split_leaf_pos = SPLIT_LEAF_POS;
    node_id_t new_leaf_id = manager.allocate();
    node_t new_leaf(manager.open_block(new_leaf_id), bp_node_type::LEAF);
    manager.mark_dirty(new_leaf_id);
    new_leaf.info->id = new_leaf_id;
    new_leaf.info->next_id = leaf.info->next_id;
    new_leaf.info->size = node_t::leaf_capacity + 1 - split_leaf_pos;
    leaf.info->next_id = new_leaf_id;
    leaf.info->size = split_leaf_pos;
    if (index < leaf.info->size) {
      std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size - 1, new_leaf.info->size * sizeof(key_type));
      std::memmove(leaf.keys + index + 1, leaf.keys + index, (leaf.info->size - index - 1) * sizeof(key_type));
      leaf.keys[index] = key;
      std::memcpy(new_leaf.values, leaf.values + leaf.info->size - 1, new_leaf.info->size * sizeof(value_type));
      std::memmove(leaf.values + index + 1, leaf.values + index, (leaf.info->size - index - 1) * sizeof(value_type));
      leaf.values[index] = value;
    } else {
      uint16_t new_index = index - leaf.info->size;
      std::memcpy(new_leaf.keys, leaf.keys + leaf.info->size, new_index * sizeof(key_type));
      new_leaf.keys[new_index] = key;
      std::memcpy(new_leaf.keys + new_index + 1, leaf.keys + index, (node_t::leaf_capacity - index) * sizeof(key_type));
      std::memcpy(new_leaf.values, leaf.values + leaf.info->size, new_index * sizeof(value_type));
      new_leaf.values[new_index] = value;
      std::memcpy(new_leaf.values + new_index + 1, leaf.values + index, (node_t::leaf_capacity - index) * sizeof(value_type));
    }
    mutexes[leaf.info->id].unlock();
    internal_insert(path, new_leaf.keys[0], new_leaf_id);
  }

  BlockManager &manager;
  mutable std::vector<shared_mutex> mutexes;
  const node_id_t root_id;
  node_id_t head_id;
  uint8_t height;
};
} // namespace ConcurrentSimpleBTree
