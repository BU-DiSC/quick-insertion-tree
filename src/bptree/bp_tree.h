#ifndef BP_TREE_H
#define BP_TREE_H

#include <algorithm>
#include <cassert>
#include <cstring>
#include <mutex>
#include <optional>
#include <ranges>
#include <vector>

#include "mtx.h"

// using std::shared_mutex;
using atm::shared_mutex;
// using srv::shared_mutex;
// using flg::shared_mutex;
// using spn::shared_mutex;
// using rwl::shared_mutex;
// using mtx::shared_mutex;

#ifdef LOL_FAT
#ifdef VARIABLE_SPLIT
#ifdef LOL_RESET
#define QUIT_FAT
#endif
#endif

#include <cmath>
#ifdef FAST_PATH
#error "FAST_PATH already defined"
#endif
#define FAST_PATH
#include "outlier_detector.h"

struct reset_stats {
  std::atomic<uint8_t> fails;
  const uint8_t threshold;

  explicit reset_stats(uint8_t t) : fails(0), threshold(t) {}

  void success() { fails = 0; }

  bool failure() {
    if (fails >= threshold) {
      return true;
    }
    ++fails;
    return false;
  }

  void reset() { fails = 0; }
};
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

template <typename key_type, typename value_type>
class bp_tree {
  friend std::ostream &operator<<(std::ostream &os, const bp_tree &tree) {
    os << tree.ctr_fast << ", " << tree.ctr_fast_fail;
    return os;
  }

  using node_id_t = uint32_t;
  using node_t = bp_node<node_id_t, key_type, value_type>;
#ifdef LOL_FAT
  using dist_f = std::size_t (*)(const key_type &, const key_type &);
#endif
  using step = node_id_t;
  using path_t = std::vector<step>;

  static constexpr uint16_t SPLIT_INTERNAL_POS = node_t::internal_capacity / 2;
  static constexpr uint16_t SPLIT_LEAF_POS = (node_t::leaf_capacity + 1) / 2;
  static constexpr uint16_t IQR_SIZE_THRESH = SPLIT_LEAF_POS;
  static constexpr node_id_t INVALID_NODE_ID = -1;
#ifdef LOL_FAT
  dist_f dist;
#endif
  BlockManager &manager;
  mutable std::vector<shared_mutex> mutexes;
  const node_id_t root_id;
  node_id_t head_id;
  node_id_t tail_id;
#ifdef FAST_PATH
  mutable shared_mutex fp_mutex;
#ifdef LOL_FAT
  node_id_t fp_id;
#endif
  key_type fp_min;
#ifdef LOL_FAT
  key_type fp_max;
  node_id_t fp_prev_id;
  key_type fp_prev_min;
  uint16_t fp_prev_size;
  uint16_t fp_size;
#endif
#endif
  uint8_t height;
#ifdef LOL_RESET
  reset_stats life;
#endif
  std::atomic<uint32_t> ctr_fast;
  std::atomic<uint32_t> ctr_fast_fail;
  std::atomic<uint32_t> ctr_size;

  void create_new_root(const key_type &key, node_id_t right_node_id) {
    node_id_t left_node_id = manager.allocate();
    node_t root(manager.open_block(root_id));
    node_t left_node(manager.open_block(left_node_id));
    std::memcpy(left_node.info, root.info, BLOCK_SIZE_BYTES);
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
    node_id_t node_id = root_id;
    mutexes[node_id].lock_shared();
    node.load(manager.open_block(node_id));
    do {
      const node_id_t parent_id = node_id;
      const uint16_t slot = node.child_slot(key);
      node_id = node.children[slot];
      mutexes[node_id].lock_shared();
      mutexes[parent_id].unlock_shared();
      node.load(manager.open_block(node_id));
    } while (node.info->type == INTERNAL);
  }

  void find_leaf_exclusive(node_t &node, path_t &path, const key_type &key
#ifdef LOL_FAT
                           , key_type &leaf_max
#endif
#ifdef FAST_PATH
                           , node_id_t leaf_id = INVALID_NODE_ID
#endif
  ) const {
//    locks.lock(root_id);
    mutexes[root_id].lock();
    path.reserve(height);
    node.load(manager.open_block(root_id));

    do {
      if (node.info->size < node_t::internal_capacity) {
        for (const auto &parent_id : path) {
//          locks.unlock(parent_id);
          mutexes[parent_id].unlock();
        }
        path.clear();
      }
      path.push_back(node.info->id);
      uint16_t slot = node.child_slot(key);
#ifdef LOL_FAT
      if (slot != node.info->size) {
        leaf_max = node.keys[slot];
      }
#endif
      node_id_t node_id = node.children[slot];
#ifdef FAST_PATH
      if (leaf_id != node_id)
#endif
      {
//        locks.lock(node_id);
        mutexes[node_id].lock();
      }
      node.load(manager.open_block(node_id));
    } while (node.info->type == bp_node_type::INTERNAL);
    if (node.info->size < node_t::leaf_capacity) {
      for (const auto &parent_id : path) {
//        locks.unlock(parent_id);
        mutexes[parent_id].unlock();
      }
      path.clear();
    }
  }

  void find_leaf_exclusive(node_t &node, const key_type &key
#ifdef LOL_FAT
                           , key_type &leaf_max
#endif
  ) const {
    node_id_t parent_id = root_id;
//    locks.lock_shared(root_id);
    mutexes[root_id].lock_shared();
    uint8_t i = height;
    node.load(manager.open_block(parent_id));
//    assert(node.info->type == INTERNAL);
    while (--i > 0) {
      const uint16_t slot = node.child_slot(key);
#ifdef LOL_FAT
      if (slot != node.info->size) {
        leaf_max = node.keys[slot];
      }
#endif
      const node_id_t child_id = node.children[slot];
//      locks.lock_shared(child_id);
      mutexes[child_id].lock_shared();
//      locks.unlock_shared(parent_id);
      mutexes[parent_id].unlock_shared();
      node.load(manager.open_block(child_id));
//      assert(node.info->type == INTERNAL);
      parent_id = child_id;
    }
    const uint16_t slot = node.child_slot(key);
#ifdef LOL_FAT
    if (slot < node.info->size) {
      leaf_max = node.keys[slot];
    }
#endif
    const node_id_t leaf_id = node.children[slot];
//    locks.lock(leaf_id);
    mutexes[leaf_id].lock();
//    locks.unlock_shared(parent_id);
    mutexes[parent_id].unlock_shared();
    node.load(manager.open_block(leaf_id));
//    assert(node.info->type == LEAF);
  }

  void internal_insert(const path_t &path, key_type key, node_id_t child_id) {
    for (node_id_t node_id : std::ranges::reverse_view(path)) {
      node_t node(manager.open_block(node_id));
//      assert(node.info->id == node_id);
//      assert(node.info->type == bp_node_type::INTERNAL);
      uint16_t index = node.child_slot(key);
//      assert(index == node.info->size || node.keys[index] != key);
      manager.mark_dirty(node_id);
      if (node.info->size < node_t::internal_capacity) {
        std::memmove(node.keys + index + 1, node.keys + index, (node.info->size - index) * sizeof(key_type));
        std::memmove(node.children + index + 2, node.children + index + 1, (node.info->size - index) * sizeof(uint32_t));
        node.keys[index] = key;
        node.children[index + 1] = child_id;
        ++node.info->size;
//        locks.unlock(node_id);
        mutexes[node_id].unlock();
        return;
      }

      node_id_t new_node_id = manager.allocate();
      node_t new_node(manager.open_block(new_node_id), INTERNAL);
      manager.mark_dirty(new_node_id);

      node.info->size = SPLIT_INTERNAL_POS;
      new_node.info->id = new_node_id;
      new_node.info->size = node_t::internal_capacity - node.info->size;

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
//        locks.unlock(node_id);
        mutexes[node_id].unlock();
      }
    }
    create_new_root(key, child_id);
//    locks.unlock(root_id);
    mutexes[root_id].unlock();
  }

#ifdef REDISTRIBUTE
  void update_internal(const path_t &path, const key_type &old_key, const key_type &new_key) {
    node_t node;
    uint8_t depth = height;
    for (uint8_t i = 1; i < depth; i++) {
      node_id_t node_id = path[i];
      node.load(manager.open_block(node_id));
//      assert(node.info->id == node_id);
//      assert(node.info->type == bp_node_type::INTERNAL);
      uint16_t index = node.child_slot(old_key) - 1;
      if (index < node.info->size && node.keys[index] == old_key) {
        manager.mark_dirty(node_id);
        node.keys[index] = new_key;
        return;
      }
    }
    assert(false);
  }

  void redistribute(const path_t &path, const node_t &leaf, uint16_t index, const key_type &key, const value_type &value) {
//    assert(fp_prev_id != INVALID_NODE_ID);

    uint16_t items = IQR_SIZE_THRESH - fp_prev_size;
    manager.mark_dirty(fp_prev_id);
    node_t fp_prev;
    fp_prev.load(manager.open_block(fp_prev_id));
//    assert(fp_prev_id == fp_prev.info->id);
//    assert(fp_prev.info->type == bp_node_type::LEAF);
    if (index < items) {
      --items;
      std::memcpy(fp_prev.keys + fp_prev_size, leaf.keys, index * sizeof(key_type));
      std::memcpy(fp_prev.keys + fp_prev_size + index + 1, leaf.keys + index, (items - index) * sizeof(key_type));
      fp_prev.keys[fp_prev_size + index] = key;
      std::memcpy(fp_prev.values + fp_prev_size, leaf.values, index * sizeof(value_type));
      std::memcpy(fp_prev.values + fp_prev_size + index + 1, leaf.values + index, (items - index) * sizeof(value_type));
      fp_prev.values[fp_prev_size + index] = value;

      std::memmove(leaf.keys, leaf.keys + items, (fp_size - items) * sizeof(key_type));
      std::memmove(leaf.values, leaf.values + items, (fp_size - items) * sizeof(value_type));
      ++items;
    } else {
      std::memcpy(fp_prev.keys + fp_prev_size, leaf.keys, items * sizeof(key_type));
      std::memcpy(fp_prev.values + fp_prev_size, leaf.values, items * sizeof(key_type));

      uint16_t new_index = index - items;
      std::memmove(leaf.keys, leaf.keys + items, new_index * sizeof(key_type));
      std::memmove(leaf.keys + new_index + 1, leaf.keys + index, (fp_size - index) * sizeof(key_type));
      leaf.keys[new_index] = key;
      std::memmove(leaf.values, leaf.values + items, new_index * sizeof(value_type));
      std::memmove(leaf.values + new_index + 1, leaf.values + index, (fp_size - index) * sizeof(value_type));
      leaf.values[new_index] = value;
    }

    update_internal(path, fp_min, leaf.keys[0]);
    fp_min = leaf.keys[0];
    fp_size = fp_size - items + 1;
    fp_prev_size = IQR_SIZE_THRESH;
    leaf.info->size = fp_size;
    fp_prev.info->size = IQR_SIZE_THRESH;
  }
#endif

  bool leaf_insert(node_t &leaf, uint16_t index, const key_type &key, const value_type &value) {
    if (index < leaf.info->size && leaf.keys[index] == key) {
      manager.mark_dirty(leaf.info->id);
      leaf.values[index] = value;
//      locks.unlock(leaf.info->id);
      mutexes[leaf.info->id].unlock();
      return true;
    }

    if (leaf.info->size >= node_t::leaf_capacity) {
      return false;
    }

    ctr_size++;
    manager.mark_dirty(leaf.info->id);
    std::memmove(leaf.keys + index + 1, leaf.keys + index, (leaf.info->size - index) * sizeof(key_type));
    std::memmove(leaf.values + index + 1, leaf.values + index, (leaf.info->size - index) * sizeof(value_type));
    leaf.keys[index] = key;
    leaf.values[index] = value;
    ++leaf.info->size;
#ifdef LOL_FAT
    if (leaf.info->id == fp_id) {
      ++fp_size;
    } else if (leaf.info->next_id == fp_id) {
      fp_prev_id = leaf.info->id;
      fp_prev_min = leaf.keys[0];
      fp_prev_size = leaf.info->size;
    }
#endif
//    locks.unlock(leaf.info->id);
    mutexes[leaf.info->id].unlock();
    return true;
  }

  void split_insert(node_t &leaf, uint16_t index, const path_t &path, const key_type &key, const value_type &value) {
    ctr_size++;
    uint16_t split_leaf_pos = SPLIT_LEAF_POS;
#ifdef LOL_FAT
#ifdef VARIABLE_SPLIT
    bool fp_move = false;
    if (leaf.info->id == fp_id) {
      if (fp_prev_id == INVALID_NODE_ID) {
        fp_move = true;
      } else if (fp_prev_size < IQR_SIZE_THRESH) {
#ifdef REDISTRIBUTE
        redistribute(path, leaf, index, key, value);
        return;
#else
        fp_move = true;
#endif
      } else {
        size_t max_distance = IKR::upper_bound(dist(fp_min, fp_prev_min), fp_prev_size, fp_size);
        uint16_t outlier_pos = leaf.value_slot2(fp_min + max_distance);
        if (outlier_pos <= SPLIT_LEAF_POS) {
          split_leaf_pos = outlier_pos;
        } else {
          split_leaf_pos = outlier_pos - 10 < SPLIT_LEAF_POS ? SPLIT_LEAF_POS : outlier_pos - 10;
          fp_move = true;
        }
        if (index < outlier_pos) {
          split_leaf_pos++;
        }
      }
    }
#endif
#endif
    node_id_t new_leaf_id = manager.allocate();
    node_t new_leaf(manager.open_block(new_leaf_id), LEAF);
    manager.mark_dirty(new_leaf_id);

//    assert(1 <= split_leaf_pos && split_leaf_pos <= node_t::leaf_capacity);
    leaf.info->next_id = new_leaf_id;
    leaf.info->size = split_leaf_pos;
    new_leaf.info->id = new_leaf_id;
    new_leaf.info->next_id = leaf.info->next_id;
    new_leaf.info->size = node_t::leaf_capacity + 1 - leaf.info->size;

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
    if (leaf.info->id == tail_id) {
      tail_id = new_leaf_id;
#ifdef TAIL_FAT
      fp_min = new_leaf.keys[0];
#endif
    }
#ifdef LOL_FAT
    if (leaf.info->id == fp_id) {
#ifndef VARIABLE_SPLIT
      bool fp_move =
          fp_id == head_id ||
          (fp_prev_size >= IQR_SIZE_THRESH &&
           dist(new_leaf.keys[0], fp_min) < IKR::upper_bound(dist(fp_min, fp_prev_min), fp_prev_size, leaf.info->size));
#endif
      if (fp_move) {
        fp_prev_min = fp_min;
        fp_prev_size = leaf.info->size;
        fp_prev_id = fp_id;
        fp_id = new_leaf_id;
        fp_min = new_leaf.keys[0];
        fp_size = new_leaf.info->size;
      } else {
        fp_max = new_leaf.keys[0];
        fp_size = leaf.info->size;
      }
    } else if (new_leaf.info->next_id == fp_id) {
      fp_prev_id = new_leaf_id;
      fp_prev_min = new_leaf.keys[0];
      fp_prev_size = new_leaf.info->size;
    }
#endif
//    locks.unlock(leaf.info->id);
    mutexes[leaf.info->id].unlock();
    internal_insert(path, new_leaf.keys[0], new_leaf_id);
  }

#ifdef LOL_FAT
  static std::size_t cmp(const key_type &max, const key_type &min) { return max - min; }
#endif

public:
  explicit bp_tree(BlockManager &m) : manager(m), mutexes(m.get_capacity()), root_id(m.allocate()), height(1)
#ifdef LOL_RESET
                                      , life(sqrt(node_t::leaf_capacity))
#endif
  {
    head_id = tail_id = m.allocate();
#ifdef FAST_PATH
    fp_min = {};
#endif
#ifdef LOL_FAT
    fp_id = tail_id;
    fp_max = {};
    dist = cmp;
    fp_prev_id = INVALID_NODE_ID;
    fp_prev_min = {};
    fp_prev_size = 0;
    fp_size = 0;
#endif
    node_t leaf(manager.open_block(head_id), LEAF);
    manager.mark_dirty(head_id);
    leaf.info->id = head_id;
    leaf.info->next_id = head_id;
    leaf.info->size = 0;

    node_t root(manager.open_block(root_id), INTERNAL);
    manager.mark_dirty(root_id);
    root.info->id = root_id;
    root.info->next_id = root_id;
    root.info->size = 0;
    root.children[0] = head_id;
  }

  void insert(const key_type &key, const value_type &value) {
    path_t path;
    uint16_t index;
    node_t leaf;
//    Locks<node_id_t> locks;
#ifdef FAST_PATH
#ifdef LOL_FAT
    key_type leaf_max{};
#endif
#ifdef TAIL_FAT
    const node_id_t &fp_id = tail_id;
#endif
    std::unique_lock fp_lock(fp_mutex);
//    std::cerr << std::setw(10) << key << std::setw(10) << (fp_id != head_id ? fp_min : 0) << std::setw(10) << (fp_id != tail_id ? fp_max : 0);
    if ((fp_id == head_id || fp_min <= key)
#ifdef LOL_FAT
        && (fp_id == tail_id || key < fp_max)
#endif
    ) {
//      std::cerr << " hit" << std::endl;
//      locks.lock(fp_id);
      mutexes[fp_id].lock();
      leaf.load(manager.open_block(fp_id));
//      assert(fp_id == leaf.info->id);
//      assert(leaf.info->type == bp_node_type::LEAF);
#ifdef LOL_RESET
      life.success();
#endif
      index = leaf.value_slot(key);
      if (leaf_insert(leaf, index, key, value)) {
        ++ctr_fast;
        return;
      }
      ++ctr_fast_fail;
      find_leaf_exclusive(leaf, path, key
#ifdef LOL_FAT
                          , leaf_max
#endif
                          , fp_id);
    } else {
//      std::cerr << " miss " << +life.fails << '/' << +life.threshold << std::endl;
#ifdef LOL_RESET
      bool reset = life.failure();
      if (!reset)
#endif
      {
        fp_lock.unlock();
      }
#endif
      find_leaf_exclusive(leaf, key
#ifdef LOL_FAT
                          , leaf_max
#endif
      );
#ifdef LOL_RESET
      if (reset) {
        if (fp_id != tail_id && leaf.keys[0] == fp_max) {
          fp_prev_id = fp_id;
          fp_prev_size = fp_size;
          fp_prev_min = fp_min;
        } else {
          fp_prev_id = INVALID_NODE_ID;
        }
        fp_id = leaf.info->id;
        fp_min = leaf.keys[0];
        fp_max = leaf_max;
        fp_size = leaf.info->size;
        life.reset();
      }
//      else
#endif
//      {
//        // fp->prev info exist
//        // fp_max is valid
//        // leaf is fp->next
//        // IQR has enough values
//        if (fp_prev_id != INVALID_NODE_ID &&
//            fp_id != tail_id &&
//            fp_max == leaf.keys[0] &&
//            fp_prev_size >= IQR_SIZE_THRESH && fp_size >= IQR_SIZE_THRESH &&
//            dist(fp_max, fp_min) < IKR::upper_bound(dist(fp_min, fp_prev_min), fp_prev_size, fp_size)) {
//          fp_prev_min = fp_min;
//          fp_prev_size = fp_size;
//          fp_prev_id = fp_id;
//          fp_id = leaf.info->id;
//          fp_min = fp_max;
//          fp_max = leaf_max;
//          fp_size = leaf.info->size;
//#ifdef LOL_RESET
//          life.reset();
//#endif
//        }
//      }
      index = leaf.value_slot(key);
      if (leaf_insert(leaf, index, key, value)) {
        return;
      }
//      locks.unlock(leaf.info->id);
      mutexes[leaf.info->id].unlock();
      find_leaf_exclusive(leaf, path, key
#ifdef LOL_FAT
                          , leaf_max
#endif
      );
#ifdef FAST_PATH
    }
#endif
    index = leaf.value_slot(key);
    if (leaf_insert(leaf, index, key, value)) {
      for (const auto &parent_id : path) {
//        locks.unlock(parent_id);
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
      if (leaf.info->id == tail_id) {
        break;
      }
      node_id_t next_id = leaf.info->next_id;
      mutexes[next_id].lock_shared();
      mutexes[leaf.info->id].unlock_shared();
      leaf.load(manager.open_block(next_id));
//      assert(next_id == leaf.info->id);
//      assert(leaf.info->type == bp_node_type::LEAF);
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
      if (leaf.info->id == tail_id) {
        break;
      }
      node_id_t next_id = leaf.info->next_id;
      mutexes[next_id].lock_shared();
      mutexes[leaf.info->id].unlock_shared();
      leaf.load(manager.open_block(next_id));
//      assert(next_id == leaf.info->id);
//      assert(leaf.info->type == bp_node_type::LEAF);
      ++loads;
    }
    mutexes[leaf.info->id].unlock_shared();
    return loads;
  }

  std::optional<value_type> get(const key_type &key) const {
    node_t leaf;
    find_leaf_shared(leaf, key);
    uint16_t index = leaf.value_slot(key);
    std::optional<value_type> res = index < leaf.info->size &&
                                    (leaf.keys[index] == key ? std::make_optional(leaf.values[index]) : std::nullopt);
    mutexes[leaf.info->id].unlock_shared();
    return res;
  }

  bool contains(const key_type &key) const { return get(key).has_value(); }
};

#endif
