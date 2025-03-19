#pragma once

#include <outlier_detector.h>

#include <chrono>
#include <cstring>
#include <limits>
#include <mutex>
#include <optional>
#include <ranges>
#include <shared_mutex>
#include <vector>

#include "../MemoryBlockManager.hpp"
#include "BTreeNode.hpp"
namespace ConcurrentQuITBTree {
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
    using path_t = std::vector<step>;

    static constexpr const char *name = "ConcurrentQuITBTree";
    static constexpr const bool concurrent = false;
    friend std::ostream &operator<<(std::ostream &os, const BTree &tree) {
        os << tree.ctr_size << ", " << tree.ctr_fast << ", "
           << tree.ctr_fast_fail << ", " << tree.ctr_reset << ", "
           << tree.ctr_sort;
        os << ", " << tree.find_leaf_slot_time << ", " << tree.move_in_leaf_time
           << ", " << tree.sort_time;
        return os;
    }

    using dist_f = std::size_t (*)(const key_type &, const key_type &);

    static constexpr uint16_t SPLIT_INTERNAL_POS =
        node_t::internal_capacity / 2;
    static constexpr uint16_t SPLIT_LEAF_POS = (node_t::leaf_capacity + 1) / 2;
    static constexpr uint16_t IQR_SIZE_THRESH = SPLIT_LEAF_POS;
    static constexpr node_id_t INVALID_NODE_ID = -1;

    dist_f dist;

    BlockManager &manager;
    mutable std::vector<std::shared_mutex> mutexes;
    const node_id_t root_id;
    node_id_t head_id;
    node_id_t tail_id;

    mutable std::shared_mutex fp_mutex;

    node_id_t fp_id;

    key_type fp_min;

    key_type fp_max;
    node_id_t fp_prev_id;
    key_type fp_prev_min;
    uint16_t fp_prev_size;
    uint16_t fp_size;

    uint8_t height;

    reset_stats life;

    bool fp_sorted;

    std::atomic<uint32_t> ctr_fast{};
    std::atomic<uint32_t> ctr_fast_fail{};
    std::atomic<uint32_t> ctr_reset{};
    std::atomic<uint32_t> ctr_sort{};
    mutable std::atomic<uint32_t> ctr_root_shared{};
    mutable uint32_t ctr_root_unique{};
    uint32_t ctr_root{};
    std::atomic<uint32_t> ctr_size;

    // timers for profiling
    long long find_leaf_slot_time = 0;
    long long move_in_leaf_time = 0;
    long long sort_time = 0;

    void create_new_root(const key_type &key, node_id_t right_node_id) {
        ++ctr_root;
        node_id_t left_node_id = manager.allocate();
        node_t root(manager.open_block(root_id));
        node_t left_node(manager.open_block(left_node_id));
        std::memcpy(left_node.info, root.info, 4096);
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
        ++ctr_root_shared;
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

    void find_leaf_exclusive(node_t &node, path_t &path, const key_type &key,
                             key_type &leaf_max) const {
        node_id_t node_id = root_id;

        mutexes[node_id].lock();
        ++ctr_root_unique;
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

            if (slot != node.info->size) {
                leaf_max = node.keys[slot];
            }

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

    void find_leaf_exclusive(node_t &node, const key_type &key,
                             key_type &leaf_max) const {
        node_id_t parent_id = root_id;

        mutexes[root_id].lock_shared();
        ++ctr_root_shared;
        uint8_t i = height;
        node.load(manager.open_block(parent_id));

        while (--i > 0) {
            const uint16_t slot = node.child_slot(key);

            if (slot != node.info->size) {
                leaf_max = node.keys[slot];
            }

            const node_id_t child_id = node.children[slot];

            mutexes[child_id].lock_shared();

            mutexes[parent_id].unlock_shared();
            node.load(manager.open_block(child_id));

            parent_id = child_id;
        }
        const uint16_t slot = node.child_slot(key);

        if (slot < node.info->size) {
            leaf_max = node.keys[slot];
        }

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
                std::memmove(node.keys + index + 1, node.keys + index,
                             (node.info->size - index) * sizeof(key_type));
                std::memmove(node.children + index + 2,
                             node.children + index + 1,
                             (node.info->size - index) * sizeof(uint32_t));
                node.keys[index] = key;
                node.children[index + 1] = child_id;
                ++node.info->size;

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
            child_id = new_node_id;
            if (node_id != root_id) {
                mutexes[node_id].unlock();
            }
        }
        create_new_root(key, child_id);

        mutexes[root_id].unlock();
    }

    bool leaf_insert(node_t &leaf, uint16_t index, const key_type &key,
                     const value_type &value, bool fast) {
        if (index < leaf.info->size && leaf.keys[index] == key) {
            manager.mark_dirty(leaf.info->id);
            leaf.values[index] = value;

            mutexes[leaf.info->id].unlock();
            return true;
        }

        if (leaf.info->size >= node_t::leaf_capacity) {
            return false;
        }

        ctr_size++;
        manager.mark_dirty(leaf.info->id);

        if (index < leaf.info->size) {
            std::chrono::high_resolution_clock::time_point start =
                std::chrono::high_resolution_clock::now();
            std::memmove(leaf.keys + index + 1, leaf.keys + index,
                         (leaf.info->size - index) * sizeof(key_type));
            std::memmove(leaf.values + index + 1, leaf.values + index,
                         (leaf.info->size - index) * sizeof(value_type));
            std::chrono::high_resolution_clock::time_point end =
                std::chrono::high_resolution_clock::now();
            move_in_leaf_time +=
                std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                     start)
                    .count();
        }
        leaf.keys[index] = key;
        leaf.values[index] = value;
        ++leaf.info->size;

        if (fast) {
            if (leaf.info->id == fp_id) {
                ++fp_size;
            } else if (leaf.info->next_id == fp_id) {
                fp_prev_id = leaf.info->id;
                fp_prev_min = leaf.keys[0];
                fp_prev_size = leaf.info->size;
            }
        }

        mutexes[leaf.info->id].unlock();
        return true;
    }

    void sort_leaf(node_t &leaf) {
        std::chrono::high_resolution_clock::time_point start =
            std::chrono::high_resolution_clock::now();
        // do an in-place sort of entries in the leaf node without making a copy
        std::sort(leaf.keys, leaf.keys + leaf.info->size,
                  [&leaf](const key_type &a, const key_type &b) {
                      // Sort keys and ensure corresponding values are
                      // swapped
                      size_t index_a = &a - leaf.keys;
                      size_t index_b = &b - leaf.keys;
                      if (a > b) {
                          std::swap(leaf.values[index_a], leaf.values[index_b]);
                      }
                      return a < b;
                  });
        std::chrono::high_resolution_clock::time_point end =
            std::chrono::high_resolution_clock::now();
        sort_time +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                .count();
    }

    void split_insert(node_t &leaf, uint16_t index, const path_t &path,
                      const key_type &key, const value_type &value, bool fast) {
        ctr_size++;
        uint16_t split_leaf_pos = SPLIT_LEAF_POS;

        bool fp_move = false;
        if (fast) {
            if (leaf.info->id == fp_id) {
                if (fp_prev_id == INVALID_NODE_ID ||
                    fp_prev_size < IQR_SIZE_THRESH) {
                    fp_move = true;
                } else {
                    size_t max_distance = IKR::upper_bound(
                        dist(fp_min, fp_prev_min), fp_prev_size, fp_size);
                    uint16_t outlier_pos =
                        leaf.value_slot2(fp_min + max_distance);
                    if (outlier_pos <= SPLIT_LEAF_POS) {
                        split_leaf_pos = outlier_pos;
                    } else {
                        split_leaf_pos = outlier_pos - 10 < SPLIT_LEAF_POS
                                             ? SPLIT_LEAF_POS
                                             : outlier_pos - 10;
                        fp_move = true;
                    }
                    if (index < outlier_pos) {
                        split_leaf_pos++;
                    }
                }
            }
        }

        node_id_t new_leaf_id = manager.allocate();
        node_t new_leaf(manager.open_block(new_leaf_id), LEAF);
        manager.mark_dirty(new_leaf_id);

        leaf.info->size = split_leaf_pos;
        new_leaf.info->id = new_leaf_id;
        new_leaf.info->next_id = leaf.info->next_id;
        new_leaf.info->size = node_t::leaf_capacity + 1 - leaf.info->size;
        leaf.info->next_id = new_leaf_id;

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

        if (fast) {
            if (leaf.info->id == fp_id) {
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
        }

        mutexes[leaf.info->id].unlock();
        internal_insert(path, new_leaf.keys[0], new_leaf_id);
    }

    static std::size_t cmp(const key_type &max, const key_type &min) {
        return max - min;
    }

   public:
    explicit BTree(BlockManager &m)
        : manager(m),
          mutexes(m.get_capacity()),
          root_id(m.allocate()),
          height(1),
          life(sqrt(node_t::leaf_capacity)) {
        head_id = tail_id = m.allocate();

        fp_min = {};

        fp_id = tail_id;
        fp_max = {};
        dist = cmp;
        fp_prev_id = INVALID_NODE_ID;
        fp_prev_min = {};
        fp_prev_size = 0;
        fp_size = 0;

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
        fp_sorted = true;
#ifdef LEAF_APPENDS
        std::cout << "leaf appends enabled" << std::endl;
#endif
    }

    bool update(const key_type &key, const value_type &value) {
        node_t leaf;
        key_type max;
        find_leaf_exclusive(leaf, key, max);
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

    void insert(const key_type &key, const value_type &value) {
        path_t path;
        uint16_t index;
        node_t leaf;

        bool fast;
        key_type leaf_max{};

        std::unique_lock fp_lock(fp_mutex);

        if ((fp_id == head_id || fp_min <= key)

            && (fp_id == tail_id || key < fp_max)

        ) {
            fast = true;

            mutexes[fp_id].lock();
            leaf.load(manager.open_block(fp_id));

            life.success();

            std::chrono::high_resolution_clock::time_point start =
                std::chrono::high_resolution_clock::now();
#ifdef LEAF_APPENDS
            index = leaf.info->size;
            // index = leaf.value_slot(key);
#else
            index = leaf.value_slot(key);
#endif
            std::chrono::high_resolution_clock::time_point end =
                std::chrono::high_resolution_clock::now();
            find_leaf_slot_time +=
                std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                     start)
                    .count();
            if (leaf_insert(leaf, index, key, value, true)) {
                ++ctr_fast;
                return;
            }
            ++ctr_fast_fail;
// sort the leaf as it is deemed full
#ifdef LEAF_APPENDS
            sort_leaf(leaf);
            ++ctr_sort;
            manager.mark_dirty(fp_id);
            fp_sorted = true;
#endif
            mutexes[fp_id].unlock();
            find_leaf_exclusive(leaf, path, key, leaf_max);
        } else {
            fast = false;

            bool reset = life.failure();
            if (!reset) {
                fp_lock.unlock();
            }

            find_leaf_exclusive(leaf, key, leaf_max);

            if (reset) {
                ++ctr_reset;
#ifdef LEAF_APPENDS
                // we need to lock the fast path
                mutexes[fp_id].lock();
                // sort the fast-path leaf node using sort_leaf()
                // if (!fp_sorted) {
                node_t fp_leaf(manager.open_block(fp_id));
                sort_leaf(fp_leaf);
                fp_sorted = true;
                ++ctr_sort;
                // mark the fast-path leaf node as dirty
                manager.mark_dirty(fp_id);
                // }
                // unlock the fast-path leaf node
                mutexes[fp_id].unlock();
#endif
                // now update associated metadata
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
                fast = true;
            }

            index = leaf.value_slot(key);
            if (leaf_insert(leaf, index, key, value, fast)) {
                return;
            }

            mutexes[leaf.info->id].unlock();
            find_leaf_exclusive(leaf, path, key, leaf_max);
        }
        index = leaf.value_slot(key);
        if (leaf_insert(leaf, index, key, value, fast)) {
            for (const auto &parent_id : path) {
                mutexes[parent_id].unlock();
            }
            return;
        }
        split_insert(leaf, index, path, key, value, fast);
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

            ++loads;
        }
        mutexes[leaf.info->id].unlock_shared();
        return loads;
    }

    std::optional<value_type> get(const key_type &key) const {
        node_t leaf;
        find_leaf_shared(leaf, key);
        std::shared_lock lock(mutexes[leaf.info->id], std::adopt_lock);
        uint16_t index = leaf.value_slot(key);
        return index < leaf.info->size &&
               (leaf.keys[index] == key ? std::make_optional(leaf.values[index])
                                        : std::nullopt);
    }

    bool contains(const key_type &key) const {
        node_t leaf;
        find_leaf_shared(leaf, key);
        std::shared_lock lock(mutexes[leaf.info->id], std::adopt_lock);
        if (leaf.info->id != fp_id) {
            uint16_t index = leaf.value_slot(key);
            return index < leaf.info->size && (leaf.keys[index] == key);
        } else {
            // do a linear scan of node
            for (uint16_t i = 0; i < leaf.info->size; i++) {
                if (leaf.keys[i] == key) {
                    return true;
                }
            }
            return false;
        }
    }
};
}  // namespace ConcurrentQuITBTree
