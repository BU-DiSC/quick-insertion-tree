#ifndef MEMORY_BLOCK_MANAGER_H
#define MEMORY_BLOCK_MANAGER_H

#include <atomic>
#include <cassert>
#include <cstring>
#include <iostream>

#ifndef BLOCK_SIZE_BYTES
#define BLOCK_SIZE_BYTES 4096
#endif

#include <cstdint>

using Block = std::array<uint8_t, BLOCK_SIZE_BYTES>;

class InMemoryBlockManager {
  friend std::ostream &operator<<(std::ostream &os, const InMemoryBlockManager &) {
    os << ", ";
    return os;
  }

  std::vector<Block> internal_memory;
  std::atomic<uint32_t> next_block_id;

public:
  static constexpr size_t block_size = BLOCK_SIZE_BYTES;

  InMemoryBlockManager(const char *, const uint32_t cap) : internal_memory(cap), next_block_id(0) {}

  void reset() {
    next_block_id = 0;
  }
  uint32_t allocate() {
    return next_block_id++;
  }

  void mark_dirty(uint32_t) {}

  [[nodiscard]] void *open_block(const uint32_t id) {
    return internal_memory[id].data();
  }

  [[nodiscard]] uint32_t get_capacity() const {
    return internal_memory.size();
  }
};

#endif
