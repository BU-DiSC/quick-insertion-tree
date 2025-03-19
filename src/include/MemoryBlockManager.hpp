#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <vector>

#ifndef BLOCK_SIZE_BYTES
#define BLOCK_SIZE_BYTES 4096
#endif

template <typename node_id_t>
class InMemoryBlockManager {
   public:
    static constexpr size_t block_size = BLOCK_SIZE_BYTES;

    explicit InMemoryBlockManager(const uint32_t cap) : internal_memory(cap) {}

    void reset() { next_block_id = 0; }

    node_id_t allocate() {
        return next_block_id.fetch_add(1, std::memory_order_acq_rel);
    }

    void mark_dirty(node_id_t) {}

    void *open_block(const node_id_t id) { return internal_memory[id].data(); }

    node_id_t get_capacity() const { return internal_memory.size(); }

   private:
    using Block = std::array<uint8_t, block_size>;
    std::vector<Block> internal_memory;
    std::atomic<node_id_t> next_block_id{};
};
