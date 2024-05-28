#ifndef MEMORY_BLOCK_MANAGER_H
#define MEMORY_BLOCK_MANAGER_H

#include <cassert>
#include <cstring>
#include <atomic>
#include <iostream>
#ifndef BLOCK_SIZE_BYTES
#define BLOCK_SIZE_BYTES 4096
#endif
#include <cstdint>

struct Block {
    uint8_t block_buf[BLOCK_SIZE_BYTES]{};
};

class InMemoryBlockManager {
    friend std::ostream &operator<<(std::ostream &os,
                                    const InMemoryBlockManager &manager) {
        os << ", ";
        return os;
    }

    const uint32_t capacity;
    std::atomic<uint32_t> next_block_id;
    Block *internal_memory;

   public:
    static constexpr size_t block_size = BLOCK_SIZE_BYTES;

    InMemoryBlockManager(const char *filepath, const uint32_t capacity)
        : capacity(capacity), next_block_id(0) {
        std::cerr << "IN MEMORY (" << capacity << ')' << std::endl;
        internal_memory = new Block[capacity];
    }

    ~InMemoryBlockManager() { delete[] internal_memory; }

    void reset() {
        next_block_id = 0;
    }

    /**
     * Allocate a block id
     * @return block id for the new block
     */
    uint32_t allocate() {
        return next_block_id++;
    }

    /**
     * Mark a block as dirty
     * @param id block id
     */
    void mark_dirty(uint32_t id) {}

    [[nodiscard]] void *open_block(const uint32_t id) const {
        return internal_memory[id].block_buf;
    }

    [[nodiscard]]
    uint32_t get_capacity() const
    {
        return capacity;
    }
};

#endif
