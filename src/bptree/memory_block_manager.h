#ifndef MEMORY_BLOCK_MANAGER_H
#define MEMORY_BLOCK_MANAGER_H

#include <cassert>
#include <cstring>
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
    uint32_t next_block_id;
    Block *internal_memory;

   public:
    static constexpr uint32_t block_size = BLOCK_SIZE_BYTES;

    InMemoryBlockManager(const char *filepath, const uint32_t capacity)
        : capacity(capacity) {
        std::cerr << "IN MEMORY" << std::endl;
        next_block_id = 0;
        internal_memory = new Block[capacity];
    }

    ~InMemoryBlockManager() { delete[] internal_memory; }

    void reset() {
        memset(internal_memory, 0, (size_t)next_block_id * block_size);
        next_block_id = 0;
    }

    /**
     * Allocate a block id
     * @return block id for the new block
     */
    uint32_t allocate() {
        assert(next_block_id < capacity);
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
};

#endif
