#ifndef MEMORY_BLOCK_MANAGER_H
#define MEMORY_BLOCK_MANAGER_H

#include <cassert>
#include <iostream>

#ifndef BLOCK_SIZE_BYTES
#define BLOCK_SIZE_BYTES 4096
#endif

struct Block {
    uint8_t block_buf[BLOCK_SIZE_BYTES]{};
};

class InMemoryBlockManager {
    friend std::ostream &operator<<(std::ostream &os, const InMemoryBlockManager &manager) {
        os << ", ";
        return os;
    }

    const uint32_t capacity;
    uint32_t next_block_id;
    Block *internal_memory;
public:
    static constexpr uint32_t block_size = BLOCK_SIZE_BYTES;

    InMemoryBlockManager(const char *filepath, uint32_t capacity) :
            capacity(capacity),
            next_block_id(0) {
        std::cout << "IN MEMORY" << std::endl;
        internal_memory = new Block[capacity];
    }

    ~InMemoryBlockManager() {
        delete[] internal_memory;
        std::cout << next_block_id << std::endl;
    }

    void flush() {
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
    void mark_dirty(uint32_t id) {
    }

    void *open_block(uint32_t id) {
        return internal_memory[id].block_buf;
    }
};

#endif
