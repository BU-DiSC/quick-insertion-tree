#ifndef BLOCK_MANAGER_H
#define BLOCK_MANAGER_H

#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <unistd.h>
#include <unordered_set>

#include "lru_cache.h"

#define BLOCK_SIZE_BYTES 4096

struct Block {
    uint8_t block_buf[BLOCK_SIZE_BYTES]{};
};

class BlockManager {
    const uint32_t capacity;
    uint32_t next_block_id;
    Block *internal_memory;
    LRUCache cache;
    int fd;
    std::unordered_set<uint32_t> dirty_nodes;
    uint32_t num_writes;
    uint32_t num_mark_dirty;

    /**
     * Write a block to disk
     * @param id block id (offset in file)
     * @param pos position in internal memory
     */
    void write_block(uint32_t id, uint32_t pos) {
        assert(pos < capacity);
        off_t offset = id * block_size;
//        assert(pwrite(fd, internal_memory[pos].block_buf, block_size, offset) == block_size);
        pwrite(fd, internal_memory[pos].block_buf, block_size, offset);
        num_writes++;
    }

    /**
     * Read a block from disk
     * @param id block id (offset in file)
     * @param pos position in internal memory
     */
    void read_block(uint32_t id, uint32_t pos) {
        off_t offset = id * block_size;
//        assert(pread(fd, internal_memory[pos].block_buf, block_size, offset) == block_size);
        pread(fd, internal_memory[pos].block_buf, block_size, offset);
    }

public:
    static constexpr uint32_t block_size = BLOCK_SIZE_BYTES;

    BlockManager(const char *filepath, uint32_t capacity) :
            capacity(capacity),
            next_block_id(0),
            cache(capacity),
            dirty_nodes(),
            num_writes(0) {
        internal_memory = new Block[capacity];
        fd = open(filepath, O_RDWR | O_CREAT | O_TRUNC, 0600);
        assert(fd != -1);
    }

    ~BlockManager() {
#ifndef BENCHMARK
        flush();
#endif
        delete[] internal_memory;
        close(fd);
    }

    void flush() {
        // write dirty blocks back to disk
        for (const auto &id: dirty_nodes) {
            uint32_t pos = cache.get(id);
            write_block(id, pos);
        }
        dirty_nodes.clear();
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
    void mark_dirty(uint32_t id) {
        assert(cache.get(id) != capacity);
        dirty_nodes.insert(id);
        num_mark_dirty++;
    }

    /**
     * Open a block (if not already in memory)
     * @param id block id
     * @return position of block in memory
     */
    void *open_block(uint32_t id) {
        uint32_t pos = cache.get(id);
        if (pos == capacity) {
            uint32_t evicted_id;
            bool eviction = cache.put(id, pos, evicted_id);

            // write old block back to disk
            if (eviction && dirty_nodes.find(evicted_id) != dirty_nodes.end()) {
                // write block if dirty
                write_block(evicted_id, pos);
                dirty_nodes.erase(evicted_id);
                std::cerr << "eviction" << std::endl;
            }

            read_block(id, pos);
        }
        return internal_memory[pos].block_buf;
    }

    uint32_t getNumWrites() const {
        return num_writes;
    }

    uint32_t getMarkDirty() const {
        return num_mark_dirty;
    }
};

#undef BLOCK_SIZE_BYTES

#endif
