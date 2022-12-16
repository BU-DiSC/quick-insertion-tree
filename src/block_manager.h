#ifndef BLOCK_MANAGER_H
#define BLOCK_MANAGER_H

#include <unistd.h>
#include <cassert>
#include <fstream>
#include <cstring>
#include <fcntl.h>
#include "lru_cache.h"

#define BLOCK_SIZE_BYTES 4096

struct Block {
    unsigned char block_buf[BLOCK_SIZE_BYTES];
};

class BlockManager {
    std::string name;
    std::string root_dir;
    uint current_blocks;
    unsigned long long size_of_each_block;
    uint blocks_in_memory_cap;
    LRUCache *open_blocks;
    int fd;

    std::unordered_map<uint, uint> dirty_nodes;

    // more counters
    unsigned long long leaf_cache_misses;
    unsigned long long internal_cache_misses;

    unsigned long long leaf_cache_hits;
    unsigned long long internal_cache_hits;

    unsigned long long total_cache_reqs;

    std::string getBlockFileName(uint id) const {
        return root_dir + "/" + std::to_string(id);
    }

    std::string getParentFileName() const {
        return root_dir + "/" + name;
    }

    void writeBlock(uint id, uint pos) {
        if (pos >= blocks_in_memory_cap)
            return;

        // get position
        off_t cor_pos = (id - 1) * size_of_each_block;

        pwrite(fd, internal_memory[pos].block_buf, size_of_each_block, cor_pos);

        num_writes++;
    }

    void readBlock(uint id, uint pos) {
        // get position
        off_t cor_pos = (id - 1) * size_of_each_block;

        pread(fd, internal_memory[pos].block_buf, size_of_each_block, cor_pos);
    }

public:
    Block *internal_memory;
    unsigned long long num_writes;

    BlockManager(const std::string &_name, const std::string &_root_dir,
                 unsigned long long _size_of_each_block, uint _blocks_in_memory_cap) :
            name(_name),
            root_dir(_root_dir),
            current_blocks(0),
            size_of_each_block(_size_of_each_block),
            blocks_in_memory_cap(_blocks_in_memory_cap),
            num_writes(0),
            leaf_cache_misses(0),
            internal_cache_misses(0),
            leaf_cache_hits(0),
            internal_cache_hits(0),
            total_cache_reqs(0) {

        internal_memory = new Block[blocks_in_memory_cap];

        open_blocks = new LRUCache(blocks_in_memory_cap);

        std::string filename = getParentFileName();
        fd = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
        assert(fd != -1);
    }

    ~BlockManager() {
        // write all blocks back to disk
        for (const auto &[id, node]: open_blocks->node_hash) {
            writeBlock(node->id, node->pos);
        }

        delete[] internal_memory;
        delete open_blocks;
        close(fd);
    }

    // allocates a file (dummy) in the backup directory on disk and returns an id
    // the id identifies the file, which would later be used as the node id
    uint allocate() {
        return ++current_blocks;
    }

    void addDirtyNode(uint nodeId) {
        dirty_nodes.insert({nodeId, nodeId});
    }

    uint OpenBlock(uint id, bool &miss) {
        uint pos = open_blocks->get(id);

        total_cache_reqs++;

        if (pos < blocks_in_memory_cap) {
            // block is already open in memory
            miss = false;
            return pos;
        }

        miss = true;
        uint evicted_id;
        pos = open_blocks->put(id, &evicted_id);

        // write old block back to disk
        if (evicted_id > 0) {
            // find if evicted id is in dirty nodes
            auto it = dirty_nodes.find(evicted_id);
            if (it != dirty_nodes.end()) {
                // write block if dirty
                writeBlock(evicted_id, pos);
                dirty_nodes.erase(evicted_id);
            }
        }

        // read new block from disk into memory at pos
        memset(internal_memory[pos].block_buf, 0, sizeof(internal_memory[pos].block_buf));
        readBlock(id, pos);
        return pos;
    }

    void addLeafCacheMisses() {
        leaf_cache_misses++;
    }

    void addInternalCacheMisses() {
        internal_cache_misses++;
    }

    void addLeafCacheHits() {
        leaf_cache_hits++;
    }

    void addInternalCacheHits() {
        internal_cache_hits++;
    }
};

#endif