#ifndef BLOCK_MANAGER_H
#define BLOCK_MANAGER_H

#include <unistd.h>
#include <cassert>
#include "lru_cache.h"
#include <fstream>
#include <cstring>
#include <fcntl.h>

#define BLOCK_SIZE_BYTES 4096

// #ifdef PROFILE
// extern unsigned long openblock_time;
// extern unsigned long readblock_time;
// extern unsigned long writeblock_time;

// unsigned long openblock_time = 0;
// unsigned long readblock_time = 0;
// unsigned long writeblock_time = 0;
// #endif

class Block {
public:
    unsigned char block_buf[BLOCK_SIZE_BYTES];
};

class BlockManager {
public:
    std::string name;
    std::string root_dir;
    uint current_blocks;
    unsigned long long size_of_each_block;
    uint blocks_in_memory_cap;
    LRUCache *open_blocks;
    Block *internal_memory;

    // std::list<int> dirty_nodes;
    std::unordered_map<uint, uint> dirty_nodes;

    uint blocks_written;

    // counters
    unsigned long long num_reads, num_writes;

    // more counters
    unsigned long long leaf_cache_misses;
    unsigned long long internal_cache_misses;

    unsigned long long leaf_cache_hits;
    unsigned long long internal_cache_hits;

    unsigned long long total_cache_reqs;

#ifdef PROFILE

    unsigned long openblock_time = 0;
    unsigned long readblock_time = 0;
    unsigned long writeblock_time = 0;

    void getTimers(unsigned long &openb, unsigned long &readb, unsigned long &writeb)
    {
        openb = openblock_time;
        readb = readblock_time;
        writeb = writeblock_time;
        // deserializeb = deserialize_time;
    }
#endif

    std::string getBlockFileName(uint id) const {
        return root_dir + "/" + std::to_string(id);
    }

    std::string getParentFileName() const {
        return root_dir + "/" + name;
    }

    void writeBlock(uint id, uint pos) {
#ifdef PROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        if (pos >= blocks_in_memory_cap)
            return;

        std::string filename = getParentFileName();

        std::ofstream fout(filename.c_str(), std::ios::out | std::ios::in | std::ios::binary);

        unsigned long long beg = fout.tellp();

        // get position

        unsigned long long cor_pos = beg + ((id - 1) * size_of_each_block);

        fout.seekp(cor_pos, std::ofstream::beg);
        fout.write(reinterpret_cast<char *>(internal_memory[pos].block_buf), size_of_each_block);
        fout.flush();
        fout.close();

        num_writes++;
#ifdef PROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        writeblock_time += duration.count();
#endif
    }

    void writeBlock(uint id, uint pos, bool dirty) {
#ifdef PROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        if (pos >= blocks_in_memory_cap)
            return;

        if (dirty) {

            std::string filename = getParentFileName();

            int fout = open(filename.c_str(), O_WRONLY, 0600);

            assert(fout != -1);

            // get position
            unsigned long long cor_pos = ((id - 1) * size_of_each_block);

            pwrite(fout, internal_memory[pos].block_buf, size_of_each_block, cor_pos);
            close(fout);

            num_writes++;
        }
#ifdef PROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        writeblock_time += duration.count();
#endif
    }

    void readBlock(uint id, uint pos) {
#ifdef PROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        std::string filename = getParentFileName();
        // std::ifstream fin(filename.c_str(), std::ios::binary | std::ios::in);
        int fin = open(filename.c_str(), O_RDONLY);

        // get position
        unsigned long long cor_pos = ((id - 1) * size_of_each_block);

        pread(fin, internal_memory[pos].block_buf, size_of_each_block, cor_pos);

        close(fin);
        num_reads++;
#ifdef PROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        readblock_time += duration.count();
#endif
    }

public:
    BlockManager(const std::string &_name, const std::string &_root_dir,
                 unsigned long long _size_of_each_block, uint _blocks_in_memory_cap) :
            name(_name),
            root_dir(_root_dir),
            current_blocks(0),
            size_of_each_block(_size_of_each_block),
            blocks_in_memory_cap(_blocks_in_memory_cap),
            blocks_written(0),
            num_reads(0),
            num_writes(0),
            leaf_cache_misses(0),
            internal_cache_misses(0),
            leaf_cache_hits(0),
            internal_cache_hits(0),
            total_cache_reqs(0) {

        internal_memory = new Block[blocks_in_memory_cap];

        open_blocks = new LRUCache(blocks_in_memory_cap);

        // leaf_cache_hits = 0;
        // internal_cache_hits = 0;
        // total_cache_reqs = 0;

        std::string filename = getParentFileName();
        std::ofstream dummy_file(filename, std::ios::out | std::ios::binary);
        if (!dummy_file) {
            std::cout << "Error creating file: " << filename << std::endl;
        }
        dummy_file.flush();
        assert(dummy_file.good());
        dummy_file.close();
    }

    ~BlockManager() {
        // write all blocks back to disk
        for (const auto &[id, node] : open_blocks->node_hash) {
            writeBlock(node->id, node->pos, true);
        }

        delete[] internal_memory;
        delete open_blocks;
    }

    // allocates a file (dummy) in the backup directory on disk and returns an id
    // the id identifies the file, which would later be used as the node id
    uint allocate() {
        return ++current_blocks;
    }

    void deallocate(uint id) const {
        std::string filename = getBlockFileName(id);
        assert(unlink(filename.c_str()) == 0);
    }

    uint OpenBlock(uint id, bool &miss) {
#ifdef PROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
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
            // auto it = std::find(dirty_nodes.begin(), dirty_nodes.end(), evicted_id);
            auto it = dirty_nodes.find(evicted_id);
            if (it != dirty_nodes.end()) {
                // write block if dirty
                writeBlock(evicted_id, pos, true);
                // dirty_nodes.remove(evicted_id);
                dirty_nodes.erase(evicted_id);
            }
        }

        // read new block from disk into memory at pos
        memset(internal_memory[pos].block_buf, 0, sizeof(internal_memory[pos].block_buf));
        readBlock(id, pos);
#ifdef PROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        openblock_time += duration.count();
#endif

        return pos;
    }

    void setLeafCacheMisses(unsigned long long counter) {
        leaf_cache_misses = counter;
    }

    void setInternalCacheMisses(unsigned long long counter) {
        internal_cache_misses = counter;
    }

    void setLeafCacheHits(unsigned long long counter) {
        leaf_cache_hits = counter;
    }

    void setInternalCacheHits(unsigned long long counter) {
        internal_cache_hits = counter;
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

    unsigned long long getLeafCacheMisses() const { return leaf_cache_misses; }

    unsigned long long getInternalCacheMisses() const { return internal_cache_misses; }

    unsigned long long getLeafCacheHits() const { return leaf_cache_hits; }

    unsigned long long getInternalCacheHits() const { return internal_cache_hits; }

    unsigned long long getTotalCacheReqs() const { return total_cache_reqs; }

    uint getCurrentBlocks() const { return current_blocks; }

    void addDirtyNode(uint nodeId) {
        dirty_nodes.insert({nodeId, nodeId});
    }
};

#endif