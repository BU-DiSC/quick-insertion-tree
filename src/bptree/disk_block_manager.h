#ifndef DISK_BLOCK_MANAGER_H
#define DISK_BLOCK_MANAGER_H

#include <cassert>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

struct Node {
    uint32_t id;
    const uint32_t pos;
    Node *prev, *next;

    Node(uint32_t id, uint32_t pos) : id(id), pos(pos) {
        prev = nullptr;
        next = nullptr;
    }
};

class LinkedList {
    Node *begin;
    Node *end;

public:
    LinkedList() {
        begin = nullptr;
        end = nullptr;
    }

    ~LinkedList() {
        while (end) {
            delete removeFromEnd();
        }
    }

    void moveToFront(Node *node) {
        if (node == begin) return;

        node->prev->next = node->next;

        if (node == end) {
            end = end->prev;
        } else {
            node->next->prev = node->prev;
        }

        // add to front
        begin->prev = node;
        node->next = begin;
        node->prev = nullptr;
        begin = node;
    }

    void addToFront(Node *node) {
        if (begin) {
            node->next = begin;
            begin->prev = node;
        } else {
            end = node;
        }
        begin = node;
    }

    Node *removeFromEnd() {
        Node *temp = end;
        if (end->prev) {
            end->prev->next = nullptr;
        } else {
            begin = nullptr;
        }
        end = end->prev;
        temp->prev = nullptr;
        return temp;
    }
};

class LRUCache {
    const uint32_t capacity;
    LinkedList list;
    std::unordered_map<uint32_t, Node *> node_hash{};

public:

    explicit LRUCache(uint32_t cap) : capacity(cap) {}

    /**
     * Get a node from cache
     * @param id
     * @return position in internal memory
     */
    std::pair<uint32_t, std::optional<uint32_t>> get(const uint32_t& key) {
        auto it = node_hash.find(key);
        if (it != node_hash.end()) {
            list.moveToFront(it->second);
            return {it->second->pos, std::nullopt};
        }

        auto value = node_hash.size();
        if (node_hash.size() < capacity) {
            auto node = new Node(key, value);
            list.addToFront(node);
            node_hash[key] = node;
            return {value, std::nullopt};
        } else {
            auto last = list.removeFromEnd();
            node_hash.erase(last->id);
            std::pair<uint32_t, uint32_t> res = {last->id, last->pos};
            last->id = key;
            list.addToFront(last);
            node_hash[key] = last;
            return res;
        }
    }

    bool contains(uint32_t id) {
        return node_hash.find(id) != node_hash.end();
    }
};

#ifndef BLOCK_SIZE_BYTES
#define BLOCK_SIZE_BYTES 4096
#endif

struct Block {
    uint8_t block_buf[BLOCK_SIZE_BYTES]{};
};

class DiskBlockManager {
    friend std::ostream &operator<<(std::ostream &os, const DiskBlockManager &manager) {
        os << manager.ctr_writes << ", " << manager.ctr_mark_dirty;
        return os;
    }

    const uint32_t capacity;
    uint32_t next_block_id;
    Block *internal_memory;
    LRUCache cache;
    int fd;
    std::unordered_set<uint32_t> dirty_nodes;
    uint32_t ctr_writes;
    uint32_t ctr_mark_dirty;

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
        ctr_writes++;
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

    DiskBlockManager(const char *filepath, uint32_t capacity) :
            capacity(capacity),
            next_block_id(0),
            cache(capacity),
            dirty_nodes(),
            ctr_writes(0),
            ctr_mark_dirty(0) {
        internal_memory = new Block[capacity];
        fd = open(filepath, O_RDWR | O_CREAT | O_TRUNC, 0600);
        assert(fd != -1);
    }

    ~DiskBlockManager() {
#ifndef BENCHMARK
        flush();
#endif
        delete[] internal_memory;
        close(fd);
    }

    void flush() {
        // write dirty blocks back to disk
        for (const auto &id: dirty_nodes) {
            auto [pos, evicted] = cache.get(id);
            assert(!evicted.has_value());
            write_block(id, pos);
        }
        dirty_nodes.clear();
    }

    void reset() {
        std::cerr << "size: " << next_block_id << std::endl;
        next_block_id = 0;
        cache.~LRUCache();
        new(&cache) LRUCache(capacity);
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
//        assert(cache.contains(id));
        dirty_nodes.insert(id);
        ctr_mark_dirty++;
    }

    /**
     * Open a block (if not already in memory)
     * @param id block id
     * @return position of block in memory
     */
    void *open_block(uint32_t id) {
        auto [pos, evicted] = cache.get(id);
        if (evicted.has_value()) {
            auto evicted_id = evicted.value();

            // write old block back to disk
            if (dirty_nodes.find(evicted_id) != dirty_nodes.end()) {
                // write block if dirty
                write_block(evicted_id, pos);
                dirty_nodes.erase(evicted_id);
                std::cerr << "eviction" << std::endl;
            }

            read_block(id, pos);
        }
        return internal_memory[pos].block_buf;
    }
};

#endif
