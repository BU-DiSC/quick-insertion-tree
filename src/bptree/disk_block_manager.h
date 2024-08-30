#ifndef DISK_BLOCK_MANAGER_H
#define DISK_BLOCK_MANAGER_H

#include <atomic>
#include <cassert>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

struct Node {
  uint32_t id;
  const uint32_t pos;
  Node *prev;
  Node *next;

  Node(uint32_t _id, uint32_t _pos) : id(_id), pos(_pos) {
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
    if (node == begin)
      return;

    node->prev->next = node->next;

    if (node == end) {
      end = end->prev;
    } else {
      node->next->prev = node->prev;
    }

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
    if (end == nullptr)
      return nullptr;

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
  uint32_t size;
  LinkedList list;
  std::unordered_map<uint32_t, Node *> node_hash{};

public:
  explicit LRUCache(uint32_t cap) : capacity(cap), size(0) {}

  uint32_t get(uint32_t id) {
    auto it = node_hash.find(id);

    if (it == node_hash.end()) {
      return capacity;
    }

    list.moveToFront(it->second);

    return it->second->pos;
  }

  bool put(uint32_t id, uint32_t &pos, uint32_t &evicted_id) {
    Node *node;
    bool eviction = size == capacity;
    if (eviction) {
      node = list.removeFromEnd();
      evicted_id = node->id;
      node_hash.erase(evicted_id);
      node->id = id;
    } else {
      node = new Node(id, size);
      size++;
    }
    pos = node->pos;
    list.addToFront(node);
    node_hash[id] = node;

    return eviction;
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
  std::atomic<uint32_t> next_block_id;
  Block *internal_memory;
  LRUCache cache;
  int fd;
  std::unordered_set<uint32_t> dirty_nodes;
  uint32_t ctr_writes;
  uint32_t ctr_mark_dirty;

  void write_block(uint32_t id, uint32_t pos) {
    assert(pos < capacity);
    off_t offset = id * block_size;
    pwrite(fd, internal_memory[pos].block_buf, block_size, offset);
    ctr_writes++;
  }

  void read_block(uint32_t id, uint32_t pos) {
    off_t offset = id * block_size;
    pread(fd, internal_memory[pos].block_buf, block_size, offset);
  }

public:
  static constexpr uint32_t block_size = BLOCK_SIZE_BYTES;

  DiskBlockManager(const char *filepath, uint32_t capacity) : capacity(capacity), next_block_id(0), cache(capacity), dirty_nodes(), ctr_writes(0), ctr_mark_dirty(0) {
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
    for (const auto &id : dirty_nodes) {
      uint32_t pos = cache.get(id);
      write_block(id, pos);
    }
    dirty_nodes.clear();
  }

  uint32_t allocate() {
    return next_block_id++;
  }

  void mark_dirty(uint32_t id) {
    assert(cache.get(id) != capacity);
    dirty_nodes.insert(id);
    ctr_mark_dirty++;
  }

  void *open_block(uint32_t id) {
    uint32_t pos = cache.get(id);
    if (pos == capacity) {
      uint32_t evicted_id;
      bool eviction = cache.put(id, pos, evicted_id);

      if (eviction && dirty_nodes.find(evicted_id) != dirty_nodes.end()) {
        write_block(evicted_id, pos);
        dirty_nodes.erase(evicted_id);
        std::cerr << "eviction" << std::endl;
      }

      read_block(id, pos);
    }
    return internal_memory[pos].block_buf;
  }

  uint32_t get_capacity() const {
    return capacity;
  }
};

#endif
