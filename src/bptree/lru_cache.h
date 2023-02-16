#ifndef LRUCache_H
#define LRUCache_H

#include <cstdint>
#include <cstdlib>
#include <unordered_map>

struct Node {
    uint32_t id;
    const uint32_t pos;
    Node *prev, *next;

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
        if (end == nullptr) return nullptr;

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

    /**
     * Get a node from cache
     * @param id
     * @return position in internal memory
     */
    uint32_t get(uint32_t id) {
        auto it = node_hash.find(id);

        if (it == node_hash.end()) {
            return capacity;
        }

        list.moveToFront(it->second);

        return it->second->pos;
    }

    /**
     * Put a node in cache evicting the least recently used node if necessary
     * @param id
     * @param[out] pos
     * @param[out] evicted_id
     * @return true if an eviction occurred
     */
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

#endif
