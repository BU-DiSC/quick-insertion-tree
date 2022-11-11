#ifndef LRUCache_H
#define LRUCache_H

//#include <unordered_map>

struct Node {
    uint id;
    const uint pos;
    Node *prev, *next;

    Node(uint _id, uint _pos) : id(_id), pos(_pos) {
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
    // denotes capacity of the cache
    uint capacity;

    // denotes current size of cache
    uint size;

    // elements of the cache
    LinkedList list;

public:
    std::unordered_map<uint, Node *> node_hash;

    LRUCache(uint _cap) : capacity(_cap), size(0) {}

    uint get(uint id) {
        auto it = node_hash.find(id);

        if (it == node_hash.end()) {
            return capacity + 1;
        }

        list.moveToFront(it->second);

        return it->second->pos;
    }

    uint put(uint id, uint *evicted_id) {
        Node *node;
        if (size == capacity) {
            node = list.removeFromEnd();
            *evicted_id = node->id;
            node_hash.erase(node->id);
            node->id = id;
        } else {
            *evicted_id = 0;
            node = new Node(id, size);
            size++;
        }
        list.addToFront(node);
        node_hash[id] = node;

        return node->pos;
    }

    std::unordered_map<uint, Node *>::iterator getBegin() {
        return node_hash.begin();
    }

    std::unordered_map<uint, Node *>::iterator getEnd() {
        return node_hash.end();
    }
};

#endif