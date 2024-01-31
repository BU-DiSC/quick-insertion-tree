#ifndef LRUCache_H
#define LRUCache_H

#include <cstdint>
#include <unordered_map>
#include <vector>
#include <bits/stdc++.h>

class Element
{
    // node id
    uint id;

    // position in memory blocks and not cache
    uint pos;

public:
    Element(uint _id, uint _pos) : id(_id), pos(_pos) {}

    int getId() { return id; }
    int getPos() { return pos; }
};

class Node
{
public:
    uint id;
    uint pos;
    Node *prev, *next;

    Node(uint _id, uint _pos) : id(_id), pos(_pos)
    {
        prev = nullptr;
        next = nullptr;
    }

    int getId() { return id; }
    int getPos() { return pos; }
};

class LinkedList
{
    Node *begin;
    Node *end;

public:
    LinkedList()
    {
        begin = new Node(-1, -1);
        end = new Node(-1, -1);

        begin->next = end;
        end->prev = begin;
    }

    ~LinkedList()
    {
        delete begin;
        delete end;
    }

    void moveToFront(Node *node)
    {
        if (node->prev)
        {
            node->prev->next = node->next;
        }

        if (node->next)
        {
            node->next->prev = node->prev;
        }

        // add to front
        node->next = begin->next;
        node->prev = begin;
        begin->next->prev = node;
        begin->next = node;
    }

    Node *addToFront(uint id, uint pos)
    {
        Node *node = new Node(id, pos);
        moveToFront(node);

        return node;
    }

    void removeFromEnd()
    {
        if (end->prev == begin)
        {
            return;
        }

        Node *temp = end->prev;
        end->prev->prev->next = end;
        end->prev = end->prev->prev;

        delete temp;
    }

    Node *getEndNode()
    {
        if (end->prev == begin)
        {
            return nullptr;
        }

        return end->prev;
    }
};

class LRUCache
{
    // denotes capacity of the cache
    uint capacity;

    // denotes current size of cache
    uint size;

    // elements of the cache
    LinkedList *list;

    std::unordered_map<uint, Node *> node_hash;

public:
    LRUCache(uint _cap) : capacity(_cap), size(0)
    {
        list = new LinkedList();
    }

    ~LRUCache()
    {
        for (std::unordered_map<uint, Node *>::iterator it = node_hash.begin(); it != node_hash.end(); ++it)
        {
            delete it->second;
        }

        delete list;
    }

    uint get(uint id)
    {
        std::unordered_map<uint, Node *>::iterator it = node_hash.find(id);

        if (it == node_hash.end())
        {
            return capacity + 1;
        }

        list->moveToFront(it->second);

        return it->second->pos;
    }

    uint put(uint id, uint *evicted_id)
    {
        uint pos = get(id);

        if (pos >= capacity)
        {
            if (size == capacity)
            {
                Node *evicted = list->getEndNode();
                pos = evicted->pos;
                if (evicted_id)
                {
                    *evicted_id = evicted->id;
                }
                node_hash.erase(evicted->id);
                list->removeFromEnd();
                --size;
            }
            else
            {
                *evicted_id = 0;
                pos = size;
            }

            Node *node = list->addToFront(id, pos);
            ++size;
            node_hash[id] = node;
        }

        return pos;
    }

    std::unordered_map<uint, Node *>::iterator getBegin()
    {
        return node_hash.begin();
    }

    std::unordered_map<uint, Node *>::iterator getEnd()
    {
        return node_hash.end();
    }
};

#endif