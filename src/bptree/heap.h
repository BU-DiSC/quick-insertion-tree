#ifndef HEAP_H
#define HEAP_H

#include <iostream>
#include <queue>
#include <unordered_map>

template<typename key_type, typename value_type>
class Heap {
    std::unordered_map<key_type, value_type> map;
    std::priority_queue<key_type, std::vector<key_type>, std::greater<key_type>> pq;
public:
    const size_t max_size;

    explicit Heap(size_t max_size) : max_size(max_size) {
        map.reserve(max_size);
    }

    bool contains(const key_type &key) const {
        return map.find(key) != map.end();
    }

    void pop() {
        map.erase(pq.top());
        pq.pop();
    }

    std::pair<key_type, value_type> top() const {
        key_type key = pq.top();
        value_type value = map.at(key);
        return {key, value};
    }

    void push(key_type key, value_type value) {
        pq.push(key);
        map.insert({key, value});
    }

    size_t size() const {
        return map.size();
    }
};

#endif
