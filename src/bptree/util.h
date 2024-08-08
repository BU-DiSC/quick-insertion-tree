#ifndef UTIL_H
#define UTIL_H

#include <iostream>
#include <set>
#include <cassert>

class Locks {
    using node_id_t = uint32_t;
    using T = node_id_t;
    std::set<T> shared;
    std::set<T> exclusive;

public:
    ~Locks() {
        if (!shared.empty()) {
            std::cerr << "Shared: ";
            for (const T &lock: shared) {
                std::cerr << lock << ' ';
            }
            std::cerr << "\n";
        }
        if (!exclusive.empty()) {
            std::cerr << "Exclusive: ";
            for (const T &lock: exclusive) {
                std::cerr << lock << ' ';
            }
            std::cerr << '\n';
        }
        assert(shared.empty() && exclusive.empty());
    }

    void lock_shared(T lock) {
        assert(shared.find(lock) == shared.end());
        assert(exclusive.find(lock) == exclusive.end());
        shared.insert(lock);
    }

    void lock(T lock) {
        assert(shared.find(lock) == shared.end());
        assert(exclusive.find(lock) == exclusive.end());
        exclusive.insert(lock);
    }

    void unlock_shared(T lock) {
        assert(exclusive.find(lock) == exclusive.end());
        assert(shared.find(lock) != shared.end());
        shared.erase(lock);
    }

    void unlock(T lock) {
        assert(exclusive.find(lock) != exclusive.end());
        assert(shared.find(lock) == shared.end());
        exclusive.erase(lock);
    }
};

#endif
