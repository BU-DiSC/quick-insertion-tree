#ifndef UTIL_H
#define UTIL_H

#include <cassert>
#include <iostream>
#include <set>

template <typename T>
class Locks {
  std::set<T> shared;
  std::set<T> exclusive;

public:
  ~Locks() {
    if (!shared.empty()) {
      std::cerr << "Shared:";
      for (const T &lock : shared) {
        std::cerr << ' ' << lock;
      }
      std::cerr << '\n';
    }
    if (!exclusive.empty()) {
      std::cerr << "Exclusive:";
      for (const T &lock : exclusive) {
        std::cerr << ' ' << lock;
      }
      std::cerr << '\n';
    }
    assert(shared.empty());
    assert(exclusive.empty());
  }

  void lock_shared(T lock) {
    assert(!shared.contains(lock));
    assert(!exclusive.contains(lock));
    shared.insert(lock);
  }

  void lock(T lock) {
    assert(!shared.contains(lock));
    assert(!exclusive.contains(lock));
    exclusive.insert(lock);
  }

  void unlock_shared(T lock) {
    assert(shared.contains(lock));
    assert(!exclusive.contains(lock));
    shared.erase(lock);
  }

  void unlock(T lock) {
    assert(!shared.contains(lock));
    assert(exclusive.contains(lock));
    exclusive.erase(lock);
  }
};

#endif
