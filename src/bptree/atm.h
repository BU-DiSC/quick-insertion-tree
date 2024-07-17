#ifndef TREE_ANALYSIS_ATM_H
#define TREE_ANALYSIS_ATM_H

#include <atomic>
#include <thread>
#include <limits>

namespace atm {
    class shared_mutex {
        using numeric_type = unsigned;
        std::atomic<numeric_type> state;
        static constexpr auto LOCKED = std::numeric_limits<numeric_type>::max();

    public:
        shared_mutex() : state(0) {}

        void lock_shared() {
            while (true) {
                numeric_type expected = state.load(std::memory_order_acquire);
                if (expected != LOCKED && state.compare_exchange_weak(expected, expected + 1, std::memory_order_acquire)) {
                    break;
                }
//                std::this_thread::yield();
            }
        }

        void unlock_shared() {
            state.fetch_sub(1, std::memory_order_release);
        }

        void lock() {
            while (true) {
                numeric_type expected = 0;
                if (state.compare_exchange_weak(expected, LOCKED, std::memory_order_acquire)) {
                    break;
                }
//                std::this_thread::yield();
            }
        }

        void unlock() {
            state.store(0, std::memory_order_release);
        }
    };
}

#endif
