#ifndef MTX_H
#define MTX_H

#include <atomic>
#include <cassert>
#include <pthread.h>
#include <mutex>
#include <thread>
#include <limits>
#include <shared_mutex>

namespace atm {
    class shared_mutex {
        using numeric_type = uint32_t;
        std::atomic<numeric_type> state{0};
        static constexpr auto LOCKED = std::numeric_limits<numeric_type>::max();

    public:
        void lock_shared() {
            while (true) {
                numeric_type expected = state.load(std::memory_order_acquire);
                if (expected != LOCKED &&
                    state.compare_exchange_weak(expected, expected + 1, std::memory_order_acquire)) {
                    break;
                }
                std::this_thread::yield();
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

namespace srv {
    class shared_mutex {
        using numeric_type = unsigned;
        std::atomic<numeric_type> active_reads{0};
        std::atomic<numeric_type> pending_writes{0};
        static constexpr auto LOCKED = std::numeric_limits<numeric_type>::max();
    public:
        void lock_shared() {
            while (true) {
                numeric_type expected = active_reads.load(std::memory_order_acquire);
                if (expected != LOCKED && pending_writes.load(std::memory_order_acquire) == 0 &&
                    active_reads.compare_exchange_weak(expected, expected + 1, std::memory_order_acquire)) {
                    break;
                }
//                std::this_thread::yield();
            }
        }

        void unlock_shared() {
            active_reads.fetch_sub(1, std::memory_order_release);
        }

        void lock() {
            pending_writes.fetch_add(1, std::memory_order_acquire);
            while (true) {
                numeric_type expected = 0;
                if (active_reads.compare_exchange_weak(expected, LOCKED, std::memory_order_acquire)) {
                    break;
                }
//                std::this_thread::yield();
            }
            pending_writes.fetch_sub(1, std::memory_order_release);
        }

        void unlock() {
            active_reads.store(0, std::memory_order_release);
        }
    };
}

namespace flg {
    class shared_mutex {
        std::atomic_flag _lock = ATOMIC_FLAG_INIT;

    public:
        void lock_shared() {
            while (_lock.test_and_set(std::memory_order_acquire))
                while (_lock.test(std::memory_order_relaxed));
        }

        void unlock_shared() {
            _lock.clear(std::memory_order_release);
        }

        void lock() {
            while (_lock.test_and_set(std::memory_order_acquire))
                while (_lock.test(std::memory_order_relaxed));
        }

        void unlock() {
            _lock.clear(std::memory_order_release);
        }
    };
}

namespace spn {
    class shared_mutex {
        pthread_spinlock_t lk;
    public:
        shared_mutex() {
            pthread_spin_init(&lk, PTHREAD_PROCESS_PRIVATE);
        }

        ~shared_mutex() {
            pthread_spin_destroy(&lk);
        }

        void lock() {
            while (pthread_spin_lock(&lk));
        }

        void unlock() {
            pthread_spin_unlock(&lk);
        }

        void lock_shared() {
            while (pthread_spin_lock(&lk));
        }

        void unlock_shared() {
            pthread_spin_unlock(&lk);
        }
    };
}

namespace rwl {
    class shared_mutex {
        pthread_rwlock_t lk = PTHREAD_RWLOCK_INITIALIZER;
    public:
        ~shared_mutex() {
            pthread_rwlock_destroy(&lk);
        }

        void lock() {
            pthread_rwlock_wrlock(&lk);
        }

        void unlock() {
            pthread_rwlock_unlock(&lk);
        }

        void lock_shared() {
            pthread_rwlock_rdlock(&lk);
        }

        void unlock_shared() {
            pthread_rwlock_unlock(&lk);
        }
    };
}

namespace mtx {
    class shared_mutex {
        std::mutex mtx;
    public:
        void lock() {
            mtx.lock();
        }

        void unlock() {
            mtx.unlock();
        }

        void lock_shared() {
            mtx.lock();
        }

        void unlock_shared() {
            mtx.unlock();
        }
    };
}

#endif
