#ifndef STDEV_DETECTOR_H
#define STDEV_DETECTOR_H

#include "outlier_detector.h"

size_t lower_bound(size_t k) {
    size_t x = k;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    return k == x ? k : x >> 1;
}

template<typename key_type>
class StdevDetector : public OutlierDetector<key_type> {
    // the number of elements to consider when calculating the standard deviation.
    size_t s0;
    // the sum of the s0 elements
    key_type s1;
    // the sum of the squares of the s0 elements
    key_type s2;

    // Stdev of last k nodes
    size_t k;

    double num_stdev;

    // Next index to update sums and sumSqs buffer
    size_t next_idx;

    // A vector storing every leaf size in sorted tree
    size_t *leaf_size;

    // Sums of keys of each node
    key_type *sums_of_keys;

    // Sums of squared keys of each node
    key_type *sums_of_squares;

    key_type prev_key;
public:
    void init(const key_type &key) {
        prev_key = key;
    }

    StdevDetector(double num_stdev, int _k) : num_stdev(num_stdev) {
        s0 = s1 = s2 = next_idx = 0;
        if (_k != 0) {
            k = lower_bound(_k);
            leaf_size = new size_t[k + 1]{};
            sums_of_keys = new key_type[k + 1]{};
            sums_of_squares = new key_type[k + 1]{};
        } else {
            leaf_size = nullptr;
            sums_of_squares = nullptr;
            sums_of_keys = nullptr;
        }
    }

    ~StdevDetector() {
        delete[] leaf_size;
        delete[] sums_of_keys;
        delete[] sums_of_squares;
    }

    bool is_outlier(const key_type &key) {
        key_type x = key - prev_key;
        size_t _s0 = s0 + 1;
        key_type _s1 = s1 + x;
        double mean = _s1 / (double) _s0;
        key_type _s2 = s2 + x * x;
        double std_dev = sqrt((_s2 - _s1 * mean) / _s0);
        if (x > mean + num_stdev * std_dev) {
            return true;
        }
        prev_key = key;
        s0 = _s0;
        s1 = _s1;
        s2 = _s2;
        return false;
    }

    void update(const bp_stats &stats) {
        // update function for last k nodes std outlier
        if (k == 0) {
            return;
        }

        // remove the popped node from count, sum, and sum_square
        s0 -= leaf_size[next_idx];
        s1 -= sums_of_keys[next_idx];
        s2 -= sums_of_squares[next_idx];

        // update buffer
        leaf_size[next_idx] = stats.keys_count;
        sums_of_keys[next_idx] = stats.keys_sum;
        sums_of_squares[next_idx] = stats.key_squares_sum;

        // update the new node to count, sum, and sum_square
        s0 += stats.keys_count;
        s1 += stats.keys_sum;
        s2 += stats.key_squares_sum;

        // update the next_idx that indicates the next node to pop
        next_idx = (next_idx + 1) & k;
    }
};

#endif
