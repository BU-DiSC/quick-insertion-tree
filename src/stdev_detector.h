#ifndef STDEV_DETECTOR_H
#define STDEV_DETECTOR_H

#include <cmath>

#include "outlier_detector.h"

template<typename key_type, typename value_type>
class StdevDetector : public OutlierDetector<key_type, value_type> {
    // the number of elements to consider when calculating the standard deviation.
    size_t s0{};
    // the sum of the s0 elements
    key_type s1{};
    // the sum of the squares of the s0 elements
    key_type s2{};

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

    StdevDetector(double _num_stdev, int _k) {
        num_stdev = _num_stdev;
        k = _k;
        next_idx = 0;
        leaf_size = new size_t[k];
        sums_of_keys = new key_type[k];
        sums_of_squares = new key_type[k];
    }

    ~StdevDetector() {
        delete[] leaf_size;
        delete[] sums_of_keys;
        delete[] sums_of_squares;
    }

    bool is_outlier(const key_type &key) {
        if (prev_key == 0) {
            prev_key = key;
            return false;
        }
        auto x = key - prev_key;
        auto _s0 = s0 + 1;
        auto _s1 = static_cast<double>(s1) + x;
        auto mean = _s1 / _s0;
        auto _s2 = s2 + x * x;
        auto std_dev = sqrt((_s2 - _s1 * mean) / _s0);
        if (x > mean + num_stdev * std_dev) {
            return true;
        }
        prev_key = key;
        s0 = _s0;
        s1 = _s1;
        s2 = _s2;
        return false;
    }

    void update(BeTree<key_type, value_type> &tree) {
        if (k == 0)
            return;
        long long sum_of_keys = tree.getSumKeys();
        long long sum_of_squares = tree.getSumSquares();
        int size = tree.getPrevTailSize();
        // update function for last k nodes std outlier

        // remove the popped node from count, sum, and sum_square
        s0 -= leaf_size[next_idx];
        s1 -= sums_of_keys[next_idx];
        s2 -= sums_of_squares[next_idx];

        // update buffer
        leaf_size[next_idx] = size;
        sums_of_keys[next_idx] = sum_of_keys;
        sums_of_squares[next_idx] = sum_of_squares;

        // update the new node to count, sum, and sum_square
        s0 += size;
        s1 += sum_of_keys;
        s2 += sum_of_squares;

        // update the next_idx that indicates the next node to pop
        next_idx = (++next_idx) % k;
    }
};

#endif
