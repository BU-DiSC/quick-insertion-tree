#ifndef STDEV_DETECTOR_H
#define STDEV_DETECTOR_H

#include <vector>
#include <iostream>
#include <math.h>
#include "outlier_detector.h"

template<typename key_type>
class StdevDetector : public OutlierDetector<key_type>
{
    long long num_stdev;
    long long n;
    long long sum;
    long long sumSq;

    // Stdev of last k nodes 
    int k;

    // Next index to update sums and sumSqs buffer
    int next_idx;

    // Current number of nodes
    int num_of_nodes;

    // A vector storing every leaf size in sorted tree
    std::vector<int> leaf_size;

    // Sums of keys of each node
    std::vector<long long> sums_of_keys;

    // Sums of squared keys of each node
    std::vector<long long> sums_of_squares;

    public:

    StdevDetector(float _num_stdev, int _k)
    {
        num_stdev = _num_stdev;
        k = _k;
        n = 0;
        sum = 0;
        sumSq = 0;

        if (k > 0) {
            next_idx = 0;
            leaf_size.resize(k);
            sums_of_keys.resize(k);
            sums_of_squares.resize(k);
        }
    }

    void updateStdev(key_type key){
        n++;
        sum += (long long)key;
        sumSq += (long long)key * (long long)key;
    }

    bool is_outlier(key_type key, int tree_size) {
        if (n < 30) {
            if (k <= 0) updateStdev(key);
            return false;
        }
        long long stdev = sqrt(sumSq / (n - 1) - sum / n * sum / (n - 1));
        if (key <= sum / n + num_stdev * stdev) {
            if (k <= 0) updateStdev(key);
            return false;
        }
        return true;
    }

    void update(int size, long long* stats) {
        n -= leaf_size[next_idx];
        sum -= sums_of_keys[next_idx];
        sumSq -= sums_of_squares[next_idx];
        // update the buffer
        leaf_size[next_idx] = size;
        sums_of_keys[next_idx] = stats[0];
        sums_of_squares[next_idx] = stats[1];
        // update elements for stdev calculation
        n += size;
        sum += stats[0];
        sumSq += stats[1];

        // update next 
        next_idx = (++next_idx) % k;
    }
};

#endif