#ifndef STDEV_DETECTOR_H
#define STDEV_DETECTOR_H

#include "outlier_detector.h"

size_t lower_bound(size_t k)
{
    size_t x = k;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    return k == x ? k : x >> 1;
}

template <typename key_type>
class StdevDetector : public OutlierDetector<key_type>
{
    // the number of elements to consider when calculating the standard deviation.
    size_t s0;
    // the sum of the s0 elements
    long long s1;
    // the sum of the squares of the s0 elements
    long long s2;

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
    int epsilon;

public:
    void init(const key_type &key) override
    {
        prev_key = key;
        s0 = 1;
        epsilon = 2;
        // s1 = 0;
        // s2 = 0;
    }

    StdevDetector(double num_stdev, int _k) : num_stdev(num_stdev)
    {
        s0 = s1 = s2 = next_idx = 0;
        if (_k != 0)
        {
            k = lower_bound(_k);
            leaf_size = new size_t[k + 1]{};
            sums_of_keys = new key_type[k + 1]{};
            sums_of_squares = new key_type[k + 1]{};
        }
        else
        {
            k = 0;
            leaf_size = nullptr;
            sums_of_squares = nullptr;
            sums_of_keys = nullptr;
        }
    }

    ~StdevDetector()
    {
        delete[] leaf_size;
        delete[] sums_of_keys;
        delete[] sums_of_squares;
    }

    bool _is_outlier(key_type *keys, uint32_t size, const key_type &key)
    {
        key_type deltas = 0;
        key_type deltas_sq = 0;
        key_type something = 0;
        for (size_t i = 1; i < size; i++)
        {
            key_type delta = keys[i] - keys[i - 1];
            deltas += delta;
            // deltas_sq += delta * delta;
        }
        double mean = deltas / (double)(size - 1);

        for (size_t i = 1; i < size; i++)
        {
            key_type delta = keys[i] - keys[i - 1];
            something += (delta - mean) * (delta - mean);
        }

        // double std_dev = sqrt((deltas_sq - deltas * mean) / (double)(size - 1));
        double std_dev = sqrt(something / (double)(size - 1));

        int _epsilon = 0;
        if (mean <= 1 || std_dev < 1)
        {
            _epsilon = epsilon;
        }
        return key - keys[size - 1] > mean + num_stdev * (std_dev + _epsilon);
    }

    bool is_outlier(key_type *keys, uint32_t size, const key_type &key) override
    {

        for (size_t i = size / 2; i < size; i++)
        {
            if (_is_outlier(keys, i - 1, keys[i]))
            {
                return true;
            }
            // deltas_sq += delta * delta;
        }
        return _is_outlier(keys, size, key);
    }

    bool is_outlier(const key_type &key) override
    {

        double mean = s1 / (double)s0;
        double std_dev = sqrt((s2 - s1 * mean) / (double)s0);
        // if (mean <= 1 && std_dev < 1)
        // {
        //     return false;
        // }
        int _epsilon = 0;
        if (mean <= 1 || std_dev < 1)
        {
            _epsilon = epsilon;
        }
        return key - prev_key > mean + num_stdev * (std_dev + _epsilon);
    }

    bool is_outlier(const key_type &key, const key_type &prev) override
    {

        double mean = s1 / (double)s0;
        double std_dev = sqrt((s2 - s1 * mean) / (double)s0);
        // if (mean <= 1 && std_dev < 1)
        // {
        //     return false;
        // }
        int _epsilon = 0;
        if (mean <= 1 || std_dev < 1)
        {
            _epsilon = epsilon;
        }
        return key - prev > mean + num_stdev * (std_dev + _epsilon);
    }

    void remove(const key_type &rem_key) override
    {
        s1 -= rem_key;
        s2 -= (rem_key * rem_key);
        s0--;
    }

    bool insert(const key_type &key) override
    {
        if (is_outlier(key))
            return false;
        key_type delta = key - prev_key;
        prev_key = key;
        if (k == 0 || s0 < k)
        {
            s0++;
            s1 += delta;
            s2 += delta * delta;
            return true;
        }
        s1 -= sums_of_keys[next_idx];
        s2 -= sums_of_squares[next_idx];

        // update buffer
        sums_of_keys[next_idx] = delta;
        sums_of_squares[next_idx] = delta * delta;

        // update the new node to count, sum, and sum_square
        s1 += delta;
        s2 += delta * delta;

        return true;
    }

    void force_insert(const key_type &key)
    {
        key_type delta = key - prev_key;
        prev_key = key;
        if (k == 0 || s0 < k)
        {
            s0++;
            s1 += delta;
            s2 += delta * delta;
            return;
        }
        s1 -= sums_of_keys[next_idx];
        s2 -= sums_of_squares[next_idx];

        // update buffer
        sums_of_keys[next_idx] = delta;
        sums_of_squares[next_idx] = delta * delta;

        // update the new node to count, sum, and sum_square
        s1 += delta;
        s2 += delta * delta;
    }
};

#endif
