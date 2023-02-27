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
    double num_stdev;

    int epsilon;

public:
    StdevDetector(double num_stdev) : num_stdev(num_stdev)
    {
        epsilon = 5;
    }

    bool _is_outlier(key_type *keys, uint32_t size, const key_type &key)
    {
        key_type deltas = keys[size-1] - keys[0];
        double mean = deltas / (double)(size - 1);

        key_type squares = 0;
        for (size_t i = 1; i < size; i++)
        {
            key_type delta = keys[i] - keys[i - 1];
            squares += (delta - mean) * (delta - mean);
        }

        double std_dev = sqrt(squares / (double)(size - 1));

        int _epsilon = 0;
        if (mean <= 1 || std_dev < 1)
        {
            _epsilon = epsilon;
        }
        return key - keys[size - 1] > mean + num_stdev * (std_dev + _epsilon);
    }

    bool has_outlier(key_type *keys, const uint32_t &size) override
    {
        for (size_t i = size / 2; i < size; i++)
        {
            if (_is_outlier(keys, i - 1, keys[i]))
            {
                return true;
            }
        }
        return false;
    }
};

#endif
