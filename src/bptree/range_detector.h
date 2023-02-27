#ifndef RANGE_DETECTOR_H
#define RANGE_DETECTOR_H

#include "outlier_detector.h"

template <typename key_type>
class RangeDetector : public OutlierDetector<key_type>
{
    // The most recently added key of the sorted tree
    key_type sum_ranges;

    // total number of elements in sorted tree
    int counter;

    double threshold = 5;

public:
    explicit RangeDetector(double threshold) : threshold(threshold)
    {
        sum_ranges = 0;
        counter = 0;
    }

    bool is_outlier(const key_type &key, const key_type &start) override
    {
        if (counter == 0)
        {
            // we don't have a mean range yet
            return false;
        }
        double mean_range = sum_ranges / (double) counter;
        return key > start + threshold * mean_range;
    }

    void update(const key_type &range) override
    {
        sum_ranges += range;
        counter++;
    }
};

#endif
