#ifndef OBVIOUS_DETECTOR_H
#define OBVIOUS_DETECTOR_H

#include <algorithm>
#include <iostream>

#include "outlier_detector.h"

template <typename key_type>
class ObviousDetector : public OutlierDetector<key_type>
{

    // The most recently added key of the sorted tree
    key_type sum_ranges;

    // total number of elements in sorted tree
    int counter;

    double threshold = 5;

public:
    explicit ObviousDetector(double _threshold) : threshold(_threshold)
    {
        sum_ranges = 0;
        std::cout<<"threshold = "<<threshold<<std::endl;
        counter = 0;
    }

    void remove(const key_type &rem_key)
    {
        // previous_key = add_key;
    }

    bool is_outlier(const key_type &key, const key_type &start_range) override
    {
        if (counter == 0)
        {
            // we don't have a mean range yet
            return false;
        }
        double mean_range = sum_ranges / (double)counter;
        return key > start_range + threshold * mean_range;
    }

    bool insert(const key_type &range) override
    {
        // first check if counter is 1
        if (counter == 0)
        {
            sum_ranges = range;
            counter++;
            return true;
        }

        sum_ranges += range;
        counter++;
        return true;
    }

    void force_insert(const key_type &key)
    {
        // // force insert means insert without thinking of outlier
        // previous_delta = key - previous_key;
        // previous_key = key;
        // counter++;
    }
};

#endif
