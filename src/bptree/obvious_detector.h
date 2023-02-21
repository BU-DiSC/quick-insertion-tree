#ifndef OBVIOUS_DETECTOR_H
#define OBVIOUS_DETECTOR_H

#include <algorithm>

#include "outlier_detector.h"

template <typename key_type>
class ObviousDetector : public OutlierDetector<key_type>
{

    // The most recently added key of the sorted tree
    key_type previous_key;

    // total number of elements in sorted tree
    int counter;

    double threshold = 3;

public:
    ObviousDetector(double _threshold) : threshold(_threshold)
    {
        counter = 0;
    }

    void init(const key_type &key) override
    {
        previous_key = key;
        counter = 1;
    }

    void remove(const key_type &rem_key, const key_type &add_key)
    {

        previous_key = add_key;
        // return;
    }

    bool is_outlier(const key_type &key) override
    {
        key_type delta = key - previous_key;
        if (delta > threshold * (previous_key + 1)) // do +1 because prev_key can be 0 and shit goes crazy 
            return true;

        previous_key = key;
        return false;
    }

    void insert(const key_type &key) override
    {
    }
};

#endif
