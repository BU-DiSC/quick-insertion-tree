#ifndef OBVIOUS_DETECTOR_H
#define OBVIOUS_DETECTOR_H

#include <algorithm>

#include "outlier_detector.h"

template <typename key_type>
class ObviousDetector : public OutlierDetector<key_type>
{

    // The most recently added key of the sorted tree
    key_type previous_key;
    key_type previous_delta;

    // total number of elements in sorted tree
    int counter;

    double threshold = 3;

public:
    explicit ObviousDetector(double _threshold) : threshold(_threshold)
    {
        counter = 0;
    }

    void init(const key_type &key) override
    {
        previous_key = key;
        counter = 1;
    }

    void remove(const key_type &rem_key)
    {
        // previous_key = add_key;
    }

    bool is_outlier(const key_type &key) override
    {
        if (counter <= 1)
        {
            // we don't have a delta yet
            return false;
        }
        // key_type delta = key - previous_key;
        // if (delta > threshold * (previous_key + 1)) // do +1 because prev_key can be 0 and shit goes crazy
        //     return true;

        // previous_key = key;
        return key - previous_key > threshold * (previous_delta + 1);
    }

    bool insert(const key_type &key) override
    {
        // first check if counter is 1
        if (counter <= 1)
        {
            // we register this key so we have some delta to work with, but return false
            previous_delta = key - previous_key;
            previous_key = key;
            counter++;
            return false;
        }
        if (is_outlier(key))
            return false;
        previous_delta = key - previous_key;
        previous_key = key;
        counter++;
        return true;
    }

    void force_insert(const key_type &key)
    {
        // force insert means insert without thinking of outlier 
        previous_delta = key - previous_key;
        previous_key = key; 
        counter++;
    }
};

#endif
