#ifndef DIST_DETECTOR_H
#define DIST_DETECTOR_H

#include "outlier_detector.h"

template<typename key_type>
class DistDetector : public OutlierDetector<key_type>
{
    // The default value of @average_distance.
    static constexpr float INIT_AVG_DIST = -1;

    // The acceptable error that @avg_distance is greater than @expected_avg_distance. This 
    //error cannot be guaranteed, but it will help outlier detector to control the tolerance factor 
    //when abs(@expected_avg_distance - @avg_distance) > the error
    static constexpr float MAX_ERROR = 0.5;


    // The minimum value of the INIT_TOLERANCE_FACTOR, when the value of tolerance factor is too small, 
    //most tuples will be inserted to the unsorted tree, thus we need to keep the value from too small
    const float min_tolerance_factor;

    // The average distance between any two consecutive keys of tuples in the sorted tree.
    float avg_dist;

    // The expected average distance.
    const float expected_avg_dist;

    // The tolerance threshold, determine whether the key of the newly added tuple is too far from the previous
    //tuple in the sorted tree. When the distance is greater than @avg_distance * @tolerance_factor,
    //the newly added tuple should be added to the unsorted tree. Should be greater than 0.
    float tolerance_factor;

    // The initial tolerance factor. When the true @avg_distance is around @expected_avg_distance, then reset
    // @toleranace_factor to the initial one.
    const float init_tolerance_factor;

    // The most recently added key of the sorted tree;
    key_type previous_key;

    // total number of elements in sorted tree
    uint counter;

    void update_tolerance_factor()
    {
        if(avg_dist < expected_avg_dist + MAX_ERROR)
        {
            tolerance_factor = init_tolerance_factor;
        }
        else
        {
            tolerance_factor *= expected_avg_dist / avg_dist;
        }
        tolerance_factor = std::max(tolerance_factor, min_tolerance_factor);
    }

public:

    DistDetector(float _tolerance_factor, float _min_tolerance_factor, 
                    float _expected_avg_dist=1) : tolerance_factor(_tolerance_factor), 
                    expected_avg_dist(_expected_avg_dist), 
                    min_tolerance_factor(_min_tolerance_factor),
                    init_tolerance_factor(_tolerance_factor)
    {
        avg_dist = INIT_AVG_DIST;
        counter = 0;
    }

    bool is_outlier(key_type key) {
        if (counter == 0) {
            // to calculate distance, there should be at least 1 keys in the tree
            previous_key = key;
            counter++;
            return false;
        }
        key_type dist = key - previous_key;
        dist = dist > 0 ? dist : 0;
        if (avg_dist == INIT_AVG_DIST) {
            // sorted tree is empty, counter = 1
            avg_dist = dist;
        }
        else {
            if (dist > avg_dist * tolerance_factor) {
                // key to insert is outlier
                return true;
            }
            avg_dist = ((double)(avg_dist * (counter - 1) + dist)) / counter;
            if (expected_avg_dist > 1) {
                update_tolerance_factor();
            }
        }
        // update last inserted key and counter
        previous_key = key > previous_key ? key : previous_key;
        counter++;

        return false;
    }
};

#endif