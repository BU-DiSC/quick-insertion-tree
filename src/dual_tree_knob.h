#ifndef DUAL_TREE_KNOB_H
#define DUAL_TREE_KNOB_H

#include <unordered_map>

#include "dist_detector.h"
#include "stdev_detector.h"

class DUAL_TREE_KNOBS {
    static std::unordered_map<std::string, std::string> &get_config();

    static std::string config_get_or_default(const std::string &knob_name, const std::string &default_val);

    static std::unordered_map<std::string, std::string> config;

public:

    static std::string CONFIG_FILE_PATH;

    // Sorted tree split fraction, it will affect the space utilization of the tree. It means how
    // many elements will stay in the original node.
    static float SORTED_TREE_SPLIT_FRAC();

    // Unsorted tree splitting fraction.
    static float UNSORTED_TREE_SPLIT_FRAC();

    static bool ENABLE_LAZY_MOVE();

    // Knob for heap buffer, if HEAP_SIZE <= 0, heap buffer will be disabled
    static uint HEAP_SIZE();

    // The initial tolerance threshold, determine whether the key of the newly added tuple is too far from the previous
    //tuple in the sorted tree. If set it to 0, the dual tree will disable the outlier detector.
    static uint INIT_TOLERANCE_FACTOR();

    static uint NUM_STDEV();

    // The minimum value of the TOLERANCE_FACTOR, when the value of tolerance factor is too small, 
    //most tuples will be inserted to the unsorted tree, thus we need to keep the value from too small.
    //This value should be less than @INIT_TOLERANCE_FACTOR
    static float MIN_TOLERANCE_FACTOR();

    // The expected average distance between any two consecutive tuples in the sorted tree. This
    //tuning knob helps to modify the tolerance factor in the outlier detector. If it is less or equal to 
    //1, then the tolerance factor becomes an ant.
    static float EXPECTED_AVG_DISTANCE();

    static int OUTLIER_DETECTOR_TYPE();

    // outlier detector type DIST: distance based outlier detector
    static constexpr int DIST = 1;

    // outlier detector type STDEV: standard deviation based outlier detector
    static constexpr int STDEV = 2;

    // Using only last k nodes to calculate the STDEV
    static int LAST_K_STDEV();

    template<typename key_type, typename value_type>
    static OutlierDetector<key_type, value_type> *get_detector() {
        if (OUTLIER_DETECTOR_TYPE() == DIST) {
            return new DistDetector<key_type, value_type>(INIT_TOLERANCE_FACTOR(), MIN_TOLERANCE_FACTOR(),
                                                          EXPECTED_AVG_DISTANCE());
        }
        if (OUTLIER_DETECTOR_TYPE() == STDEV) {
            return new StdevDetector<key_type, value_type>(NUM_STDEV(), LAST_K_STDEV());
        }
        return nullptr;
    }
};

#endif