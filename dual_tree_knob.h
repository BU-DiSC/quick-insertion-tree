#ifndef DUAL_TREE_KNOB_H
#define DUAL_TREE_KNOB_H
#define TYPE int

#include <fstream>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <iostream>

template<typename _key, typename _value>
class DUAL_TREE_KNOBS
{
private:
    static std::unordered_map<std::string, std::string>& get_config();
 
    static std::string config_get_or_default(std::string knob_name, std::string default_val);
    
public:
    static const std::string CONFIG_FILE_PATH; 

    // Sorted tree split fraction, it will affect the space utilization of the tree. It means how
    // many elements will stay in the original node.
    static const float SORTED_TREE_SPLIT_FRAC;
 
    // Unsorted tree splitting fraction.
    static const float UNSORTED_TREE_SPLIT_FRAC;  

    static const bool ENABLE_LAZY_MOVE;

    // Knob for heap buffer, if HEAP_SIZE <= 0, heap buffer will be disabled
    static const uint HEAP_SIZE;

    // The initial tolerance threshold, determine whether the key of the newly added tuple is too far from the previous
    //tuple in the sorted tree. If set it to 0, the dual tree will disable the outlier detector.
    static const uint INIT_TOLERANCE_FACTOR;

    // The minimum value of the TOLERANCE_FACTOR, when the value of tolerance factor is too small, 
    //most tuples will be inserted to the unsorted tree, thus we need to keep the value from too small.
    //This value should be less than @INIT_TOLERANCE_FACTOR
    static const float MIN_TOLERANCE_FACTOR;

    // The expected average distance between any two consecutive tuples in the sorted tree. This
    //tuning knob helps to modify the tolerance factor in the outlier detector. If it is less or equal to 
    //1, then the tolerance factor becomes a constant.
    static const float EXPECTED_AVG_DISTANCE;

    static const bool ENABLE_OUTLIER_DETECTOR;

    static const TYPE OUTLIER_DETECTOR_TYPE;

    // outlier detector type DIST
    static constexpr TYPE DIST = 1;

    static constexpr TYPE STDEV = 2;


};

#include "dual_tree_knob.tpp"

#endif // !DUAL_TREE_KNOB_H