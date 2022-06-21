#ifndef DUAL_TREE_KNOB_H
#define DUAL_TREE_KNOB_H

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
};

#include "dual_tree_knob.tpp"

#endif // !DUAL_TREE_KNOB_H