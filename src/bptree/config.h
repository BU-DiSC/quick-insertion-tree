#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <unordered_map>
#include <fstream>

#include "dist_detector.h"
#include "stdev_detector.h"
#include "obvious_detector.h"
#include "heap.h"

void str2lower(std::string &str) {
    transform(str.begin(), str.end(), str.begin(), tolower);
}

bool str2bool(std::string str) {
    str2lower(str);
    return str == "true";
}

struct Config {
    float sorted_tree_split_frac;
    float unsorted_tree_split_frac;
    bool enable_lazy_move;
#ifdef ENABLE_HEAP
    size_t heap_size;
#endif
    size_t blocks_in_memory;
    float init_tolerance_factor;
    float num_stdev;
    float num_obvious; 
    float min_tolerance_factor;
    float expected_avg_distance;
    enum {
        DIST, STDEV, BASIC, NONE
    } outlier_detector_type, obvious_detector_type;
    int last_k_stdev;
    int min_stdev_ct; 

    template<typename key_type>
    OutlierDetector<key_type> *get_outlier_detector() const {
        if (outlier_detector_type == DIST) {
            return new DistDetector<key_type>(init_tolerance_factor, min_tolerance_factor, expected_avg_distance);
        }
        if (outlier_detector_type == STDEV) {
            return new StdevDetector<key_type>(num_stdev, last_k_stdev);
        }       
        return nullptr;
    }

    template<typename key_type>
    OutlierDetector<key_type> *get_obvious_detector() const {
        if (obvious_detector_type == BASIC) {
            return new ObviousDetector<key_type>(num_obvious);
        }     
         if (obvious_detector_type == STDEV) {
            return new StdevDetector<key_type>(num_stdev, last_k_stdev);
        }       
        return nullptr;
    }

#ifdef ENABLE_HEAP
    template<typename key_type, typename value_type>
    Heap<key_type, value_type> *get_heap_buffer() const {
        if (heap_size == 0) {
            return nullptr;
        }
        return new Heap<key_type, value_type>(heap_size);
    }
#endif

    explicit Config(const char *file) {
        sorted_tree_split_frac = 0.8;
        unsorted_tree_split_frac = 0.5;
        enable_lazy_move = true;
#ifdef ENABLE_HEAP
        heap_size = 0;
#endif
        init_tolerance_factor = 100;
        num_stdev = 3;
        num_obvious = 3;
        min_tolerance_factor = 20;
        expected_avg_distance = 2.5;
        outlier_detector_type = NONE;
        last_k_stdev = 0;
        min_stdev_ct = 30;
        blocks_in_memory = 15000;
        if (file == nullptr) return;

        std::ifstream infile(file);
        std::string line;
        while (std::getline(infile, line)) {
            // delete whitespace from line
            line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
            if (line.empty() || line[0] == '#') continue;
            std::string knob_name = line.substr(0, line.find('='));
            std::string knob_value = line.substr(knob_name.length() + 1, line.size());
            if (knob_name == "SORTED_TREE_SPLIT_FRAC") {
                sorted_tree_split_frac = std::stof(knob_value);
            } else if (knob_name == "UNSORTED_TREE_SPLIT_FRAC") {
                unsorted_tree_split_frac = std::stof(knob_value);
            } else if (knob_name == "BLOCKS_IN_MEMORY") {
                blocks_in_memory = std::stoi(knob_value);
            } else if (knob_name == "ENABLE_LAZY_MOVE") {
                enable_lazy_move = str2bool(knob_value);
            } else if (knob_name == "HEAP_SIZE") {
#ifdef ENABLE_HEAP
                heap_size = std::stoi(knob_value);
#else
                std::cerr << "Heap is not enabled" << std::endl;
#endif
            } else if (knob_name == "INIT_TOLERANCE_FACTOR") {
                init_tolerance_factor = std::stof(knob_value);
            } else if (knob_name == "NUM_STDEV") {
                num_stdev = std::stof(knob_value);
            } else if (knob_name == "NUM_OBVIOUS") {
                num_obvious = std::stof(knob_value);
            } else if (knob_name == "MIN_TOLERANCE_FACTOR") {
                min_tolerance_factor = std::stof(knob_value);
            } else if (knob_name == "EXPECTED_AVG_DISTANCE") {
                expected_avg_distance = std::stof(knob_value);
            } else if (knob_name == "OUTLIER_DETECTOR_TYPE") {
                if (knob_value == "\"DIST\"") {
                    outlier_detector_type = DIST;
                } else if (knob_value == "\"STDEV\"") {
                    outlier_detector_type = STDEV;
                } else {
                    std::cerr << "Invalid OUTLIER_DETECTOR_TYPE: " << knob_value << std::endl;
                }
            } else if (knob_name == "OBVIOUS_DETECTOR_TYPE") {
                if (knob_value == "\"BASIC\"") {
                    obvious_detector_type = BASIC;
                } 
                else if (knob_value == "\"STDEV\"") {
                    obvious_detector_type = STDEV;
                } else {
                    std::cerr << "Invalid OBVIOUS_DETECTOR_TYPE: " << knob_value << std::endl;
                }
            } else if (knob_name == "LAST_K_STDEV") {
                last_k_stdev = std::stoi(knob_value);
            } else if (knob_name == "MIN_STDEV_CT") {
                min_stdev_ct = std::stoi(knob_value);
            } else {
                std::cerr << "Invalid knob name: " << knob_name << std::endl;
            }
        }
    }
};

#endif
