#include <algorithm>
#include <iostream>

#include "dual_tree_knob.h"

// Some helper functions
std::string str2lower(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) { return std::tolower(c); });
    return str;
}

bool str2bool(const std::string &str) {
    return str2lower(str) == "true";
}

std::unordered_map<std::string, std::string> DUAL_TREE_KNOBS::config;
std::string DUAL_TREE_KNOBS::CONFIG_FILE_PATH;

std::unordered_map<std::string, std::string> &DUAL_TREE_KNOBS::get_config() {
    if (config.empty()) {
        std::ifstream infile(CONFIG_FILE_PATH);
        std::string line;
        while (std::getline(infile, line)) {
            line.erase(std::remove_if(line.begin(), line.end(), [](unsigned char x) { return std::isspace(x); }),
                       line.end());
            if (!line.empty() && line[0] != '#') {
                // This line is not comment
                std::string knob_name = line.substr(0, line.find('='));
                std::string knob_val = line.substr(knob_name.length() + 1, line.size());
                config.insert({knob_name, knob_val});
            }
        }
    }
    return config;
}


std::string
DUAL_TREE_KNOBS::config_get_or_default(const std::string &knob_name, const std::string &default_val) {
    const auto &tmp = get_config();
    if (tmp.find(knob_name) == tmp.end()) {
        return default_val;
    }
    return tmp.at(knob_name);
}


float DUAL_TREE_KNOBS::SORTED_TREE_SPLIT_FRAC() {
    return std::stof(DUAL_TREE_KNOBS::config_get_or_default("SORTED_TREE_SPLIT_FRAC", "0.8"));
}


float DUAL_TREE_KNOBS::UNSORTED_TREE_SPLIT_FRAC() {
    return std::stof(DUAL_TREE_KNOBS::config_get_or_default("UNSORTED_TREE_SPLIT_FRAC", "0.5"));
}


bool DUAL_TREE_KNOBS::ENABLE_LAZY_MOVE() {
    return str2bool(DUAL_TREE_KNOBS::config_get_or_default("ENABLE_LAZY_MOVE", "false"));
}


uint DUAL_TREE_KNOBS::HEAP_SIZE() {
    return std::stoi(DUAL_TREE_KNOBS::config_get_or_default("HEAP_SIZE", "0"));
}


uint DUAL_TREE_KNOBS::NUM_STDEV() {
    return std::stoi(DUAL_TREE_KNOBS::config_get_or_default("NUM_STDEV", "3"));
}


uint DUAL_TREE_KNOBS::INIT_TOLERANCE_FACTOR() {
    return std::stoi(DUAL_TREE_KNOBS::config_get_or_default("INIT_TOLERANCE_FACTOR", "100"));
}


float DUAL_TREE_KNOBS::MIN_TOLERANCE_FACTOR() {
    return std::stof(DUAL_TREE_KNOBS::config_get_or_default("MIN_TOLERANCE_FACTOR", "20"));
}


float DUAL_TREE_KNOBS::EXPECTED_AVG_DISTANCE() {
    return std::stof(DUAL_TREE_KNOBS::config_get_or_default("EXPECTED_AVG_DISTANCE", "2.5"));
}


int DUAL_TREE_KNOBS::LAST_K_STDEV() {
    return std::stoi(DUAL_TREE_KNOBS::config_get_or_default("LAST_K_STDEV", "0"));
}


int DUAL_TREE_KNOBS::OUTLIER_DETECTOR_TYPE() {
    return std::stoi(DUAL_TREE_KNOBS::config_get_or_default("OUTLIER_DETECTOR_TYPE", "1"));
}
