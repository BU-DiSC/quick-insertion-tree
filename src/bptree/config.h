#ifndef CONFIG_H
#define CONFIG_H

#include <iostream>
#include <cstring>
#include <algorithm>

struct Config {
    size_t blocks_in_memory = 15000;
    unsigned raw_read_perc = 0;
    unsigned raw_write_perc = 0;
    unsigned mix_load_perc = 0;
    unsigned updates_perc = 0;
    unsigned short_range = 0;
    unsigned mid_range = 0;
    unsigned long_range = 0;
    unsigned runs = 1;
    unsigned repeat = 1;
    unsigned seed = 1234;

    explicit Config(const char *file) {
        if (file == nullptr) return;

        std::ifstream infile(file);
        std::string line;
        while (std::getline(infile, line)) {
            // delete whitespace from line
            line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
            if (line.empty() || line[0] == '#') continue;
            std::string knob_name = line.substr(0, line.find('='));
            std::string knob_value = line.substr(knob_name.length() + 1, line.size());
            if (knob_name == "BLOCKS_IN_MEMORY") {
                blocks_in_memory = std::stoi(knob_value);
            } else if (knob_name == "RAW_READS_PERCENTAGE") {
                raw_read_perc = std::stoi(knob_value);
            } else if (knob_name == "RAW_WRITES_PERCENTAGE") {
                raw_write_perc = std::stoi(knob_value);
            } else if (knob_name == "MIXED_LOAD_PERCENTAGE") {
                mix_load_perc = std::stoi(knob_value);
            } else if (knob_name == "UPDATES_PERCENTAGE") {
                updates_perc = std::stoi(knob_value);
            } else if (knob_name == "SHORT_RANGE_QUERIES") {
                short_range = std::stoi(knob_value);
            } else if (knob_name == "MID_RANGE_QUERIES") {
                mid_range = std::stoi(knob_value);
            } else if (knob_name == "LONG_RANGE_QUERIES") {
                long_range = std::stoi(knob_value);
            } else if (knob_name == "RUNS") {
                runs = std::stoi(knob_value);
            } else if (knob_name == "REPEAT") {
                repeat = std::stoi(knob_value);
            } else if (knob_name == "SEED") {
                seed = std::stoi(knob_value);
            } else {
                std::cerr << "Invalid knob name: " << knob_name << std::endl;
            }
        }
    }
};

#endif
