#ifndef CONFIG_H
#define CONFIG_H

#include <iostream>
#include <cstring>
#include <algorithm>

struct Config {
    size_t blocks_in_memory;

    explicit Config(const char *file) {
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
            if (knob_name == "BLOCKS_IN_MEMORY") {
                blocks_in_memory = std::stoi(knob_value);
            } else {
                std::cerr << "Invalid knob name: " << knob_name << std::endl;
            }
        }
    }
};

#endif
