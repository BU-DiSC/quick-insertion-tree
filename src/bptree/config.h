#ifndef CONFIG_H
#define CONFIG_H

#include <algorithm>
#include <cstring>
#include <iostream>

struct Config {
    size_t blocks_in_memory = 15000;
    unsigned raw_read_perc = 0;
    unsigned raw_write_perc = 0;
    unsigned mixed_writes_perc = 0;
    unsigned mixed_reads_perc = 0;
    unsigned updates_perc = 0;
    unsigned short_range = 0;
    unsigned mid_range = 0;
    unsigned long_range = 0;
    unsigned runs = 1;
    unsigned repeat = 1;
    unsigned seed = 1234;
    unsigned num_r_threads = 1;
    unsigned num_w_threads = 1;
    std::string results_csv = "results.csv";
    bool binary_input = true;
    bool validate = false;

    static std::string str_val(const std::string &val) {
        return val.substr(1, val.size() - 2);
    }

    static bool bool_val(const std::string &val) {
        return val == "true";
    }

    explicit Config(const char *file) {
        if (file == nullptr) return;

        std::ifstream infile(file);
        if (!infile) {
            std::cerr << "Error: could not open config file " << file << std::endl;
            return;
        }
        std::string line;
        while (std::getline(infile, line)) {
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
            } else if (knob_name == "MIXED_WRITES_PERCENTAGE") {
                mixed_writes_perc = std::stoi(knob_value);
            } else if (knob_name == "MIXED_READ_PERCENTAGE") {
                mixed_reads_perc = std::stoi(knob_value);
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
            } else if (knob_name == "RESULTS_FILE") {
                results_csv = str_val(knob_value);
            } else if (knob_name == "NUM_R_THREADS") {
                num_r_threads = std::stoi(knob_value);
            } else if (knob_name == "NUM_W_THREADS") {
                num_w_threads = std::stoi(knob_value);
            } else if (knob_name == "BINARY_INPUT") {
                binary_input = bool_val(knob_value);
            } else if (knob_name == "VALIDATE") {
                validate = bool_val(knob_value);
            } else {
                std::cerr << "Invalid knob name: " << knob_name << std::endl;
            }
        }
    }
};

#endif
