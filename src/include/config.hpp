#pragma once

#include <span>
#include <string>

struct Config {
    unsigned blocks_in_memory = 2000000;
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
    unsigned num_threads = 1;
    std::string results_csv = "results.csv";
    bool binary_input = true;
    bool validate = false;
    std::span<char *> files;

    void parse(const char *file);
    void parse(int argc, char **argv);
    void print();
};
