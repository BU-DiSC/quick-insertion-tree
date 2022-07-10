#ifndef FILE_READER_H
#define FILE_READER_H
#include <iostream>
#include <string>
#include <vector>

class FileReader 
{
    std::ifstream ifs;
    std::vector<int> data;
public:
    FileReader(std::string filename) {
        ifs.open(filename);
    }

    std::vector<int> read() {
        std::string line;
        while(std::getline(ifs, line)) {
            int key = std::stoi(line);
            data.push_back(key);
        }
        return data;
    }
};

#endif