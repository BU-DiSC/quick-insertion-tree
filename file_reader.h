#ifndef FILE_READER_H
#define FILE_READER_H
#include <iostream>
#include <string>
#include <vector>

class FileReader 
{
    std::string filename;

public:
    FileReader(std::string _filename) {
        filename = _filename;
    }

    ~FileReader() {
    }

    std::vector<int> read() {
        std::vector<int> data;
        std::string line;
        std::ifstream ifs;
        ifs.open(filename);
        while(std::getline(ifs, line)) {
            int key = std::stoi(line);
            data.push_back(key);
        }
        ifs.close();
        return data;
    }
};

#endif