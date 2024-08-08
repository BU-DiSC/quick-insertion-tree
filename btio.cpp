#include <iostream>
#include <string>
#include <istream>
#include <ostream>

using key_type = unsigned;

void text_to_bin(std::istream& input, std::ostream& output) {
    std::string line;
    while (std::getline(input, line)) {
        key_type key = std::stoul(line);
        output.write(reinterpret_cast<const char *>(&key), sizeof(key_type));
    }
}

void bin_to_text(std::istream& input, std::ostream& output) {
    key_type key;
    while (input.read(reinterpret_cast<char*>(&key), sizeof(key_type))) {
        output << key << std::endl;
    }
}

int main(int argc, char const *argv[]) {
    if (argc == 1) {
        text_to_bin(std::cin, std::cout);
    } else {
        bin_to_text(std::cin, std::cout);
    }
    return 0;
}
