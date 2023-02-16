#include <iostream>

#include "bamboofilter.h"

int main(int argc, char *argv[]) {
    srand(1234);
    int fp = 0;
    int fn = 0;
    BambooFilter bbf(65536, 2);
    for (int i = 0; i < 100000; ++i) {
        if (bbf.Lookup(&i, sizeof(i))) {
            fp++;
        }
    }
    for (int i = 0; i < 100000; ++i) {
        bbf.Insert(&i, sizeof(i));
    }
    for (int i = 0; i < 100000; ++i) {
        if (!bbf.Lookup(&i, sizeof(i))) {
            fn++;
        }
    }
    for (int i = 25000; i < 75000; ++i) {
        bbf.Delete(&i, sizeof(i));
    }
    for (int i = 0; i < 25000; ++i) {
        if (!bbf.Lookup(&i, sizeof(i))) {
            fn++;
        }
    }
    for (int i = 25000; i < 75000; ++i) {
        if (bbf.Lookup(&i, sizeof(i))) {
            fp++;
        }
    }
    for (int i = 75000; i < 100000; ++i) {
        if (!bbf.Lookup(&i, sizeof(i))) {
            fn++;
        }
    }

    std::cout << "False positives: " << fp << std::endl;
    std::cout << "False negatives: " << fn << std::endl;
    for (int i = 100000; i < 1000000; ++i) {
        if (bbf.Lookup(&i, sizeof(i))) {
            fp++;
        }
    }
    std::cout << "Total false positives: " << fp << std::endl;
    std::cout << "Total false negatives: " << fn << std::endl;
    return 0;
}
