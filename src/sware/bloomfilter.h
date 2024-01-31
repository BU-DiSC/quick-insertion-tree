#include <vector>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <cmath>
#include "murmurhash.h"

class BloomFilter
{

private:
    int numIndex;
    int size;
    bool *bf_vec;

    void makeBloomFilter()
    {
        bf_vec = new bool[size];
        memset(bf_vec, 0, size * sizeof(*bf_vec));
    }

    void getIndex(std::string key, std::vector<int> *index)
    {
        unsigned int h = MurmurHash2(key.c_str(), key.size(), 0xbc9f1d34);

        const unsigned int delta = (h >> 17) | (h << 15); // Rorate right 17 bits
        for (int i = 0; i < numIndex; i++)
        {
            // index[i] = h % size;
            index->at(i) = h % size;
            h += delta;
        }

        return;
    }

public:
    int numElement;
    int bitsPerElement;
    int numInserted;
    BloomFilter()
    {
        numElement = 1024;
        bitsPerElement = 10;
        numInserted = 0;
        numIndex = (int)std::floor(0.693 * bitsPerElement + 0.5);
        size = numElement * bitsPerElement;
    }

    BloomFilter(int numElement_, int bitsPerElement_)
    {
        numElement = numElement_;
        bitsPerElement = bitsPerElement_;
        numInserted = 0;
        numIndex = (int)std::floor(0.693 * bitsPerElement + 0.5);
        size = numElement * bitsPerElement;

        makeBloomFilter();
    }

    void program(std::string key)
    {
        std::vector<int> index(numIndex, 0);
        getIndex(key, &index);
        numInserted += 1;
        assert(numInserted <= numElement);

        for (int i = 0; i < numIndex; i++)
        {
            bf_vec[index[i]] = 1;
        }


    }

    bool query(std::string key)
    {
        std::vector<int> index(numIndex, 0);
        getIndex(key, &index);

        for (int i = 0; i < numIndex; i++)
        {
            if (bf_vec[index[i]] == 0)
                return false;
        }

        return true; // positive
    }

    bool reset()
    {

        memset(bf_vec, 0, size * sizeof(*bf_vec));
        numInserted = 0;
        return true;
    }

    int getIndexNum()
    {
        return numIndex;
    }

    int getSize()
    {
        return size;
    }

    ~BloomFilter()
    {
        delete[] bf_vec;
    }
};
