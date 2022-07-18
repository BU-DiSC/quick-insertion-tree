#ifndef STDEV_DETECTOR_H
#define STDEV_DETECTOR_H

#include <vector>
#include <iostream>
#include <math.h>
#include "outlier_detector.h"

template<typename key_type>
class StdevDetector : public OutlierDetector<key_type>
{
    float num_stdev;
    float n;
    float sum;
    float sumSq;

    // Stdev of last k nodes 
    int k;

    // Current number of nodes
    int num_of_nodes;

    // A vector storing every leaf size in sorted tree
    std::vector<int> leaf_size;

    // Sums of keys of each node
    std::vector<long long> sums_of_keys;

    // Sums of squared keys of each node
    std::vector<long long> sums_of_squares;

    public:

    StdevDetector(float _num_stdev, int _k)
    {
        num_stdev = _num_stdev;
        k = _k;
    }

    void updateStdev(key_type key){
        n++;
        sum+=key;
        sumSq += key*key;
    }

    bool is_outlier(key_type key, int tree_size) {
        float sd = sqrt((n * sumSq - sum * sum)/(n * (n - 1)));

        if(tree_size <30){
            std::cout<<"inset key : "<< key << "    sd: " << sd << std::endl;

            updateStdev(key);
            return false;
        }
        // std::cout<<"inset key : "<< key << "    sd: " << sd << std::endl;
        if(key > sum/n + num_stdev * sd){
            return true;
        }
        updateStdev(key);
        std::cout<<"inset key : "<< key << "    sd: " << sd << std::endl;

        return false;
    }

    void update(int size, long long* stats) {
        leaf_size.push_back(size);
        sums_of_keys.push_back(stats[0]);
        sums_of_squares.push_back(stats[1]);

        n += size;
        sum += stats[0];
        sumSq += stats[1];

        if (k <= 0 || leaf_size.size() <= k) {
            return;
        }
        int remove_idx = leaf_size.size() - k - 1;
        n -= leaf_size[remove_idx];
        sum -= sums_of_keys[remove_idx];
        sumSq -= sums_of_squares[remove_idx];
    }
};

#endif