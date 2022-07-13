#ifndef STDEV_DETECTOR_H
#define STDEV_DETECTOR_H

#include "outlier_detector.h"

template<typename key_type>
class StdevDetector : public OutlierDetector<key_type>
{
    float num_stdev;
    float n;
    float sum;
    float sumSq;

    public:

    StdevDetector(float _num_stdev)
    {
        num_stdev = _num_stdev;
        
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
};

#endif