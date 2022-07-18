#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

template<typename key_type>
class OutlierDetector
{
public:

    virtual bool is_outlier(key_type key) = 0;
    
};

#endif