#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

template<typename key_type>
class OutlierDetector
{
public:

    virtual bool is_outlier(key_type key, int tree_size) = 0;

    virtual void update(int size, long long* stats) = 0;
};

#endif