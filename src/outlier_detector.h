#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

#include "betree.h"

template<typename key_type, typename value_type>
class OutlierDetector {
public:

    virtual bool is_outlier(const key_type &key) = 0;

    virtual void update(BeTree<key_type, value_type> *tree) = 0;

    virtual ~OutlierDetector() = default;
};

#endif