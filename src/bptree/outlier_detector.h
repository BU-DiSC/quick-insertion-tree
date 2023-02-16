#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

#include "bp_tree.h"

template<typename key_type>
class OutlierDetector {
public:

    virtual void init(const key_type &key) = 0;

    virtual bool is_outlier(const key_type &key) = 0;

    virtual void update(const bp_stats &stats) = 0;

    virtual ~OutlierDetector() = default;
};

#endif
