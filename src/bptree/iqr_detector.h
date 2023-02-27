#ifndef IQR_DETECTOR_H
#define IQR_DETECTOR_H

#include "outlier_detector.h"

template <typename key_type>
class IQRDetector : public OutlierDetector<key_type>
{
public:
    bool has_outlier(key_type *keys, const uint32_t &size) override
    {
        return keys[size * 2 / 3] + (keys[size * 2 / 3] - keys[0]) * 1.5 < keys[size - 1];
    }
};

#endif
