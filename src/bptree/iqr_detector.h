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

    static bool is_outlier(key_type *keys, const uint32_t &size, key_type &key) {
        return true;
        key_type &iqr = keys[size - 1];
        return iqr + (iqr - keys[0]) * 10 < key;
    }
};

#endif
