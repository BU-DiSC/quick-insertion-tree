#ifndef IQR_DETECTOR_H
#define IQR_DETECTOR_H

#include "outlier_detector.h"

enum OUTLIER
{
    LEFT,
    NO,
    RIGHT
};

template <typename key_type>
class IQRDetector : public OutlierDetector<key_type>
{
public:
    bool has_outlier(key_type *keys, const uint32_t &size) override
    {
        return keys[size * 2 / 3] + (keys[size * 2 / 3] - keys[0]) * 1.5 < keys[size - 1];
    }

    static bool is_extreme_outlier(key_type *keys, const uint32_t &size, const key_type &key) 
    {
        return keys[size * 2 / 3] + (keys[size * 2 / 3] - keys[0]) * 1.5 < key || keys;
    }

    static OUTLIER is_outlier(key_type *keys, const uint32_t &size, const key_type &key)
    {
        // return true;
        // key_type &iqr = keys[size - 1];
        // return iqr + (iqr - keys[0]) * 1.5 < key;
        if (keys[size * 1 / 4] - (keys[size * 3 / 4] - keys[size * 1 / 4]) * 1.5 > key)
            return LEFT;
        if (keys[size * 3 / 4] + (keys[size * 3 / 4] - keys[size * 1 / 4]) * 1.5 < key)
            return RIGHT;
        return NO;
    }
};

#endif
