#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

#include <cstdint>

namespace IQRDetector {
    template<typename key_type>
    key_type max_outlier(const key_type &q1, const key_type &q2) {
        return q2 + (q2 - q1) * 1.5;
    }

    template<typename key_type>
    key_type max_outlier2(const key_type &q1, const key_type &q2) {
        return q2 + (q2 - q1) * 3;
    }
};


#endif
