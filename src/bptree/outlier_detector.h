#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

#include <cstdint>

namespace IQRDetector {
    template<typename key_type>
    key_type max_distance(const key_type &q1, const key_type &q2, const uint16_t &n1, const uint16_t &n2) {
        return (q2 - q1) * 1.5 * n2 / n1;
    }
}

#endif
