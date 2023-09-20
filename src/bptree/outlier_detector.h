#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

#include <cstdint>

namespace IQRDetector {
//    template<typename key_type>
//    size_t max_distance(const key_type &q1, const key_type &q2, uint16_t n1, uint16_t n2) {
//        return (q2 - q1) * 1.5 * n2 / n1;
//    }

    size_t max_distance(size_t dq, uint16_t n1, uint16_t n2) {
        return (dq + dq / 2) * n2 / n1;
    }
}

#endif
