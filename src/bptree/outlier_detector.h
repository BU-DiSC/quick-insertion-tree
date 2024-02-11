#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

#include <cstdint>

namespace IKR {
    inline size_t max_distance(const size_t dq, const uint16_t n1, const uint16_t n2) {
        return (dq + dq / 2) * n2 / n1;
    }
}

#endif
