#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

#include <cstdint>

namespace IQRDetector {
    size_t max_distance(size_t dq, uint16_t n1, uint16_t n2) {
        return (dq + dq / 2) * n2 / n1;
    }
}

#endif
