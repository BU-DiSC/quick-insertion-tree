#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

#include <cstdint>

namespace IQRDetector {
    size_t lower_bound(size_t dq, uint16_t n1, uint16_t n2) {
        return (dq * .7) * n2 / n1;
    }

    size_t upper_bound(size_t dq, uint16_t n1, uint16_t n2) {
        return (dq / .7) * n2 / n1;
    }
}

#endif
