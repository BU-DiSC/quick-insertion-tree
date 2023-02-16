#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

struct outlier_stats {
    uint64_t keys_sum;
    uint64_t key_squares_sum;
    uint32_t keys_count;
};

template<typename key_type>
class OutlierDetector {
public:

    virtual void init(const key_type &key) = 0;

    virtual bool is_outlier(const key_type &key) = 0;

    virtual void update(const key_type *keys, uint32_t size) = 0;

    virtual ~OutlierDetector() = default;
};

#endif