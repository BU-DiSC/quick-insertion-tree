#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

template <typename key_type>
class OutlierDetector
{
public:
    virtual void update(const key_type &key) {};
    virtual bool is_outlier(const key_type &key) {
        return false;
    };
    virtual bool is_outlier(const key_type &key, const key_type &range) {
        return false;
    };
    virtual bool has_outlier(key_type *keys, const uint32_t &size) {
        return false;
    };
    virtual ~OutlierDetector() = default;
};

#endif
