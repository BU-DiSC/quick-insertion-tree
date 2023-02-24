#ifndef OUTLIER_DETECTOR_H
#define OUTLIER_DETECTOR_H

template <typename key_type>
class OutlierDetector
{
public:
    virtual void init(const key_type &key){};

    virtual bool is_outlier(const key_type &key){};
    virtual bool has_outlier(key_type *keys, const uint32_t &size){};

    virtual bool is_outlier(const key_type &key, const key_type &prev){};

    virtual bool insert(const key_type &key) = 0;
    virtual void force_insert(const key_type &key) = 0;

    virtual void remove(const key_type &rem_key) = 0;

    virtual ~OutlierDetector() = default;
};

#endif
