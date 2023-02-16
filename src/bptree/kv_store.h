#ifndef KV_STORE_H
#define KV_STORE_H

#include <optional>
#include <ostream>

template<typename key_type, typename value_type>
class kv_store {
    virtual std::ostream &get_stats(std::ostream &os) const = 0;

public:
    /**
     * @param key
     * @param value
     * @return true if the key is inserted, false if the key was updated
     */
    virtual bool insert(const key_type &key, const value_type &value) = 0;

    /**
     * @param key
     * @param value
     * @return true if the key was updated
     */
    virtual bool update(const key_type &key, const value_type &value) = 0;

    virtual std::optional<value_type> get(const key_type &key) = 0;

    virtual bool contains(const key_type &key) {
        return get(key).has_value();
    }

    friend std::ostream &operator<<(std::ostream &os, const kv_store &store) {
        return store.get_stats(os);
    }
};

#endif
