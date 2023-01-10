#ifndef COLLECTION_H
#define COLLECTION_H

#include <ostream>

template<typename key_type, typename value_type>
class collection {
    virtual std::ostream &get_stats(std::ostream &os) const = 0;

public:
    virtual ~collection() = default;

    virtual void add(const key_type &, const value_type &) = 0;

    virtual bool contains(const key_type &) = 0;

    friend std::ostream &operator<<(std::ostream &os, const collection &that) {
        return that.get_stats(os);
    }
};

#endif
