#ifndef COLLECTION_H
#define COLLECTION_H

template<typename K, typename V>
class collection {
    virtual std::ostream &get_stats(std::ostream &os) const = 0;

public:
    virtual ~collection() = default;

    virtual void add(const K &, const V &) = 0;

    virtual bool contains(const K &) = 0;

    friend std::ostream &operator<<(std::ostream &os, const collection &that) {
        return that.get_stats(os);
    }
};

#endif
