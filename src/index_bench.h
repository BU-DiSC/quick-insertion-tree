#ifndef INDEX_BENCH
#define INDEX_BENCH

namespace index_bench {
    template<typename KEY_TYPE, typename VALUE_TYPE>
    class Index {
    public:
        virtual ~Index() = default;

        virtual void insert(KEY_TYPE key, VALUE_TYPE value) = 0;

        virtual bool contains(KEY_TYPE key) = 0;

        virtual size_t top_k(size_t count, const KEY_TYPE &min_key) = 0;

        virtual void print(std::ostream &str) const = 0;

        friend std::ostream &operator<<(std::ostream &str, const Index &data) {
            data.print(str);
            return str;
        }
    };
}

#endif
