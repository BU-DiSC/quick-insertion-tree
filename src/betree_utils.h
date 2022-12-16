#ifndef BETREE_UTILS_H
#define BETREE_UTILS_H

#include <cmath>

enum Result {
    SPLIT, NOSPLIT
};

// Defining all required tuning knobs/sizes for the tree
template<typename key_type, typename value_type>
struct BeTree_Default_Knobs {
    // Epsilon value
    static constexpr double EPSILON = 0.5;

    // size of every block in Bytes
    static constexpr int BLOCK_SIZE = 4096;

    // size of metadata in a node
    static constexpr int METADATA_SIZE = 40;

    // leftover data size after deducting space for metadata
    static constexpr int DATA_SIZE = BLOCK_SIZE - METADATA_SIZE;

    // size of every leaf in Bytes
    static constexpr int LEAF_SIZE = DATA_SIZE;

    // number of data pairs that the tree will hold per leaf
    static constexpr int NUM_DATA_PAIRS = (LEAF_SIZE - sizeof(int)) / (sizeof(key_type) + sizeof(value_type));

    // size of a key-value pair unit
    static constexpr int UNIT_SIZE = sizeof(key_type *) + sizeof(value_type *);

// number of buffer elements that can be held (at max)
// equal to (Buffer size - Buffer metadata size)/sizeof(pair)

    // size of pivots in Bytes
    // Number of keys = Block_size/sizeof(key)
    // Pivot size = ((Number of keys)^Epsilon) * sizeof(key)
    static const int PIVOT_SIZE = int(pow(NUM_DATA_PAIRS, EPSILON)) * UNIT_SIZE;

    // size of Buffer in Bytes
    static constexpr int BUFFER_SIZE = DATA_SIZE - PIVOT_SIZE;
    static constexpr int NUM_UPSERTS = (BUFFER_SIZE - sizeof(int)) / sizeof(std::pair<key_type, value_type>);

    static constexpr int S = (PIVOT_SIZE - sizeof(key_type)) / (sizeof(uint) + sizeof(key_type));
    // number of pivots for every node in the tree
    static constexpr int NUM_PIVOTS = S;

    // number of children for every node
    static constexpr int NUM_CHILDREN = NUM_PIVOTS + 1;

// internal node flush threshold
// the internal node will flush elements (equal to flush threshold)
// of its buffer once full
    static constexpr int FLUSH_LIMIT = NUM_UPSERTS;
    static constexpr int LEAF_FLUSH_LIMIT = NUM_UPSERTS;

    static constexpr int BLOCKS_IN_MEMORY = 500000;
};

struct BeTraits {
    int internal_flushes = 0;
    int leaf_flushes = 0;
    int internal_splits = 0;
    int leaf_splits = 0;
    int num_blocks = 0;
};

template<typename key_type, typename value_type>
struct compare_pair_kv {
    bool operator()(const std::pair<key_type, value_type> &value, const key_type &key) {
        return std::less<key_type>()(value.first, key);
    }

    bool operator()(const key_type &key, const std::pair<key_type, value_type> &value) {
        return std::less<key_type>()(key, value.first);
    }
};

// Buffer for the tree that holds the elements until full
// will flush a batch of it (equal to flush_size)
// size holds the current number of elements in the buffer
template<typename key_type, typename value_type, typename knobs = BeTree_Default_Knobs<key_type, value_type>>
struct Buffer {
    int size;
    std::pair<key_type, value_type> buffer[knobs::NUM_UPSERTS];

    Buffer() {
        size = 0;
    }
};

// Structure that holds the data in the tree leaves
// size signifies the current number of data pairs in the leaf
template<typename key_type, typename value_type, typename knobs = BeTree_Default_Knobs<key_type, value_type>>
struct Data {
    int size;
    std::pair<key_type, value_type> data[knobs::NUM_DATA_PAIRS];

    Data() {
        size = 0;
    }
};

#endif //BETREE_UTILS_H
