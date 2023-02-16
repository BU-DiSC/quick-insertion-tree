#ifndef BAMBOOFILTER_H_
#define BAMBOOFILTER_H_

#include <cmath>
#include <vector>

#include "segment.h"
#include "hash.h"

class BambooFilter {
public:
    const uint32_t INIT_TABLE_BITS;
    uint32_t num_table_bits_;

    std::vector<Segment *> hash_table_;

    uint32_t split_condition_;

    uint32_t next_split_idx_;
    uint32_t num_items_;

    static inline uint32_t BucketIndexHash(uint32_t index) {
        return index & ((1 << BUCKETS_PER_SEG) - 1);
    }

    inline uint32_t SegIndexHash(uint32_t index) const {
        return index & ((1 << NUM_SEG_BITS) - 1);
    }

    static inline uint32_t TagHash(uint32_t tag) {
        return tag & FINGUREPRINT_MASK;
    }

    inline void GenerateIndexTagHash(const void *item, size_t size, uint32_t &seg_index, uint32_t &bucket_index,
                                     uint32_t &tag) const {
        const uint32_t hash = hash::MurmurHash2(item, size, 3);

        bucket_index = BucketIndexHash(hash);
        seg_index = SegIndexHash(hash >> BUCKETS_PER_SEG);
        tag = TagHash(hash >> INIT_TABLE_BITS);

        if (!(tag)) {
            if (num_table_bits_ > INIT_TABLE_BITS) {
                seg_index |= (1 << (INIT_TABLE_BITS - BUCKETS_PER_SEG));
            }
            tag++;
        }

        if (seg_index >= hash_table_.size()) {
            seg_index = seg_index - (1 << (NUM_SEG_BITS - 1));
        }
    }

public:
    BambooFilter(uint32_t capacity, uint32_t split_condition_param);

    ~BambooFilter();

    bool Insert(const void *key, size_t size);

    bool Lookup(const void *key, size_t size) const;

    bool Delete(const void *key, size_t size);

    void Extend();

    void Compress();
};

BambooFilter::BambooFilter(uint32_t capacity, uint32_t split_condition_param)
        : INIT_TABLE_BITS((uint32_t) ceil(log2(capacity / 4))) {
    num_table_bits_ = INIT_TABLE_BITS;

    for (int num_segment = 0; num_segment < (1 << NUM_SEG_BITS); num_segment++) {
        hash_table_.push_back(new Segment(1 << BUCKETS_PER_SEG));
    }

    split_condition_ = uint32_t(split_condition_param * 4 * (1 << BUCKETS_PER_SEG)) - 1;
    next_split_idx_ = 0;
    num_items_ = 0;
}

BambooFilter::~BambooFilter() {
    for (auto &segment_idx: hash_table_) {
        delete segment_idx;
    }
}

bool BambooFilter::Insert(const void *key, size_t size) {
    uint32_t seg_index, bucket_index, tag;

    GenerateIndexTagHash(key, size, seg_index, bucket_index, tag);

    hash_table_[seg_index]->Insert(bucket_index, tag);

    num_items_++;

    if (!(num_items_ & split_condition_)) {
        Extend();
    }

    return true;
}

bool BambooFilter::Lookup(const void *key, size_t size) const {
    uint32_t seg_index, bucket_index, tag;

    GenerateIndexTagHash(key, size, seg_index, bucket_index, tag);
    return hash_table_[seg_index]->Lookup(bucket_index, tag);
}

bool BambooFilter::Delete(const void *key, size_t size) {
    uint32_t seg_index, bucket_index, tag;
    GenerateIndexTagHash(key, size, seg_index, bucket_index, tag);

    if (hash_table_[seg_index]->Delete(bucket_index, tag)) {
        num_items_--;
        if (!(num_items_ & split_condition_)) {
            Compress();
        }
        return true;
    } else {
        return false;
    }
}

void BambooFilter::Extend() {
    Segment *src = hash_table_[next_split_idx_];
    auto *dst = new Segment(*src);
    hash_table_.push_back(dst);

    auto num_seg_bits_ = (uint32_t) ceil(log2(hash_table_.size()));
    num_table_bits_ = num_seg_bits_ + BUCKETS_PER_SEG;

    src->EraseEle(true, ACTV_TAG_BIT - 1);
    dst->EraseEle(false, ACTV_TAG_BIT - 1);

    next_split_idx_++;
    if (next_split_idx_ == (1UL << (num_seg_bits_ - 1))) {
        next_split_idx_ = 0;
    }
}

void BambooFilter::Compress() {
    auto num_seg_bits_ = (uint32_t) ceil(log2(hash_table_.size() - 1));
    num_table_bits_ = num_seg_bits_ + BUCKETS_PER_SEG;
    if (!next_split_idx_) {
        next_split_idx_ = (1UL << (num_seg_bits_ - 1));
    }
    next_split_idx_--;

    Segment *src = hash_table_[next_split_idx_];
    Segment *dst = hash_table_.back();
    src->Absorb(dst);
    delete dst;
    hash_table_.pop_back();
}

#endif
