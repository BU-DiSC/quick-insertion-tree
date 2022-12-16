#ifndef BETREE_H
#define BETREE_H

#include <algorithm>
#include <ostream>

#include "block_manager.h"
#include "betree_utils.h"

// structure that holds all stats for the tree

// class that defines the B Epsilon tree Node
template<typename key_type, typename value_type, typename knobs = BeTree_Default_Knobs<key_type, value_type>, typename compare = std::less<key_type>>
class BeNode {

    uint id;
    // boolean flag if leaf
    bool *is_leaf;

    // boolean flag if root
    bool *is_root;

    // parent of node (node id)
    uint *parent;

    // counter for no. of pivots
    int *pivots_ctr;

    // node's buffer
    Buffer<key_type, value_type, knobs> *buffer;

    // // node's child pointer keys

    // // node's pivots/children
    // node's child pointer keys
    key_type *child_key_values;

    // node's pivots/children
    // pivot_pointers will now be node id's rather than actual nodes
    uint *pivot_pointers;

    // data pairs for the node if leaf
    Data<key_type, value_type, knobs> *data;

    // next node pointer (node id)
    uint *next_node;

    BlockManager *manager;

public:
    // opens the node from disk/memory for access
    void open() {
        bool miss = false;
        Deserialize(manager->internal_memory[manager->OpenBlock(id, miss)]);
        if (miss) {
            if (*is_leaf)
                manager->addLeafCacheMisses();
            else
                manager->addInternalCacheMisses();
        } else {
            if (*is_leaf)
                manager->addLeafCacheHits();
            else
                manager->addInternalCacheHits();
        }
    }

    void setToId(uint _id) {
        id = _id;
        open();
    }

    // Parameterized constructor
    /**
     *  returns: N/A
     *  Function: creates a new node set to the given id
     *              and parent as set.
     */
    BeNode(BlockManager *_manager, uint _id, uint _parent, bool _is_leaf, bool _is_root, uint _next_node) {
        manager = _manager;
        id = _id;
        parent = nullptr;
        is_leaf = nullptr;
        is_root = nullptr;
        buffer = nullptr;
        data = nullptr;
        next_node = nullptr;
        child_key_values = nullptr;
        pivot_pointers = nullptr;
        pivots_ctr = nullptr;
        open();

        // set parent now
        *parent = _parent;
        *is_leaf = _is_leaf;
        *is_root = _is_root;
        *next_node = _next_node;
    }

    // constructor used when we know that the node exists in disk
    /**
     *  returns: N/A
     *  Function: initializes a node to the given id and retrieves from disk.
     */
    BeNode(BlockManager *_manager, uint _id) {
        manager = _manager;
        id = _id;
        parent = nullptr;
        is_leaf = nullptr;
        is_root = nullptr;
        buffer = nullptr;
        data = nullptr;
        next_node = nullptr;
        child_key_values = nullptr;
        pivot_pointers = nullptr;
        pivots_ctr = nullptr;
        if (id != 0)
            open();
    }

    bool isLeaf() {
        open();
        return *is_leaf;
    }

    bool isRoot() {
        open();
        return *is_root;
    }

    void setLeaf(bool _is_leaf) {
        open();
        *is_leaf = _is_leaf;
        manager->addDirtyNode(id);
    }

    void setRoot(bool _is_root) {
        open();
        *is_root = _is_root;
        manager->addDirtyNode(id);
    }

    void setNextNode(uint *_next_node) {
        open();
        next_node = _next_node;
        manager->addDirtyNode(id);
    }

    void setNextNode(uint next_node_id) {
        open();
        *next_node = next_node_id;
        manager->addDirtyNode(id);
    }

    uint *getNextNode() {
        open();
        return next_node;
    }

    /**
     *  returns: index of child slot
     *  Function finds suitable index where key can be placed
     *  in the current node (identifies the child)
     */
    uint slotOfKey(key_type key) {
        // make sure that the caller is an internal node
        open();
        assert(!*is_leaf);

        // we have k pointers and k+1 pivots
        // where the first pivot will be for any key
        // X<= pointer[0] and last pivot will be for
        // any key X > pointer[k-1] (this will be pivot[k])

        uint lo = 0, hi = getPivotsCtr() - 1;

        while (lo < hi) {
            uint mid = (lo + hi) >> 1;

            if (key <= child_key_values[mid]) {
                hi = mid; // key <= mid
            } else {
                lo = mid + 1; // key > mid
            }
        }

        return lo;
    }

    /**
     *  returns: true or false indicating a split
     *  Function: inserts [num] data pairs in leaf. If leaf
     *  becomes full, returns true - indicating a split. Else
     *  returns false.
     */
    bool insertInLeaf(std::pair<key_type, value_type> buffer_elements[], int &num) {
        // make sure that caller node is a leaf
        open();
        assert(*is_leaf);

        // set node as dirty
        manager->addDirtyNode(id);

        // we want to maintain all elements in the leaf in a sorted order
        // since existing elements will be sorted and the buffer elements will also come
        // in a sorted order, we can simply merge the two arrays

        int i = data->size - 1;
        int j = num - 1;
        int last_index = data->size + num - 1;

        while (j >= 0) {
            if (i >= 0) {
                if (data->data[i] > buffer_elements[j]) {
                    // copy element
                    data->data[last_index] = data->data[i];
                    i--;
                } else {
                    data->data[last_index] = buffer_elements[j];
                    j--;
                }
            } else {
                data->data[last_index] = buffer_elements[j];
                j--;
            }
            last_index--;
        }

        data->size += num;

        // check if after adding, the leaf  ` has exceeded limit and
        // return accordingly
        return data->size >= knobs::NUM_DATA_PAIRS;
    }

    bool insertInLeaf(std::pair<key_type, value_type> element) {
        open();
        assert(*is_leaf);
        // set node as dirty
        manager->addDirtyNode(id);

        if (data->size > 0)
            assert(element >= data->data[data->size - 1]);

        data->data[data->size++] = element;

        // check if after adding, the leaf has exceeded limit and return accordingly
        return data->size >= knobs::NUM_DATA_PAIRS;
    }

    /**
     * Swap the max key that is greater than the @key. This function can only be called by the BeTree::swap_in_tail_leaf
     * currently. It is used by the "lazy move" strategy of dual tree system, thus there must be a key that is greater
     * than @key
     *
     * @param key: The key that will be inserted into the node.
     * @param val: The corresponding value of the key.
     * @param is_max: An indicator, set to true when the swapped key is the maximum key of the node.
     * @param is_min: An indicator, play a same role as @is_max.
     *
     * @return The swapped key and its value.
    */
    std::pair<key_type, value_type> swap_in_leaf(key_type key, value_type val) {
        // When using this method, no need to consider splitting of leaf node.
        open();

        assert(*is_leaf);

        manager->addDirtyNode(id);
        // Search the first key that is greater than the inserted key.
        int max_pos = 0;
        key_type max_key = data->data[0].first;
        for (int i = 1; i < data->size; i++) {
            if (data->data[i].first > max_key) {
                max_pos = i;
                max_key = data->data[i].first;
            }
        }
        if (max_key > key) {
            auto ret = data->data[max_pos];
            data->data[max_pos] = {key, val};
            return ret;
        }
        return std::pair<key_type, value_type>(key, val);
    }

    key_type max_key() {
        open();
        assert(*is_leaf);
        key_type max_key = data->data[0].first;
        for (int i = 1; i < data->size; i++) {
            if (data->data[i].first > max_key) {
                max_key = data->data[i].first;
            }
        }
        return max_key;
    }

    /**
     *  returns: new node id
     *  Function: splits leaf into two
     */
    uint splitLeaf(key_type &split_key, BeTraits &traits, uint &new_id, float split_frac = 0.5) {
        // make sure that caller is a leaf node
        open();
        assert(*is_leaf);
        // set node as dirty
        manager->addDirtyNode(id);

        new_id = manager->allocate();
        // create new node
        BeNode<key_type, value_type, knobs, compare> new_sibling(manager, new_id);
        new_sibling.setParent(*parent);
        new_sibling.setLeaf(true);
        new_sibling.setRoot(false);
        new_sibling.setNextNode(*next_node);

        manager->addDirtyNode(new_id);

        traits.num_blocks++;

        // start moving data pairs
        int start_index = split_frac * data->size;
        for (int i = start_index; i < data->size; i++) {
            new_sibling.data->data[new_sibling.data->size++] = data->data[i];
        }

        // update size of old node
        data->size -= new_sibling.data->size;

        if (split_frac <= 0.5) {
            assert(data->size <= new_sibling.data->size);
        } else {
            assert(data->size >= new_sibling.data->size);
        }

        // change current node's next node to new_node
        assert(new_sibling.getId() != 0);
        *next_node = new_sibling.getId();

        new_sibling.setParent(*parent);

        // split_key becomes lower bound of newly added sibling's keys
        split_key = data->data[data->size - 1].first;
        return new_id;
    }

    /**
     *  returns: node whose b
     *  Function: splits internal node into two. Distributes
     *              buffer and pivots as required
     */
    uint splitInternal(key_type &split_key, BeTraits &traits, uint &new_id, float split_frac = 0.5) {
        open();
        // make sure that caller is not a leaf node
        assert(!*is_leaf);

        manager->addDirtyNode(id);

        // make sure that we split only when we have hit the
        // pivot capacity
        assert(getPivotsCtr() == knobs::NUM_PIVOTS);

        // create a new node (blockid, parent = this->parent, is_leaf = false)
        new_id = manager->allocate();
        BeNode<key_type, value_type, knobs, compare> new_node(manager, new_id, *parent, false, false, *next_node);
        traits.num_blocks++;

        manager->addDirtyNode(new_id);

        BeNode temp_mover(manager, new_id);

        // move half the pivots to the new node
        int start_index = (getPivotsCtr()) * split_frac;

        for (int i = start_index; i < getPivotsCtr(); i++) {
            open();
            new_node.open();
            // move all child keys
            if (i < getPivotsCtr() - 1) {
                new_node.child_key_values[i - start_index] = child_key_values[i];
            }
            // move all pointers
            new_node.pivot_pointers[i - start_index] = pivot_pointers[i];
            new_node.setPivotCounter(new_node.getPivotsCtr() + 1);

            // change the parent node for the pivots
            temp_mover.setToId(pivot_pointers[i]);
            *temp_mover.parent = new_node.getId();
            manager->addDirtyNode(temp_mover.getId());
        }

        // reset pivots counter for old node
        setPivotCounter(getPivotsCtr() - new_node.getPivotsCtr());

        // split key is the first pointer of new node's children
        // alternatively, it is the pointer at pivots_ctr in the old node
        // since those have not been destroyed but the counter has been decreased
        split_key = getChildKey(getPivotsCtr() - 1);

        // move buffer elements to new node as required
        // create a temp buffer that will later replace the old buffer with elements removed
        auto *temp = new Buffer<key_type, value_type, knobs>();
        std::pair<key_type, value_type> empty_pair = temp->buffer[0];
        for (int i = 0; i < buffer->size; i++) {
            if (buffer->buffer[i].first >= split_key) {
                new_node.buffer->buffer[new_node.buffer->size] = buffer->buffer[i];
                new_node.buffer->size++;
            } else {
                temp->buffer[temp->size] = buffer->buffer[i];
                temp->size++;
            }
        }

        // copy temp to buffer
        for (int i = 0; i < buffer->size; i++) {
            if (i < temp->size) {
                buffer->buffer[i] = temp->buffer[i];
            } else {
                buffer->buffer[i] = empty_pair;
            }
        }

        buffer->size = temp->size;

        delete temp;

        manager->addDirtyNode(id);

        *next_node = new_id;

        new_node.setParent(*parent);
        // return the newly created node
        return new_id;
    }

    void prepare_for_flush(uint &chosen_child, int &num_to_flush, std::pair<key_type, value_type> *&elements_to_flush) {
        open();
        assert(!*is_leaf);

        if (isRoot()) {
            std::sort(buffer->buffer, buffer->buffer + buffer->size);
            manager->addDirtyNode(id);
        }

        // find no. of elements that can be flushed to each child
        int num_elements[knobs::NUM_CHILDREN];
        memset(num_elements, 0, sizeof(num_elements));

        uint *buffer_spots = new uint[buffer->size];
        uint max_slot = 0;

        for (int i = 0; i < buffer->size; i++) {
            uint s = slotOfKey(buffer->buffer[i].first);
            num_elements[s]++;
            buffer_spots[i] = s;

            if (s != max_slot) {
                if (num_elements[s] > num_elements[max_slot])
                    max_slot = s;
            }
        }

        chosen_child = max_slot;

        BeNode<key_type, value_type, knobs, compare> child(manager, pivot_pointers[chosen_child]);

        int available_spots = knobs::NUM_UPSERTS - child.getBufferSize();

        int flush_limit = knobs::FLUSH_LIMIT;

        if (child.isLeaf()) {
            flush_limit = knobs::LEAF_FLUSH_LIMIT;
            available_spots = knobs::NUM_DATA_PAIRS - child.getDataSize();
        }
        assert(available_spots > 0);

        elements_to_flush = new std::pair<key_type, value_type>[flush_limit];
        // create a temp buffer that will later replace the current one
        // we will extract elements that need to be flushed into elements_to_flush
        // remaining elements will go into the temp buffer and will then replace
        // the original buffer

        auto *temp = new Buffer<key_type, value_type, knobs>();
        for (int i = 0; i < buffer->size; i++) {
            // if (slotOfKey(buffer->buffer[i].first) == chosen_child)
            if (buffer_spots[i] == chosen_child) {
                // check if we are exceeding threshold
                // we are fine till the time we are less than the threshold limit,
                // and we are less than available spots in the child's buffer
                if ((num_to_flush < flush_limit) && (num_to_flush < available_spots)) {
                    elements_to_flush[num_to_flush] = buffer->buffer[i];
                    num_to_flush++;

                    // we can now move to the next iteration
                    continue;
                }
            }

            temp->buffer[temp->size] = buffer->buffer[i];
            temp->size++;
        }
        delete[] buffer_spots;
#ifdef PROFILE
        auto start_replace = std::chrono::high_resolution_clock::now();
#endif
        // copy temp to buffer
        memcpy(buffer->buffer, temp->buffer, temp->size * sizeof(buffer->buffer[0]));

        buffer->size = temp->size;

        manager->addDirtyNode(id);

        delete temp;
    }

    /**
     *  returns: true or false based on split or no split
     *  Function: flushes element of internal node buffer to its child leaf.
     *              If exceeding capacity of leaf, it splits
     */
    bool
    flushLeaf(BeNode<key_type, value_type, knobs, compare> &child, std::pair<key_type, value_type> *elements_to_flush,
              int &num_to_flush, key_type &split_key, uint &new_node_id, BeTraits &traits, float split_frac = 0.5) {
        // make sure caller is not a leaf node
        open();
        assert(!*is_leaf);

        // set node as dirty
        manager->addDirtyNode(id);

        child.open();
        manager->addDirtyNode(child.getId());

        // make sure child is a leaf node
        assert(child.isLeaf());

        int init_data_size = child.getDataSize();
        // flush all elements to the leaf
        if (child.insertInLeaf(elements_to_flush, num_to_flush)) {
            // require a split operation
            new_node_id = child.splitLeaf(split_key, traits, new_node_id, split_frac);

            assert(data->size <= knobs::NUM_DATA_PAIRS);

            return true;
        }

        assert(child.getDataSize() == init_data_size + num_to_flush);
        // update trait characteristic
        // we have inserted and do not require a split so return false
        return false;
    }

    /**
     *  returns: true or false indicating buffer exceeding capacity or not
     *  Function: flushes elements of buffer from internal node to its
     *              child internal node.
     */
    bool flushInternal(BeNode<key_type, value_type, knobs, compare> &child,
                       std::pair<key_type, value_type> *elements_to_flush, int &num_to_flush) {
        // make sure caller is not a leaf node
        open();
        assert(!*is_leaf);

        child.open();

        // set node as dirty
        manager->addDirtyNode(id);
        manager->addDirtyNode(child.getId());

        // make sure child is not a leaf node
        assert(!child.isLeaf());

        // add all elements to child's buffer
        // we want to maintain all elements in the buffer in a sorted order
        // since existing elements will be sorted and the buffer elements will also come
        // in a sorted order, we can simply merge the two arrays

        int i = child.buffer->size - 1;
        int j = num_to_flush - 1;
        int last_index = child.buffer->size + num_to_flush - 1;

        while (j >= 0) {
            if (i >= 0 && child.buffer->buffer[i] > elements_to_flush[j]) {
                // copy element
                child.buffer->buffer[last_index] = child.buffer->buffer[i];
                i--;
            } else {
                child.buffer->buffer[last_index] = elements_to_flush[j];
                j--;
            }
            last_index--;
        }

        child.buffer->size += num_to_flush;

        // return true or false based on whether we exceeded capacity
        // if returning true, the caller for this function will have to
        // perform a subsequent flush operation at least for one more level
        return child.buffer->size >= knobs::NUM_UPSERTS;
    }

    /**
     *  returns: SPLIT or NOSPLIT
     *  Function: flushes a level of a tree from a node. If the internal node flush
     *  resulted in exceeding capacity, flushes a level again from that internal node
     */
    Result flushLevel(key_type &split_key, uint &new_node_id, BeTraits &traits, float split_frac = 0.5) {
        open();

        int num_to_flush = 0;
        uint chosen_child_idx = 0;

        // prepare current buffer for flush. Fetch elements required for flush from current buffer
        // and stores in elements_to_flush. It will also decrease the existing buffer after removing
        // elements to flush
        std::pair<key_type, value_type> *elements_to_flush = NULL;
        prepare_for_flush(chosen_child_idx, num_to_flush, elements_to_flush);

        assert(buffer->size <= knobs::NUM_UPSERTS);

        // fetch the child node that has been chosen to flush to
        BeNode<key_type, value_type, knobs, compare> child(manager, pivot_pointers[chosen_child_idx]);
        child.open();
        manager->addDirtyNode(id);

        Result res = NOSPLIT;

        if (child.isLeaf()) {

            Result flag = flushLeaf(child, elements_to_flush, num_to_flush, split_key, new_node_id, traits, split_frac)
                          ? SPLIT : NOSPLIT;

            if (flag == SPLIT) {
                traits.leaf_splits++;
            }

            traits.leaf_flushes++;

            delete[] elements_to_flush;
            return flag;
        }

        traits.internal_flushes++;
        if (flushInternal(child, elements_to_flush, num_to_flush)) {
            res = child.flushLevel(split_key, new_node_id, traits, split_frac);
        }

        // if flushInternal was a success and buffer didn't exceed capacity
        delete[] elements_to_flush;
        return res;
    }

    /**
     *  returns: true or false
     *  Function: adds a pivot to a node. Returns true if after adding the pivot,
     *          exceeds capacity for the node
     */
    bool addPivot(key_type &split_key, uint &new_node_id) {
        open();
        assert(!*is_leaf);

        // set node as dirty
        manager->addDirtyNode(id);

        uint node_position = slotOfKey(split_key);

        for (uint i = getPivotsCtr() - 1; i >= node_position; i--) {
            pivot_pointers[i + 2] = pivot_pointers[i + 1];
            child_key_values[i + 1] = child_key_values[i];
        }

        child_key_values[node_position] = split_key;
        pivot_pointers[node_position + 1] = new_node_id;
        setPivotCounter(getPivotsCtr() + 1);

        return getPivotsCtr() == knobs::NUM_PIVOTS;
    }

    bool insertInBuffer(key_type key, value_type value) {
        open();

        std::pair<key_type, value_type> new_insert(key, value);
        buffer->buffer[buffer->size] = new_insert;
        buffer->size++;

        // set node as dirty
        manager->addDirtyNode(id);

        return buffer->size >= knobs::NUM_UPSERTS;
    }

    bool query(key_type key, const BeTraits &traits) {
        open();

        // if current node is a leaf
        // search all data pairs
        if (*is_leaf) {
            // perform binary search
            bool found = std::binary_search(data->data, data->data + data->size, key,
                                            compare_pair_kv<key_type, value_type>());
            return found;
        }

        // if current node is not a leaf, first check the buffer

        // binary search on buffer
        bool found = std::binary_search(buffer->buffer, buffer->buffer + buffer->size, key,
                                        compare_pair_kv<key_type, value_type>());
        if (found)
            return true;

        // if not found in buffer, we need to search its pivots
        int chosen_child_idx = slotOfKey(key);
        BeNode<key_type, value_type, knobs, compare> child(manager, pivot_pointers[chosen_child_idx]);

        return child.query(key, traits);
    }

    uint getId() {
        open();
        return id;
    }

    int getBufferSize() {
        open();
        return buffer->size;
    }

    int getDataSize() {
        open();
        return data->size;
    }

    int getPivotsCtr() {
        open();
        return *pivots_ctr;
    }

    uint getParent() {
        open();
        return *parent;
    }

    std::pair<key_type, value_type> getLastDataPair() {
        open();
        assert(*is_leaf);

        return data->data[data->size - 1];
    }

    void setPivotCounter(int _size) {
        open();
        *pivots_ctr = _size;
        manager->addDirtyNode(id);
    }

    void setParent(uint _parent) {
        open();
        *parent = _parent;
        manager->addDirtyNode(id);
    }

    void setChildKey(key_type child_key, int slot) {
        open();
        assert(slot >= 0);

        child_key_values[slot] = child_key;
        manager->addDirtyNode(id);
    }

    void setPivot(uint pivot_node_id, int slot) {
        open();
        assert(slot >= 0 && slot < knobs::NUM_PIVOTS + 1);

        pivot_pointers[slot] = pivot_node_id;
        manager->addDirtyNode(id);
    }

    key_type getChildKey(int slot) {
        open();
        assert(slot >= 0);

        return child_key_values[slot];
    }

    key_type getDataPairKey(int slot) {
        open();
        assert(slot >= 0);

        return data->data[slot].first;
    }

    long long getSumKeys() const {
        long long sum_keys = 0;
        for (int i = 0; i < data->size; i++) {
            sum_keys += (long long) data->data[i].first;
        }
        return sum_keys;
    }

    long long getSumSquares() const {
        long long sum_squares = 0;
        for (int i = 0; i < data->size; i++) {
            sum_squares += static_cast<long long>(data->data[i].first) * data->data[i].first;
        }
        return sum_squares;
    }

    void fanout(int &num, int &total, int &max, int &min, int *arr, int &internal) {
        open();
        uint orig_id = id;
        if (!*is_leaf) {
            internal++;
            total += getPivotsCtr();
            arr[num] = getPivotsCtr();
            num++;

            if (getPivotsCtr() >= max)
                max = getPivotsCtr();

            if (getPivotsCtr() <= min)
                min = getPivotsCtr();

            for (int i = 0; i < getPivotsCtr(); i++) {
                uint curr_id = id;
                setToId(pivot_pointers[i]);
                fanout(num, total, max, min, arr, internal);
                setToId(curr_id);
            }
            setToId(orig_id);
        }
    }

    double buffer_occupancy(int &num, int &total, int &max, int &min, int &empty, int *arr) {
        open();
        uint orig_id = id;
        if (!*is_leaf) {
            total += buffer->size;
            arr[num] = buffer->size;
            num++;

            if (buffer->size >= max)
                max = buffer->size;

            if (buffer->size <= min)
                min = buffer->size;

            if (buffer->size == 0)
                empty++;

            for (int i = 0; i < getPivotsCtr(); i++) {
                uint curr_id = id;
                setToId(pivot_pointers[i]);
                buffer_occupancy(num, total, max, min, empty, arr);
                setToId(curr_id);
            }
            setToId(orig_id);
        }

        return total / static_cast<double>(num);
    }

    int depth() {
        open();
        uint orig_id = id;
        int d = 0;
        if (*is_leaf)
            return 0;

        for (int i = 0; i < getPivotsCtr(); i++) {
            setToId(pivot_pointers[i]);
            d = std::max(d, depth());
        }
        setToId(orig_id);
        return d + 1;
    }

    int get_leaf_occupancy() {
        open();
        assert(*is_leaf);

        return data->size;
    }

    void Deserialize(Block &disk_store) {
        is_leaf = reinterpret_cast<bool *>(disk_store.block_buf);
        is_root = reinterpret_cast<bool *>(disk_store.block_buf + sizeof(bool));
        parent = reinterpret_cast<uint *>(disk_store.block_buf + sizeof(bool) + sizeof(bool));
        next_node = reinterpret_cast<uint *>(disk_store.block_buf + sizeof(bool) + sizeof(bool) + sizeof(uint));
        pivots_ctr = reinterpret_cast<int *>(disk_store.block_buf + sizeof(bool) + sizeof(bool) + sizeof(uint) +
                                             sizeof(uint));

        // every node has either a buffer or data. So essentially, both are in the same place
        data = reinterpret_cast<Data<key_type, value_type, knobs> *>(disk_store.block_buf + sizeof(bool) +
                                                                     sizeof(bool) + sizeof(uint) +
                                                                     sizeof(uint) + sizeof(int));
        buffer = reinterpret_cast<Buffer<key_type, value_type, knobs> *>(disk_store.block_buf + sizeof(bool) +
                                                                         sizeof(bool) + sizeof(uint) +
                                                                         sizeof(uint) + sizeof(int));

        child_key_values = reinterpret_cast<key_type *>(disk_store.block_buf + sizeof(uint) + sizeof(bool) +
                                                        sizeof(bool) + sizeof(uint) + sizeof(int) +
                                                        sizeof(Buffer<key_type, value_type, knobs>));

        int num = knobs::NUM_CHILDREN;

        pivot_pointers = reinterpret_cast<uint *>(disk_store.block_buf + sizeof(uint) + sizeof(bool) + sizeof(bool) +
                                                  sizeof(uint) + sizeof(int) +
                                                  sizeof(Buffer<key_type, value_type, knobs>) +
                                                  (num * sizeof(key_type)));

        assert(*is_leaf == true || *is_leaf == false);
        assert(*is_root == true || *is_root == false);
        assert(*pivots_ctr >= 0);
        assert(*parent >= 0);
        assert(*next_node >= 0);
    }
};

#include "collection.h"

template<typename key_type, typename value_type, typename knobs = BeTree_Default_Knobs<key_type, value_type>, typename compare = std::less<key_type>>
class BeTree : public collection<key_type, value_type> {
    void add(const key_type &key, const value_type &value) override {
        insert(key, value);
    }

    bool contains(const key_type &key) override {
        return query(key);
    }

    std::ostream &get_stats(std::ostream &os) const override {
        os << depth() << ", " << getNumWrites();
        return os;
    }

    BlockManager *manager;

    BeNode<key_type, value_type, knobs, compare> *root;

    BeNode<key_type, value_type, knobs, compare> *tail_leaf;

    BeNode<key_type, value_type, knobs, compare> *prev_tail;

    BeNode<key_type, value_type, knobs, compare> *head_leaf;

    uint tail_leaf_id;

    key_type max_key;

    float split_frac;

    BeTraits traits;

public:
    BeTree(const std::string &_name, const std::string &_rootDir, unsigned long long _size_of_each_block,
           uint _blocks_in_memory, float split_frac = 0.5) : tail_leaf(nullptr), head_leaf(nullptr),
                                                             split_frac(split_frac) {
        manager = new BlockManager(_name, _rootDir, _size_of_each_block, _blocks_in_memory);

        uint root_id = manager->allocate();
        root = new BeNode<key_type, value_type, knobs, compare>(manager, root_id);
        root->setRoot(true);
        root->setLeaf(true);

        tail_leaf_id = root_id;

        max_key = {};
    }

    ~BeTree() {
        delete root;
        delete manager;
    }

    long long getSumKeys() const {
        // return the sum of keys in the previous tail leaf
        assert(prev_tail != nullptr);
        return prev_tail->getSumKeys();
    }

    long long getSumSquares() const {
        // return the sum of squared keys in the previous tail leaf
        assert(prev_tail != nullptr);
        return prev_tail->getSumSquares();
    }

    key_type get_minimum_key_of_tail_leaf() {
        assert(tail_leaf != nullptr);
        return tail_leaf->getDataPairKey(0);
    }

    int depth() const {
        return root->depth();
    }

    int getPrevTailSize() {
        assert(prev_tail != nullptr);
        return prev_tail->getDataSize();
    }

    unsigned long long getNumWrites() const { return manager->num_writes; }

    bool more_than_one_leaf() {
        return head_leaf && head_leaf != tail_leaf;
    }

    bool is_tail_leaf_empty() {
        return tail_leaf->get_leaf_occupancy() == 0;
    }

    key_type get_prev_tail_maximum_key() {
        assert(prev_tail != nullptr);
        return prev_tail->getLastDataPair().first;
    }

    bool insert(key_type key, value_type value) {
        // if root is a leaf node, we insert in leaf until it exceeds capacity
        root->open();
        manager->addDirtyNode(root->getId());
        if (root->isLeaf()) {
            std::pair<key_type, value_type> elements_to_insert[] = {std::pair<key_type, value_type>(key, value)};
            int num_to_insert = 1;
            bool flag = root->insertInLeaf(elements_to_insert, num_to_insert);
            manager->addDirtyNode(root->getId());
            uint new_id;
            if (tail_leaf == nullptr || tail_leaf == nullptr) {
                tail_leaf = root;
                head_leaf = root;
                tail_leaf_id = root->getId();
            }

            if (root->getDataSize() == 1) {
                max_key = key;
            }

            // if flag returns true, it means we need to split the current leaf (actually the root)
            // once split, we create a new root that points to these two leaves
            if (flag) {
                key_type split_key_new;
                root->splitLeaf(split_key_new, traits, new_id, split_frac);
                BeNode<key_type, value_type, knobs, compare> new_leaf(manager, new_id);
                traits.leaf_splits++;

                // create new root node
                uint new_root_id = manager->allocate();
                auto *new_root = new BeNode<key_type, value_type, knobs, compare>(manager, new_root_id);
                new_root->setRoot(true);
                new_root->setChildKey(split_key_new, 0);
                new_root->setPivot(root->getId(), 0);
                new_root->setPivot(new_leaf.getId(), 1);
                new_root->setPivotCounter(new_root->getPivotsCtr() + 2);

                manager->addDirtyNode(new_root_id);

                // change old root
                root->setRoot(false);

                // set parents
                new_leaf.setParent(new_root->getId());
                root->setParent(new_root->getId());

                manager->addDirtyNode(root->getId());
                manager->addDirtyNode(new_leaf.getId());

                assert(root->getDataSize() <= knobs::NUM_DATA_PAIRS);

                // initially, the root is the head_leaf and tail_leaf. Now that we split, we need to update tail_leaf
                tail_leaf_id = new_leaf.getId();
                tail_leaf->setToId(new_leaf.getId());

                root = new_root;
            }

            return true;
        }

        if (root->insertInBuffer(key, value)) {
            // buffer became full, so we need to flush at root level

            key_type split_key;
            uint new_node_id = 0;

            Result result = root->flushLevel(split_key, new_node_id, traits, split_frac);
            manager->addDirtyNode(root->getId());
            BeNode<key_type, value_type, knobs, compare> new_node(manager, new_node_id);

            bool flag;
            while (true) {
                if (result == SPLIT) {

                    // add pivot
                    BeNode<key_type, value_type, knobs, compare> child_parent(manager, new_node.getParent());
                    flag = child_parent.addPivot(split_key, new_node_id);

                    // since the result was a split, we check if  new_node's id matches with tail_leaf's next_node
                    BeNode<key_type, value_type, knobs, compare> tail(manager, tail_leaf_id);
                    if (*tail.getNextNode() == new_node.getId()) {
                        // update tail_leaf_id
                        tail_leaf_id = new_node.getId();
                        tail.setToId(tail_leaf_id);
                        tail_leaf->setToId(tail_leaf_id);
                    }

                    if (!flag) {
                        break;
                    }

                    if (child_parent.isRoot()) {
                        child_parent.splitInternal(split_key, traits, new_node_id, split_frac);
                        BeNode<key_type, value_type, knobs, compare> new_sibling(manager, new_node_id);
                        manager->addDirtyNode(new_node_id);
                        traits.internal_splits++;

                        // create new root
                        uint new_root_id = manager->allocate();
                        auto *new_root = new BeNode<key_type, value_type, knobs, compare>(manager, new_root_id);
                        new_root->setRoot(true);
                        manager->addDirtyNode(new_root_id);

                        new_root->setChildKey(split_key, 0);
                        new_root->setPivot(child_parent.getId(), 0);
                        new_root->setPivot(new_sibling.getId(), 1);
                        new_root->setPivotCounter(new_root->getPivotsCtr() + 2);

                        child_parent.setRoot(false);
                        child_parent.setParent(new_root->getId());
                        manager->addDirtyNode(child_parent.getId());

                        new_sibling.setParent(new_root->getId());
                        manager->addDirtyNode(new_sibling.getId());

                        root = new_root;

                        // counting one I/O for writing old root back to disk as it was modified done later

                        break;
                    }

                    // if flag returned true and child_parent's parent is not the root
                    // we need to split this internal node

                    // we set new_node to the newly split node
                    child_parent.splitInternal(split_key, traits, new_node_id, split_frac);
                    manager->addDirtyNode(child_parent.getId());
                    new_node.setToId(new_node_id);
                    manager->addDirtyNode(new_node_id);
                } else {
                    root->open();
                    break;
                }
            }
        }

        if (key > max_key)
            max_key = key;

        return true;
    }

    bool query(key_type key) const {
        return root->query(key, traits);
    }

    key_type getMaximumKey() {
        return max_key;
    }

    bool insert_to_tail_leaf(key_type key, value_type val, bool append, bool &need_split) {
        need_split = false;
        if (tail_leaf == nullptr) {
            // No tuple has been inserted, the tree is empty, but a root node is created when the
            //constructor was called, so we directly use it.
            BeNode<key_type, value_type, knobs, compare> *leaf = root;
            leaf->setLeaf(true);
            leaf->insertInLeaf(std::pair<key_type, value_type>(key, val));
            manager->addDirtyNode(root->getId());

            head_leaf = leaf;
            tail_leaf = leaf;
            tail_leaf_id = leaf->getId();

            max_key = key;
            return true;
        }
        if (append) {
            need_split = tail_leaf->insertInLeaf(std::pair<key_type, value_type>(key, val));
            max_key = key;
        } else {
            std::pair<key_type, value_type> a[] = {std::pair<key_type, value_type>(key, val)};
            int num_to_insert = 1;
            need_split = tail_leaf->insertInLeaf(a, num_to_insert);
        }
        if (!need_split) {
            // No split is needed
            manager->addDirtyNode(tail_leaf_id);
            return true;
        }
        key_type split_key_leaf = tail_leaf->getDataPairKey(tail_leaf->getDataSize() - 1);
        uint new_leaf_id = manager->allocate();
        tail_leaf->splitLeaf(split_key_leaf, this->traits, new_leaf_id, split_frac);
        traits.leaf_splits++;
        auto *new_leaf = new BeNode<key_type, value_type, knobs, compare>(manager, new_leaf_id);
        new_leaf->setLeaf(true);
        if (root->isLeaf()) {
            // only one node in the tree, the split is simple
            key_type split_key_new = root->getDataPairKey(root->getDataSize() - 1);

            uint new_root_id = manager->allocate();
            auto *new_root = new BeNode<key_type, value_type, knobs, compare>(manager, new_root_id);
            new_root->setRoot(true);

            new_root->setChildKey(split_key_new, 0);
            new_root->setPivot(root->getId(), 0);
            new_root->setPivot(new_leaf->getId(), 1);
            new_root->setPivotCounter(new_root->getPivotsCtr() + 2);
            manager->addDirtyNode(new_root_id);

            root->setRoot(false);
            root->setNextNode(new_leaf->getId());

            // set parents
            new_leaf->setParent(new_root_id);
            root->setParent(new_root_id);

            manager->addDirtyNode(new_leaf->getId());
            manager->addDirtyNode(root->getId());

            root = new_root;

            prev_tail = tail_leaf;
            tail_leaf = new_leaf;
            tail_leaf_id = new_leaf->getId();
        } else {
            // The tree exists and:
            // add the leaf is enough or a split of internal node(s) is needed
            key_type split_key = tail_leaf->getDataPairKey(tail_leaf->getDataSize() - 1);
            uint new_node_id = new_leaf->getId();
            new_leaf->setParent(tail_leaf->getParent());
            tail_leaf->setNextNode(new_leaf->getId());

            manager->addDirtyNode(new_leaf->getId());
            manager->addDirtyNode(tail_leaf->getId());

            prev_tail = tail_leaf;
            tail_leaf = new_leaf;
            tail_leaf_id = new_leaf->getId();

            // reload the newly added node
            BeNode<key_type, value_type, knobs, compare> new_node(manager, new_leaf->getId());
            while (true) {
                BeNode<key_type, value_type, knobs, compare> child_parent(manager, new_node.getParent());
                bool flag = child_parent.addPivot(split_key, new_node_id);
                manager->addDirtyNode(child_parent.getId());
                if (!flag) {
                    // Do not need to split anymore
                    break;
                }
                if (child_parent.isRoot()) {
                    // Split root, and after splitting the root, the entire splitting process
                    //ends.
                    // Here the splitInternal() will change the value @new_node_id to the
                    //id of the node that is newly split.
                    child_parent.splitInternal(split_key, traits, new_node_id, split_frac);
                    BeNode<key_type, value_type, knobs, compare> new_sibling(manager, new_node_id);
                    manager->addDirtyNode(new_node_id);
                    traits.internal_splits++;

                    uint new_root_id = manager->allocate();
                    auto *new_root = new BeNode<key_type, value_type, knobs, compare>(manager, new_root_id);
                    new_root->setRoot(true);
                    new_root->setChildKey(split_key, 0);
                    new_root->setPivot(child_parent.getId(), 0);
                    new_root->setPivot(new_sibling.getId(), 1);
                    new_root->setPivotCounter(new_root->getPivotsCtr() + 2);
                    manager->addDirtyNode(new_root_id);

                    child_parent.setRoot(false);
                    child_parent.setParent(new_root->getId());
                    manager->addDirtyNode(child_parent.getId());
                    new_sibling.setParent(new_root->getId());
                    manager->addDirtyNode(new_sibling.getId());

                    root = new_root;
                    break;
                }

                // The parent node is not root, we just split it, and to see whether another
                //split is needed.
                child_parent.splitInternal(split_key, traits, new_node_id, split_frac);
                traits.internal_splits++;
                manager->addDirtyNode(child_parent.getId());
                new_node.setToId(new_node_id);
                manager->addDirtyNode(new_node_id);
            }
        }

        return true;
    }

    std::pair<key_type, value_type> swap_in_tail_leaf(key_type key, value_type val) {
        // When using this method, no need to consider splitting of leaf node.
        assert(tail_leaf != nullptr);
        auto ret = tail_leaf->swap_in_leaf(key, val);
        if (ret.first == max_key) {
            max_key = tail_leaf->max_key();
        }
        return ret;
    }

};

#endif
