#ifndef OSMTREE_H
#define OSMTREE_H
#define FAST_PATH
#include <iostream>
#include <math.h>
#include <cassert>
#include <algorithm>
#include <functional>
#include <string.h>
#include <queue>
#include <chrono>
#include <memory>
#include <cmath>

// #include "betree.h"
#include "bp_tree.h"
#include "adaptive.h"
#include "swareBuffer.h"

#include <cmath>

#define OSMSIZE 100

#ifdef OSMTIMER
struct OsmTimer
{
    unsigned long insert_time = 0;
    unsigned long point_query_time = 0;
    unsigned long range_query_time = 0;
};
#endif

#ifdef OSMPROFILE

extern unsigned long flushPage_time;

extern unsigned long insertInBuf_time;

extern unsigned long osmLoad_time;
extern unsigned long flush_time;
extern unsigned long zoneupdate_time;
extern unsigned long seq_search_time;
extern unsigned long bin_search_time;

extern unsigned long tot_zones_scanned;
extern unsigned long zone_seq_time;
extern unsigned long sample_time;
extern unsigned long zone_scan_time;
extern unsigned long bf_scan_time;
extern unsigned long global_bf_insert_time;
extern unsigned long sublevel_bf_insert_time;
extern unsigned long sublevel_bf_scan_time;
extern unsigned long unsorted_sorting_time;
extern unsigned long unsorted_interpolation_time;

unsigned long flushPage_time = 0;

unsigned long insertInBuf_time = 0;

unsigned long osmLoad_time = 0;
unsigned long flush_time = 0;
unsigned long zoneupdate_time = 0;
unsigned long seq_search_time = 0;
unsigned long bin_search_time = 0;

unsigned long tot_zones_scanned = 0;
unsigned long zone_seq_time = 0;
unsigned long sample_time = 0;
unsigned long zone_scan_time = 0;
unsigned long bf_scan_time = 0;

unsigned long global_bf_insert_time = 0;
unsigned long sublevel_bf_insert_time = 0;
unsigned long sublevel_bf_scan_time = 0;
unsigned long unsorted_sorting_time = 0;
unsigned long unsorted_interpolation_time = 0;

#endif

#ifdef OSMP
extern unsigned long bulkload_time;
extern unsigned long sort_time;
extern unsigned long tree_search_time;
extern unsigned long topInsert_time;

unsigned long bulkload_time = 0;
unsigned long tree_search_time = 0;
unsigned long sort_time = 0;
unsigned long topInsert_time = 0;
#endif

extern long top_inserts;
long top_inserts = 0;

extern long num_flushes;
long num_flushes = 0;

extern unsigned long num_tree_scan;
extern unsigned long num_tree_found;
unsigned long num_tree_scan = 0;
unsigned long num_tree_found = 0;

extern unsigned long tree_zone_queries;
extern unsigned long tree_zone_positive;
unsigned long tree_zone_queries = 0;
unsigned long tree_zone_positive = 0;

extern unsigned long tot_tree_range;
unsigned long tot_tree_range = 0;
extern unsigned long tot_buffer_range;
unsigned long tot_buffer_range = 0;

template <typename _Key, typename _Value>
class OsmTree : public bp_tree<_Key, _Value>
{
    OsmBuffer<_Key, _Value> *osmBuffer;
    double fillFactor;

public:
#ifdef OSMTIMER
    OsmTimer osmTimer;

#endif

    static constexpr uint8_t LEAF_CAPACITY = node_t::leaf_capacity;

public:
    OsmTree(std::string _name, std::string _rootDir, int _size_of_each_block, uint _blocks_in_memory, int _osmCap, double _fillFactor = 1) : BeTree<_Key, _Value, _Knobs, _Compare>(_name, _rootDir, _size_of_each_block, _blocks_in_memory)
    {

        assert(_fillFactor > 0 && _fillFactor <= 1.0);
        fillFactor = _fillFactor;
        int num_per_zone = (LEAF_CAPACITY - 1 - 1) * fillFactor;
        osmBuffer = new OsmBuffer<_Key, _Value>(_osmCap, num_per_zone);
    }

    ~OsmTree()
    {
        delete osmBuffer;
    }

    OsmBufferCounters getBufferCounters()
    {
        return osmBuffer->getCounters();
    }

public:
    template <typename Iterator>
    bool osmLoad(Iterator ibegin, Iterator iend)
    {
#ifdef OSMPROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        int count = std::distance(ibegin, iend);
        // bool flag = false;
        // case 1: tree is not empty
        if (this->ctr_size != 0)
        {
            node_t tail;
            tail.load(manager.open_block(fp_id));
            _Key last_key = tail.fp_max;
            _Key first_key_to_insert = ibegin->first;

            // bulk load if last key is less than
            if (last_key < first_key_to_insert)
            {
#ifdef OSMP
                auto start_bulk = std::chrono::high_resolution_clock::now();
#endif
                this->bulkload_leaf(ibegin, iend);
#ifdef OSMP
                auto stop_bulk = std::chrono::high_resolution_clock::now();
                auto duration_bulk = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_bulk - start_bulk);
                bulkload_time += duration_bulk.count();
#endif
            }
            else
            {
#ifdef OSMP
                auto start_top = std::chrono::high_resolution_clock::now();
#endif
                // in this case, we just insert one by one into the tree
                int s = 0;
                for (Iterator it = ibegin; it != iend; ++it, s++)
                {
                    this->insert(it->first, it->second);
                }
                top_inserts += s;
                this->insert(iend->first, iend->second);
#ifdef OSMP
                auto stop_top = std::chrono::high_resolution_clock::now();
                auto duration_top = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_top - start_top);
                topInsert_time += duration_top.count();
#endif
            }
        }
        else
        {
// the tree is empty, so we can just bulk load blindly
#ifdef OSMP
            auto start_bulk = std::chrono::high_resolution_clock::now();
#endif
            this->bulkload_leaf(ibegin, iend);
#ifdef OSMP
            auto stop_bulk = std::chrono::high_resolution_clock::now();
            auto duration_bulk = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_bulk - start_bulk);
            bulkload_time += duration_bulk.count();
#endif
        }
#ifdef OSMPROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        osmLoad_time += duration.count();
#endif
        return true;
    }

    bool osmInsert(_Key key, _Value value)
    {
#ifdef OSMTIMER
        auto start = std::chrono::high_resolution_clock::now();
#endif
        std::pair<_Key, _Value> element(key, value);
        bool flag = osmBuffer->insertInOsmBuf(element);

        // if flag returns true, it means buffer is full. We need to flush and insert
        if (flag)
        {
            // if we have at least one zone sorted, flush at most half the zones
            if (osmBuffer->getLastSortedZone() >= 0)
            {
                int n = osmBuffer->getNumPerZone();
                int sorted = osmBuffer->getLastSortedZone();
                if (sorted > osmBuffer->getNumZones() / 2)
                {
                    sorted = osmBuffer->getNumZones() / 2;
                }

                if (osmBuffer->getLastSortedZone() == osmBuffer->getNumZones() - 1 - 1)
                {
                    osmBuffer->setSortedBuffer(true);
                }
#ifdef OSMPROFILE
                auto start_flush = std::chrono::high_resolution_clock::now();
#endif

                int num_to_flush = (sorted + 1) * osmBuffer->getNumPerZone();
                assert(num_to_flush > 0);
                std::pair<_Key, _Value> *elements_to_flush = new std::pair<_Key, _Value>[num_to_flush];

                osmBuffer->flushPage(num_to_flush, elements_to_flush);

                // since we have flushed sorted number of pages from buffer, lets modify boundaries
                osmBuffer->modifySortedBoundaries(sorted);

                int start_index = 0;
                while (sorted >= 0)
                {
                    bool f = osmLoad(elements_to_flush + start_index, elements_to_flush + start_index + n - 1);
                    sorted--;
                    num_flushes++;
                    start_index += n;

                    if (start_index >= num_to_flush)
                    {
                        break;
                    }
                }
                assert(start_index >= num_to_flush);

#ifdef OSMPROFILE
                auto stop_flush = std::chrono::high_resolution_clock::now();
                auto duration_flush = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_flush - start_flush);
                flush_time += duration_flush.count();
#endif
                // since we have flushed all that we had sorted, now we can sort the remaining
                if (!osmBuffer->isBufferSorted())
                {
#ifdef OSMP
                    auto start_sort = std::chrono::high_resolution_clock::now();
#endif

                    osmBuffer->sortOsmBufNew();
#ifdef OSMP
                    auto stop_sort = std::chrono::high_resolution_clock::now();
                    auto duration_sort = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_sort - start_sort);
                    sort_time += duration_sort.count();
#endif
                }

#ifdef OSMPROFILE
                auto start_zoneupdate = std::chrono::high_resolution_clock::now();
#endif
                osmBuffer->resetGlobalBF();
                // update all zonemaps
                osmBuffer->updateZonemaps();
#ifdef OSMPROFILE
                auto stop_zoneupdate = std::chrono::high_resolution_clock::now();
                auto duration_zoneupdate = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_zoneupdate - start_zoneupdate);
                zoneupdate_time += duration_zoneupdate.count();
#endif

                osmBuffer->performPostFlushProcedure();
            }
            else
            {
#ifdef OSMP
                auto start_sort = std::chrono::high_resolution_clock::now();
#endif
                // first sort the buffer
                osmBuffer->sortOsmBufNew();
#ifdef OSMP
                auto stop_sort = std::chrono::high_resolution_clock::now();
                auto duration_sort = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_sort - start_sort);
                sort_time += duration_sort.count();
#endif

                // flush half the pages
                int n = (LEAF_CAPACITY - 1 - 1) * fillFactor;
                int i = osmBuffer->getNumZones() / 2;
#ifdef OSMPROFILE
                auto start_flush = std::chrono::high_resolution_clock::now();
#endif
                int num_to_flush = i * osmBuffer->getNumPerZone();
                std::pair<_Key, _Value> *elements_to_flush = new std::pair<_Key, _Value>[num_to_flush];

                osmBuffer->flushPage(num_to_flush, elements_to_flush);

                int start_index = 0;
                while (i >= 0)
                {
                    bool f = osmLoad(elements_to_flush + start_index, elements_to_flush + start_index + n - 1);
                    i--;
                    num_flushes++;
                    start_index += n;

                    if (start_index >= num_to_flush)
                    {
                        break;
                    }
                }
                assert(start_index >= num_to_flush);

#ifdef OSMPROFILE
                auto stop_flush = std::chrono::high_resolution_clock::now();
                auto duration_flush = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_flush - start_flush);
                flush_time += duration_flush.count();
#endif
#ifdef OSMPROFILE
                auto start_zoneupdate = std::chrono::high_resolution_clock::now();
#endif
                osmBuffer->resetGlobalBF();
                // update all zonemaps
                osmBuffer->updateZonemaps();
#ifdef OSMPROFILE
                auto stop_zoneupdate = std::chrono::high_resolution_clock::now();
                auto duration_zoneupdate = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_zoneupdate - start_zoneupdate);
                zoneupdate_time += duration_zoneupdate.count();
#endif

                osmBuffer->performPostFlushProcedure();
            }
        }

#ifdef OSMTIMER
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        osmTimer.insert_time += duration.count();
#endif
        return false;
    }

    bool osmQuery(_Key key)
    {

#ifdef OSMTIMER
        auto start = std::chrono::high_resolution_clock::now();
#endif
        bool flag = false;

        flag = osmBuffer->query(key);

        if (flag)
        {
#ifdef OSMTIMER
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            osmTimer.point_query_time += duration.count();
#endif
            return true;
        }

        // perform tree scans
        num_tree_scan += 1;
        flag = false;
        bool within_tree_range = (key >= this->getMinimumKey() && key <= this->getMaximumKey());
        tree_zone_queries += 1;
        if (within_tree_range)
        {
            tree_zone_positive += 1;
#ifdef OSMP
            auto start_tree = std::chrono::high_resolution_clock::now();
#endif
            // if not found in buffer, call query function from tree
            flag = this->get(key);
#ifdef OSMP
            auto stop_tree = std::chrono::high_resolution_clock::now();
            auto duration_tree = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_tree - start_tree);
            tree_search_time += duration_tree.count();
#endif
#ifdef OSMTIMER
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            osmTimer.point_query_time += duration.count();
#endif
        }

        if (flag)
        {
            num_tree_found += 1;
        }
        return flag;
    }

    //     std::vector<std::pair<_Key, _Value>> osmRangeQuery(_Key low, _Key high)
    //     {
    // #ifdef OSMTIMER
    //         auto start = std::chrono::high_resolution_clock::now();
    // #endif

    //         std::vector<std::pair<_Key, _Value>> elements = osmBuffer->rangeQuery(low, high);
    //         tot_buffer_range += elements.size();

    //         // if not found in buffer, call query function from tree
    //         bool within_tree_range = (!(high < this->getMinimumKey()) && !(low > this->getMaximumKey()));
    //         if (within_tree_range)
    //         {
    //             std::vector<std::pair<_Key, _Value>> tree_elements = this->rangeQuery(low, high);
    //             tot_tree_range += tree_elements.size();

    //             elements.insert(elements.end(), tree_elements.begin(), tree_elements.end());
    //         }
    // #ifdef OSMTIMER
    //         auto stop = std::chrono::high_resolution_clock::now();
    //         auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    //         osmTimer.range_query_time += duration.count();
    // #endif

    //         return elements;
    //     }

    int getOsmBufCap()
    {
        return osmBuffer->getOsmBufCap();
    }

    int getOsmBufSize()
    {
        return osmBuffer->getOsmBufSize();
    }
};

#endif