#ifndef OSMBUFFER_H
#define OSMBUFFER_H

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

#include "adaptive.h"
#include <cmath>
#include "bloomfilter.h"
#include "sort.h"

#define subsorted_cap 100

#ifdef OSMPROFILE

extern unsigned long flushPage_time;
extern unsigned long sort_time;
extern unsigned long insertInBuf_time;
extern unsigned long topInsert_time;
extern unsigned long osmLoad_time;
extern unsigned long flush_time;
extern unsigned long zoneupdate_time;
extern unsigned long seq_search_time;
extern unsigned long bin_search_time;
extern unsigned long tree_search_time;
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
unsigned long sort_time = 0;
unsigned long insertInBuf_time = 0;
unsigned long topInsert_time = 0;
unsigned long osmLoad_time = 0;
unsigned long flush_time = 0;
unsigned long zoneupdate_time = 0;
unsigned long seq_search_time = 0;
unsigned long bin_search_time = 0;
unsigned long tree_search_time = 0;
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
extern unsigned long unsorted_sorting_time;
extern unsigned long seq_search_time;
extern unsigned long bin_search_time;

unsigned long unsorted_sorting_time = 0;
unsigned long seq_search_time = 0;
unsigned long bin_search_time = 0;
#endif

struct OsmBufferCounters
{
    long num_adaptive_fail = 0;
    long num_adaptive = 0;
    long num_quick = 0;
    long total_sort = 0;
    long num_merge = 0;
    unsigned long num_seq_scan = 0;
    unsigned long num_bin_scan = 0;
    unsigned long num_seq_found = 0;
    unsigned long num_bin_found = 0;
    unsigned long osm_fence_queries = 0;
    unsigned long osm_fence_positive = 0;
    unsigned long global_bf_queried = 0;
    unsigned long global_bf_positive = 0;
    unsigned long num_zones_queried = 0;
    unsigned long num_zones_positive = 0;
    unsigned long sublevel_bf_queried = 0;
    unsigned long sublevel_bf_positive = 0;
    unsigned long seq_zone_queries = 0;
    unsigned long seq_zone_positive = 0;
    unsigned long unsorted_zonemap_queries = 0;
    unsigned long unsorted_zonemap_positive = 0;
    unsigned long sorted_zonemap_queries = 0;
    unsigned long sorted_zonemap_positive = 0;
    unsigned long subblock_interpolation_queries = 0;
    unsigned long subblock_interpolation_positive = 0;
    unsigned long num_unsorted_positive = 0;
    unsigned long sort_buffer_time = 0;
    unsigned long gen_sort_time = 0;
    unsigned long merge_sort_time = 0;
};

template <typename _Key, typename _Value>
class OsmBuffer
{
    // the actual buffer
    std::pair<_Key, _Value> *osmBuffer;

    // current size of the buffer
    int osmBufSize;

    // total capacity of the buffer
    int osmCap;

    // zonemaps
    int num_zones;
    int current_zone;
    int num_per_zone;
    int current_zone_element_count;

    int max_discrepancy;
    int out_of_order_elements;
    int previous_boundary;

    _Key last_element;
    int last_sorted_zone;
    bool sorted_buffer;
    bool out_of_order;
    long flush_cycle;

    _Key buf_min, buf_max;
    _Key sorted_min, sorted_max, unsorted_min, unsorted_max;

    std::pair<_Key, _Key> *zones;

    int DISCREPANCY_THRESHOLD;
    int NOISE_THRESHOLD;

#ifdef GLOBALBF
    BloomFilter *filter;
#endif
#ifdef SUBBFS
    BloomFilter **subfilters;
#endif
    int subsorted_boundaries[subsorted_cap];
    int subsorted_size;
    bool atleast_one_subsorted;

    bool range_sorted;
    int range_sorted_boundary;

    double fill_threshold;

    OsmBufferCounters counters;

    // constructors and destructor
public:
    OsmBuffer(int _osmCap, int _num_per_zone) : osmCap(_osmCap)
    {
        osmBufSize = 0;
        osmBuffer = new std::pair<_Key, _Value>[osmCap];

        num_per_zone = _num_per_zone;
        num_zones = std::ceil(osmCap / (double)num_per_zone);

        current_zone = 0;
        current_zone_element_count = 0;
        out_of_order = false;
        out_of_order_elements = 0;
        max_discrepancy = 0;

        zones = new std::pair<_Key, _Key>[num_zones];

        DISCREPANCY_THRESHOLD = num_zones / 10;
        NOISE_THRESHOLD = osmCap / 10;

#ifdef GLOBALBF
        filter = new BloomFilter(osmCap, 10);
#endif

#ifdef SUBBFS
        // init one BF per zone
        subfilters = new BloomFilter *[num_zones];
        for (int i = 0; i < num_zones; i++)
        {
            subfilters[i] = new BloomFilter(num_per_zone + 1, 10);
        }
#endif
        flush_cycle = 1;

        subsorted_size = 0;
        atleast_one_subsorted = false;

        range_sorted = false;

        fill_threshold = 0.1;
        std::cout << "Fill Threshold:" << fill_threshold << std::endl;
    }

    ~OsmBuffer()
    {
        delete[] osmBuffer;
        delete[] zones;

#ifdef GLOBALBF
        delete filter;
#endif
#ifdef SUBBFS
        for (int i = 0; i < num_zones; i++)
        {
            delete subfilters[i];
        }
        delete[] subfilters;
#endif
    }

    // getters
public:
    _Key getDummyKey()
    {
        _Key temp;

        return temp;
    }

    int getOsmBufSize()
    {
        return osmBufSize;
    }

    int getOsmBufCap()
    {
        return osmCap;
    }

    bool isOsmBufFull()
    {
        return osmBufSize == osmCap;
    }

    void stlSortOsmBuf()
    {
        std::sort(osmBuffer, osmBuffer + osmBufSize);
    }

    int getNumPerZone()
    {
        return num_per_zone;
    }

    int getNumZones()
    {
        return num_zones;
    }

    int getCurrentZone()
    {
        return current_zone;
    }

    int getPreviousBoundary()
    {
        return previous_boundary;
    }

    std::pair<_Key, _Value> *getOsmBuffer()
    {
        return osmBuffer;
    }

    std::pair<_Key, _Key> *getOsmZones()
    {
        return zones;
    }

    int getLastSortedZone()
    {
        return last_sorted_zone;
    }

    void setSortedBuffer(bool flag)
    {
        sorted_buffer = flag;
    }

    OsmBufferCounters getCounters()
    {
        return counters;
    }

public:
    int searchZone(_Key key, int l, int r)
    {

        while (l <= r)
        {
            int m = l + (r - l) / 2;

            // check if key can be placed at mid zone
            if (key >= zones[m].first && key <= zones[m].second)
            {
                return m;
            }

            // check for a corner case
            if (m - 1 >= 0)
            {
                if (key >= zones[m - 1].second && key <= zones[m].first)
                {
                    return m;
                }
            }

            // if key is greater than mid zone's max, ignore the left half
            if (key > zones[m].second)
            {
                l = m + 1;
            }
            else if (key < zones[m].first)
            {
                r = m - 1;
            }
        }

        return -1;
    }

    void updateZonemaps()
    {
        // reset all counters
        current_zone = 0;
        current_zone_element_count = 0;

        // calculate number of required zones to update
        // int num_required_zones = osmBufSize / num_per_zone + 1;
        int num_required_zones = std::ceil(osmBufSize / (double)num_per_zone);

        // since we have flushed elements (at least a since page/zone), this would always be less than our zones capacity
        assert(num_required_zones < num_zones);

        // update zone maps is always done after the sorting process
        // so the first element and the last element in every zone would be our min and max

        zones[0].first = osmBuffer[0].first;
        zones[0].second = osmBuffer[num_per_zone - 1].first;

        int start, end;

        for (int i = 0; i < num_required_zones; i++)
        {
            start = num_per_zone * i;
            end = start + num_per_zone - 1;
            if (end > osmBufSize)
            {
                end = osmBufSize - 1;
            }
            int num = end - start + 1;

            current_zone_element_count = num;
            zones[i].first = osmBuffer[start].first;
            zones[i].second = osmBuffer[end].first;
            current_zone = i;
        }
#ifdef SUBBFS
        // reset all subfilters
        for (int i = 0; i < num_zones; i++)
        {
            subfilters[i]->reset();
        }
#endif

        // check if current zone element count is not at capacity
        if (current_zone_element_count > 0 && current_zone_element_count < num_per_zone)
        {
            bool flag = true;
            for (int i = start; i <= end; i++)
            {
                std::string str_key = std::to_string(osmBuffer[i].first);
#ifdef GLOBALBF
                filter->program(str_key);
#endif
#ifdef SUBBFS
                subfilters[current_zone]->program(str_key);
#endif
            }
        }

        // ensure all elements have been covered by the map
        assert(end == osmBufSize - 1);

        // assert(zones.size() <= num_zones);
        assert(current_zone <= num_zones);

        // buffer's min and max will be the first and last element in the buffer
        buf_min = osmBuffer[0].first;
        buf_max = osmBuffer[osmBufSize - 1].first;

        sorted_min = buf_min;
        sorted_max = buf_max;

        // we now move into the next flush cycle
        flush_cycle += 1;
    }

public:
    bool adaptiveSortOsmBuf()
    {
        int k = out_of_order_elements;
        int l = (num_per_zone * max_discrepancy);
        int n = osmBufSize;

        std::pair<_Key, _Value> *OUT = new std::pair<_Key, _Value>[n];


        bool try_adaptive_sort = adaptive_sort(osmBuffer, n, k, l, OUT);

        // if adaptive sort is successful, replace buffer with sorted order
        if (try_adaptive_sort)
        {
            for (int i = 0; i < n; i++)
            {
                osmBuffer[i] = OUT[i];
            }
        }
        delete[] OUT;
        return try_adaptive_sort;
    }

public:
    void merge(std::pair<_Key, _Value> array[], int const left, int const mid, int const right)
    {
        auto const subArrayOne = mid - left + 1;
        auto const subArrayTwo = right - mid;

        // Create temp arrays
        auto *leftArray = new std::pair<_Key, _Value>[subArrayOne],
             *rightArray = new std::pair<_Key, _Value>[subArrayTwo];

        // Copy data to temp arrays leftArray[] and rightArray[]
        for (auto i = 0; i < subArrayOne; i++)
            leftArray[i] = array[left + i];
        for (auto j = 0; j < subArrayTwo; j++)
            rightArray[j] = array[mid + 1 + j];

        auto indexOfSubArrayOne = 0,   // Initial index of first sub-array
            indexOfSubArrayTwo = 0;    // Initial index of second sub-array
        int indexOfMergedArray = left; // Initial index of merged array

        // Merge the temp arrays back into array[left..right]
        while (indexOfSubArrayOne < subArrayOne && indexOfSubArrayTwo < subArrayTwo)
        {
            if (leftArray[indexOfSubArrayOne] <= rightArray[indexOfSubArrayTwo])
            {
                array[indexOfMergedArray] = leftArray[indexOfSubArrayOne];
                indexOfSubArrayOne++;
            }
            else
            {
                array[indexOfMergedArray] = rightArray[indexOfSubArrayTwo];
                indexOfSubArrayTwo++;
            }
            indexOfMergedArray++;
        }
        // Copy the remaining elements of
        // left[], if there are any
        while (indexOfSubArrayOne < subArrayOne)
        {
            array[indexOfMergedArray] = leftArray[indexOfSubArrayOne];
            indexOfSubArrayOne++;
            indexOfMergedArray++;
        }
        // Copy the remaining elements of
        // right[], if there are any
        while (indexOfSubArrayTwo < subArrayTwo)
        {
            array[indexOfMergedArray] = rightArray[indexOfSubArrayTwo];
            indexOfSubArrayTwo++;
            indexOfMergedArray++;
        }
    }

    void sortOsmBuf()
    {
        auto start = std::chrono::high_resolution_clock::now();
        if (out_of_order_elements != 0)
        {
            counters.total_sort += 1;

            int k_limit_1 = 10 / 100.0 * osmCap;
            int k_limit_2 = 5 / 100 * osmCap;

            int l_limit_1 = 10 / 100.0 * num_zones;
            int l_limit_2 = 5 / 100.0 * num_zones;

            if ((out_of_order_elements < k_limit_1 && max_discrepancy < l_limit_1) ||
                out_of_order_elements < k_limit_2 ||
                max_discrepancy < l_limit_2)
            {
                bool flag = adaptiveSortOsmBuf();
                counters.num_adaptive += 1;
                if (!flag)
                {
                    stlSortOsmBuf();
                    counters.num_adaptive_fail += 1;
                    counters.num_quick += 1;
                }
            }

            else
            {
                stlSortOsmBuf();
                counters.num_quick += 1;
            }
        }
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        // counters.sort_buffer_time += duration.count();
    }

    void sortOsmBufNew()
    {
        auto start = std::chrono::high_resolution_clock::now();
        if (out_of_order_elements != 0)
        {
            auto start_merge = std::chrono::high_resolution_clock::now();
            counters.total_sort += 1;
            if (atleast_one_subsorted)
            {
                counters.num_merge += 1;
                int last_boundary = subsorted_boundaries[subsorted_size - 1];
                // first sort the unsorted block starting from last_boundary+1
                int start_index = (num_per_zone * (last_boundary + 1));
                // sort from start_index to osmBufSize
                std::stable_sort(osmBuffer + start_index, osmBuffer + osmBufSize);

                // now, our entire buffer consists of sorted blocks
                // we simply need to call merge on pairs of those blocks until we have
                // one left

                // our sorted blocks are as follows:
                // 0...previous_boundary, previous_boundary+1...subsorted_boundaries[0], subsorted_boundaries[1]...subsorted[subsorted_size-1],

                // let's start merging all subsorted blocks first
                start_index = (num_per_zone * (previous_boundary + 1));
                int num_blocks_to_merge = subsorted_size;
                int i = 0;
                int mid_index, end_index;
                while (num_blocks_to_merge > 1)
                {

                    mid_index = (num_per_zone * (subsorted_boundaries[i] + 1)) - 1;
                    assert(i + 1 < subsorted_size);
                    end_index = num_per_zone * (subsorted_boundaries[i + 1] + 1) - 1;

                    // merge now
                    merge(osmBuffer, start_index, mid_index, end_index);
                    i = i + 1;
                    num_blocks_to_merge--;
                }

                // now we will have only 3 sorted blocks:
                // one pre-sorted, one query-sorted, and one flush-sorted
                // let's merge the first two
                int pre_sorted_end = (num_per_zone * (previous_boundary + 1)) - 1;
                int query_sorted_end = (num_per_zone * (last_boundary + 1)) - 1;
                merge(osmBuffer, 0, pre_sorted_end, query_sorted_end);

                // merge the remaining
                merge(osmBuffer, 0, query_sorted_end, osmBufSize - 1);
                auto stop_merge = std::chrono::high_resolution_clock::now();
                auto duration_merge = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_merge - start_merge);
                counters.merge_sort_time += duration_merge.count();
            }
            else
            {
                auto start_gen = std::chrono::high_resolution_clock::now();
                int k_limit_1 = 10 / 100.0 * osmCap;
                int k_limit_2 = 5 / 100 * osmCap;

                int l_limit_1 = 10 / 100.0 * num_zones;
                int l_limit_2 = 5 / 100.0 * num_zones;

                if ((out_of_order_elements < k_limit_1 && max_discrepancy < l_limit_1) ||
                    out_of_order_elements < k_limit_2 ||
                    max_discrepancy < l_limit_2)
                {
                    bool flag = adaptiveSortOsmBuf();
                    counters.num_adaptive += 1;
                    if (!flag)
                    {
                        stlSortOsmBuf();
                        counters.num_adaptive_fail += 1;
                        counters.num_quick += 1;
                    }
                }

                else
                {
                    stlSortOsmBuf();
                    counters.num_quick += 1;
                }
                auto stop_gen = std::chrono::high_resolution_clock::now();
                auto duration_gen = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_gen - start_gen);
                counters.gen_sort_time += duration_gen.count();
            }
        }
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        counters.sort_buffer_time += duration.count();
    }

    void modifySortedBoundaries(int number_of_pages_flushed)
    {
        // here, we simply deduct number_of_pages_flushed from each boundary

        // first deduct from previous_boundary
        if (!sorted_buffer)
        {
            previous_boundary -= number_of_pages_flushed;
            assert(previous_boundary >= 0);

            if (atleast_one_subsorted)
            {
                for (int i = 0; i < subsorted_size; i++)
                {
                    subsorted_boundaries[i] -= number_of_pages_flushed;
                    assert(subsorted_boundaries[i] >= 0);
                }
            }
        }
    }

public:
    void flushPage(int num_to_flush, std::pair<_Key, _Value> *&flushed)
    {
#ifdef OSMPROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        // temp to hold buffer elements that are not copied
        // will replace original buf
        std::pair<_Key, _Value> *temp = new std::pair<_Key, _Value>[osmCap];

        std::pair<_Key, _Value> empty_pair = temp[0];

        int c = 0;

        for (int i = 0; i < osmBufSize; i++)
        {
            if (i < num_to_flush)
            {
                flushed[i] = osmBuffer[i];
                continue;
            }
            temp[c] = osmBuffer[i];
            c++;
        }

        // copy temp to buffer from start

        memcpy(osmBuffer, temp, c * sizeof(*temp));

        osmBufSize = c;

        delete[] temp;
#ifdef OSMPROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        flushPage_time += duration.count();
#endif
    }

    bool insertInOsmBuf(std::pair<_Key, _Value> element)
    {
#ifdef OSMPROFILE
        auto start_ins = std::chrono::high_resolution_clock::now();
#endif
        assert(osmBufSize < osmCap);
        osmBuffer[osmBufSize++] = element;

        // set range_sorted to false
        range_sorted = false;

        if (osmBufSize == 0)
        {
            last_element = element.first;
        }

        bool current_out_of_order = false;
        // check if inserted element is out of order
        if (element.first >= last_element)
        {
            if (!out_of_order)
            {
                last_sorted_zone = current_zone - 1;
            }
        }
        else
        {
            sorted_buffer = false;
            current_out_of_order = true;
            bool lsz_moved_left = false;
            if (current_zone == 0 || last_sorted_zone == -1)
            {
                last_sorted_zone = -1;
            }
            else if (element.first < zones[last_sorted_zone].second) // was second; not as bug
            {
                // update last sorted zone
                // last_sorted_zone = searchZone(element.first) - 1;
                int z = searchZone(element.first, 0, last_sorted_zone);
                if (z >= 0)
                {
                    last_sorted_zone = z - 1;
                    lsz_moved_left = true;

                }
                else
                {
                    last_sorted_zone = -1; // basically z will return -1 so set to -1
                    // max_discrepancy = current_zone + 1; // since the element we had is smaller than smallest in buffer, max_discrepency is equal to cur. length of buffer
                }
            }
       

            // handle the max_discrepancy here by replacing the else block above
            int discrepancy = -1;
            if (last_sorted_zone >= previous_boundary)
            {
                discrepancy = current_zone - last_sorted_zone;
            }
            else
            {
                if (last_sorted_zone == -1)
                {
                    discrepancy = current_zone + 1;
                }
                else if (lsz_moved_left)
                {
                    discrepancy = current_zone - last_sorted_zone;
                }
                else if (element.first >= zones[last_sorted_zone].second && element.first <= zones[previous_boundary].second)
                {
                    int z = searchZone(element.first, last_sorted_zone + 1, previous_boundary);
                    discrepancy = current_zone - z;
                }
                else
                {
                    discrepancy = current_zone - previous_boundary;
                }
            }
            if (discrepancy > max_discrepancy && discrepancy != -1)
                max_discrepancy = discrepancy;

            out_of_order = true;
            out_of_order_elements++;
        }

        std::string str_key = std::to_string(element.first);
#ifdef GLOBALBF
// add element to bloom filter
#ifdef OSMPROFILE
        auto start_globalbf = std::chrono::high_resolution_clock::now();
#endif
        filter->program(str_key);
#ifdef OSMPROFILE
        auto stop_globalbf = std::chrono::high_resolution_clock::now();
        auto duration_globalbf = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_globalbf - start_globalbf);
        global_bf_insert_time += duration_globalbf.count();
#endif
#endif
        // assert(zones.size() <= num_zones);
        assert(current_zone <= num_zones);
        last_element = element.first;

        // if map is empty or zone does not exist, simply add min and max for zone as element
        if (current_zone == 0 && current_zone_element_count == 0)
        {

            zones[current_zone].first = element.first;
            zones[current_zone].second = element.first;
            current_zone_element_count++;
        }
        else
        {
            // if space exists in current zone
            if (current_zone_element_count < num_per_zone)
            {
                // if zone exists in map, then compare with min and max
                _Key min = zones[current_zone].first;
                _Key max = zones[current_zone].second;
                _Key element_key = element.first;

                if (element_key < min)
                {
                    zones[current_zone].first = element_key;
                }
                else if (element_key > max)
                {
                    zones[current_zone].second = element_key;
                }
                current_zone_element_count++;
            }
            else
            {
                current_zone++;
                current_zone_element_count = 0;

                zones[current_zone].first = element.first;
                zones[current_zone].second = element.first;
                current_zone_element_count++;

                if (!out_of_order)
                {
                    last_sorted_zone = current_zone - 1;
                    // std::cout << "LSZ MOVING RIGHT" << std::endl;
                }
            }
        }

#ifdef SUBBFS
#ifdef OSMPROFILE
        auto start_sublevel = std::chrono::high_resolution_clock::now();
#endif
        subfilters[current_zone]->program(str_key);
#ifdef OSMPROFILE
        auto stop_sublevel = std::chrono::high_resolution_clock::now();
        auto duration_sublevel = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_sublevel - start_sublevel);
        sublevel_bf_insert_time += duration_sublevel.count();
#endif
#endif
        assert(current_zone <= num_zones);

        // similarly, compare for overall buffer minimum and maximum
        if (buf_min == 0 || buf_max == 0)
        {
            buf_min = element.first;
            buf_max = element.first;
        }
        else if (element.first < buf_min)
        {
            buf_min = element.first;
        }
        else if (element.first > buf_max)
        {
            buf_max = element.first;
        }

        // if current element is out of order, check if it is the first one in this cycle
        if (current_out_of_order && out_of_order_elements == 1)
        {
            // if we are in the first flush cycle, then set previous boundary to
            // the one before the current zone
            if (flush_cycle == 1)
            {
                previous_boundary = current_zone - 1;
            }

            // since this is the first unsorted element in this buffer cycle
            // initialize both unsorted min and max to this number.
            unsorted_min = element.first;
            unsorted_max = element.first;
        }

        // now we have to update the sortedness zonemaps for sorted and unsorted sections
        // if sorted_buffer is true, we can simply check current element against sorted zonemap's min and max
        // else we check against unsorted
        if (sorted_buffer)
        {
            // if this is the first element in the buffer, initialize both min and max to this key
            if (osmBufSize == 1)
            {
                sorted_min = element.first;
                sorted_max = element.first;
            }
            else
            {
                if (element.first < sorted_min)
                    sorted_min = element.first;
                else if (element.first > sorted_max)
                    sorted_max = element.first;
            }
        }
        else
        {
            if (element.first < unsorted_min)
                unsorted_min = element.first;
            else if (element.first > unsorted_max)
                unsorted_max = element.first;
        }

#ifdef OSMPROFILE
        auto stop_ins = std::chrono::high_resolution_clock::now();
        auto duration_ins = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_ins - start_ins);
        insertInBuf_time += duration_ins.count();
#endif
        return osmBufSize == osmCap;
    }

public:
    bool searchKey(_Key key)
    {
        int boundary = previous_boundary;
        if (last_sorted_zone > previous_boundary)
        {
            boundary = last_sorted_zone;
        }

        int n = num_per_zone * (boundary + 1);
        if (current_zone == boundary)
        {
            n = osmBufSize;
        }
        int l = 0;
        int r = n - 1;

        while (l <= r)
        {
            int m = l + (r - l) / 2;

            if (osmBuffer[m].first == key)
            {
                return true;
            }

            if (osmBuffer[m].first < key)
            {
                l = m + 1;
            }
            else
            {
                r = m - 1;
            }
        }

        return false;
    }

    int interpolationSearchKey(_Key key)
    {
        int boundary = previous_boundary;
        if (last_sorted_zone > previous_boundary)
        {
            boundary = last_sorted_zone;
        }

        assert(boundary >= 0);

        int n = num_per_zone * (boundary + 1);
        if (current_zone - 1 == boundary && sorted_buffer)
        {
            n = osmBufSize;
        }

        int low = 0;
        int high = n - 1;
        int mid;

        while (low <= high && key >= osmBuffer[low].first && key <= osmBuffer[high].first)
        {
            if (low == high)
            {
                if (osmBuffer[low].first == key)
                    return low;

                return -1;
            }

            int pos = low + ((double)(high - low) / (osmBuffer[high].first - osmBuffer[low].first) * (key - osmBuffer[low].first));

            if (osmBuffer[pos].first == key)
            {
                return pos;
            }

            if (osmBuffer[pos].first < key)
            {
                low = pos + 1;
            }
            else
            {
                high = pos - 1;
            }
        }
        return -1;
    }

    int interpolationSearchKey(_Key key, int low, int high)
    {

        while (low <= high && key >= osmBuffer[low].first && key <= osmBuffer[high].first)
        {
            if (low == high)
            {
                if (osmBuffer[low].first == key)
                    return low;

                return -1;
            }

            int pos = low + ((double)(high - low) / (osmBuffer[high].first - osmBuffer[low].first) * (key - osmBuffer[low].first));

            if (osmBuffer[pos].first == key)
            {
                return pos;
            }

            if (osmBuffer[pos].first < key)
            {
                low = pos + 1;
            }
            else
            {
                high = pos - 1;
            }
        }
        return -1;
    }

    // a few helpers
public:
    bool searchWithinZone(_Key key, int zone_id)
    {
#ifdef OSMPROFILE
        auto start = std::chrono::high_resolution_clock::now();
#endif
        int start_index = (num_per_zone * (zone_id));
        int end_index = (start_index + num_per_zone + 1);
        if (end_index > osmBufSize)
        {
            end_index = osmBufSize;
        }
        bool f = false;
        for (int j = start_index; j < end_index; j++)
        {
            if (key == osmBuffer[j].first)
            {
                f = true;
                break;
            }
        }

#ifdef OSMPROFILE
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        // std::cout<<"duration = "<<duration.count()<<std::endl;
        zone_seq_time += duration.count();
#endif
        return f;
    }

    void get_qualifying_zones(int qualifying_zones[], int &num_qualified, _Key key, int start_boundary)
    {
        for (int i = start_boundary; i <= current_zone; i++)
        {
            // increment #. of zones scanned
            counters.num_zones_queried += 1;
            if (key >= zones[i].first && key <= zones[i].second)
            {
                // increment #. of zones that returned positive
                counters.num_zones_positive += 1;

                qualifying_zones[num_qualified] = i;
                num_qualified++;

#ifdef OSMPROFILE
                tot_zones_scanned++;
#endif
            }
        }
    }

    void updateZonemaps(int start_zone, int end_zone)
    {
        int start, end;
        for (int i = start_zone; i < end_zone; i++)
        {
            start = num_per_zone * i;
            end = start + num_per_zone - 1;

            assert(end <= osmBufSize);

            zones[i].first = osmBuffer[start].first;
            zones[i].second = osmBuffer[end].first;
        }

#ifdef SUBBFS
        // reset all subfilters
        for (int i = start_zone; i < end_zone; i++)
        {
            subfilters[i]->reset();
        }
#endif
    }

    // more helpers
public:
    bool isAtLeastOneZoneSorted()
    {
        return last_sorted_zone >= 0;
    }

    bool isBufferSorted()
    {
        return sorted_buffer;
    }

    void resetGlobalBF()
    {
#ifdef GLOBALBF
        filter->reset();
#endif
    }

    void performPostFlushProcedure()
    {
        sorted_buffer = true;

        // after update, current_zone tells us the currently operating zone which would also be the last sorted one
        last_sorted_zone = current_zone - 1;
        out_of_order = false;
        last_element = osmBuffer[osmBufSize - 1].first;
        out_of_order_elements = 0;
        max_discrepancy = 0;

        // make previous boundary = last_sorted_zone
        // from subsequent inserts, last_sorted_zone will change but previous_boundary remains the same
        previous_boundary = current_zone - 1;

        // reset the subsorted_boundaries and its size counter
        memset(subsorted_boundaries, 0, sizeof(subsorted_boundaries));
        subsorted_size = 0;
        atleast_one_subsorted = false;
    }

public:
    bool query(_Key key)
    {
        bool flag = false;
        counters.osm_fence_queries += 1;
        std::string str_key = std::to_string(key);
        // if we are within buffer range...

        // init this flag to always true so we can use even without zonemaps
        bool within_buf_range = true;
#ifdef ZONES
        within_buf_range = key >= buf_min && key <= buf_max;
        counters.osm_fence_positive += 1;
#endif
        // if (key >= buf_min && key <= buf_max)
        if (within_buf_range)
        {
            // if (last_sorted_zone != current_zone - 1)
            bool within_unsorted_section_range = true;

#ifdef ZONES
            within_unsorted_section_range = !sorted_buffer && (key >= unsorted_min && key <= unsorted_max);
            // increment #. of unsorted section zonemap queries
            counters.unsorted_zonemap_queries += 1;
#endif

            if (within_unsorted_section_range)
            {
                // increment #. of unsorted section zonemap positives
                counters.unsorted_zonemap_positive += 1;

                // this means there is some unsortedness.
                counters.num_seq_scan += 1;

                // now we will query the global bf
                // we want to always continue probe if we don't have a global BF
                bool result = true;

                // check bloom filter to see if element can exist in unsorted section of the  buffer
#ifdef GLOBALBF

#ifdef OSMPROFILE
                auto start_bf = std::chrono::high_resolution_clock::now();
#endif
                result = filter->query(str_key);
#ifdef OSMPROFILE
                auto stop_bf = std::chrono::high_resolution_clock::now();
                auto duration_bf = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_bf - start_bf);
                bf_scan_time += duration_bf.count();
#endif
                // increment number of times global bf was global_bf_queried
                counters.global_bf_queried += 1;
#endif
                // if global BF says yes, then element is potentially in unsorted section of the buffer

                if (result)
                {

                    // if gbf returned a positive result
#ifdef GLOBALBF
                    // increment #. of times global bf returned positive
                    counters.global_bf_positive += 1;
#endif

                    int filled_pages = 0;
                    int last_boundary;
                    int current_boundary;

                    if (current_zone_element_count == num_per_zone)
                    {
                        current_boundary = current_zone;
                    }
                    else
                    {
                        current_boundary = current_zone - 1;
                    }

                    if (atleast_one_subsorted)
                    {
                        // number of filled pages post last sort will be currentzone - last boundary
                        last_boundary = subsorted_boundaries[subsorted_size - 1];
                        filled_pages = current_zone - last_boundary;
                    }
                    else
                    {
                        last_boundary = previous_boundary;
                        filled_pages = current_zone - previous_boundary;
                    }

                    double filled_percent = filled_pages / (double)(num_zones);
#ifdef OSMP
                    auto start_unsorted_sorting = std::chrono::high_resolution_clock::now();
#endif
                    if (filled_percent > fill_threshold)
                    {
                        // sort the pages from last boundary+1 to current boundary included
                        int start_index = (num_per_zone * (last_boundary + 1));
                        int end_index = (num_per_zone * (current_boundary + 1)) - 1; // -1 for starting from 0 index
                        assert(end_index < osmBufSize && end_index < osmCap);
                        std::sort(osmBuffer + start_index, osmBuffer + end_index + 1);
                        // update the zonemaps concerning last_boundary to current_boundary included
                        updateZonemaps(last_boundary + 1, current_boundary + 1);

                        atleast_one_subsorted = true;

                        // add the new boundary to subsorted_boundaries; remember it is the right side of the boundary (inclusive)
                        subsorted_boundaries[subsorted_size] = current_boundary;
                        subsorted_size += 1;

                        // update last_boundary to new last_boundary
                        last_boundary = subsorted_boundaries[subsorted_size - 1];
                    }

#ifdef OSMP
                    auto stop_unsorted_sorting = std::chrono::high_resolution_clock::now();
                    auto duration_unsorted_sorting = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_unsorted_sorting - start_unsorted_sorting);
                    unsorted_sorting_time += duration_unsorted_sorting.count();
#endif

                    // new modified procedure....
                    // first sequentially scan from last_boundary+1 till current_zone
                    // next, if subsorted_size > 0, then in a while loop do interpolation search on
                    // each subsorted section.
                    assert(last_boundary <= current_zone);

                    // sequentially scan zonemaps to see if a zone can contain the key

#ifdef OSMPROFILE
                    auto start_seq = std::chrono::high_resolution_clock::now();
#endif
                    int qualifying_zones[num_zones];
                    int num_qualified = 0;
#ifdef OSMPROFILE
                    auto start_zone_scan = std::chrono::high_resolution_clock::now();
#endif
                    // change start boundary from previous_boundary to last_boundary
                    // get_qualifying_zones(qualifying_zones, num_qualified, key, last_boundary + 1);
#ifdef OSMPROFILE
                    auto stop_zone_scan = std::chrono::high_resolution_clock::now();
                    auto duration_zone_scan = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_zone_scan - start_zone_scan);
                    zone_scan_time += duration_zone_scan.count();
#endif
#ifdef OSMP
                    auto start_sample = std::chrono::high_resolution_clock::now();
#endif
                    bool result = false;
                    // search within qualifying zones
                    // for (int i = 0; i < num_qualified; i++)
                    for (int i = current_zone; i >= last_boundary + 1; i--)
                    {
                        counters.num_zones_queried += 1;
                        // check if zone qualifies
                        if (key >= zones[i].first && key <= zones[i].second)
                        {
                            counters.num_zones_positive += 1;
                            // first query this zone's BF
                            bool zoneBFResult = true;
#ifdef SUBBFS
#ifdef OSMPROFILE
                            auto start_sublevelbf = std::chrono::high_resolution_clock::now();
#endif
                            // bool zoneBFResult = subfilters[qualifying_zones[i]]->query(str_key);
                            zoneBFResult = subfilters[i]->query(str_key);

#ifdef OSMPROFILE
                            auto stop_sublevelbf = std::chrono::high_resolution_clock::now();
                            auto duration_sublevelbf = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_sublevelbf - start_sublevelbf);
                            sublevel_bf_scan_time += duration_sublevelbf.count();
#endif

                            // increment #. of sublevel BF queries
                            counters.sublevel_bf_queried += 1;
#endif
                            if (zoneBFResult)
                            {
// increment #. of sublevel BF positives
#ifdef SUBBFS
                                counters.sublevel_bf_positive += 1;
#endif
                                flag = false;
                                // flag = searchWithinZone(key, qualifying_zones[i]);
                                flag = searchWithinZone(key, i);
                                // increment #. of sequential scans for zones
                                counters.seq_zone_queries += 1;

                                if (flag)
                                {
                                    result = true;

                                    // increment #. of sequential zone scan positives
                                    counters.seq_zone_positive += 1;

                                    // we can do early stopping now since we scan from right
                                    break;
                                }
                            }
                        }
                    }
#ifdef OSMP
                    auto stop_sample = std::chrono::high_resolution_clock::now();
                    auto duration_sample = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_sample - start_sample);
                    seq_search_time += duration_sample.count();
#endif
                    // since we found the element, return true
                    if (result)
                    {

                        counters.num_seq_found += 1;
                        counters.num_unsorted_positive += 1;

                        return true;
                    }

#ifdef OSMP
                    auto start_unsorted_inter = std::chrono::high_resolution_clock::now();
#endif
                    // if we didn't find the result here, we will have to now do interpolation search
                    // this interpolation search will be done on each of the sorted blocks after previous_boundary

                    // we initialize the number of blocks that we need to perform interpolation search on
                    // this will always be equal to number of boundaries we have (because last boundary is inclusive)
                    int num_interpolation_blocks = subsorted_size;

                    // we perform the interpolation search from the rightmost to the leftmost block
                    // this is because rightmost block will contain the latest information
                    int start_b = subsorted_size - 1;
                    int end_b = previous_boundary;
                    while (num_interpolation_blocks > 0)
                    {
                        // initialize the boundaries of each block
                        int right_boundary, left_boundary;
                        right_boundary = subsorted_boundaries[start_b];
                        int start_i;
                        if (start_b - 1 < 0)
                        {
                            left_boundary = previous_boundary + 1;
                            start_i = num_per_zone * (left_boundary);
                        }
                        else
                        {
                            left_boundary = subsorted_boundaries[start_b - 1];
                            start_i = num_per_zone * (left_boundary + 1);
                        }

                        // now that we have defined the boundaries in terms of zone ids, we need to calculate exact indexes

                        int end_i = num_per_zone * (right_boundary + 1) - 1;

                        // lets do some basic sanity checks
                        assert(end_i < osmBufSize);
                        assert(start_i < end_i);

                        // perform interpolation search of the key within this block
                        bool block_interpolation_result = false;
                        int f = interpolationSearchKey(key, start_i, end_i);
                        counters.subblock_interpolation_queries += 1;

                        // let's interpret the result of the interpolation search
                        if (f < 0)
                            block_interpolation_result = false;
                        else
                            block_interpolation_result = true;

                        // if we found the key in this block, we can simply stop the scan here itself
                        if (block_interpolation_result)
                        {
                            counters.subblock_interpolation_positive += 1;
                            counters.num_unsorted_positive += 1;
#ifdef OSMP
                            auto stop_unsorted_inter = std::chrono::high_resolution_clock::now();
                            auto duration_unsorted_inter = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_unsorted_inter - start_unsorted_inter);
                            bin_search_time += duration_unsorted_inter.count();
#endif
                            return true;
                        }

                        // for next iteration, start_b becomes start_b - 1
                        start_b -= 1;
                        num_interpolation_blocks -= 1;

                        if (start_b < 0 || num_interpolation_blocks < 0)
                            break;
                    }

#ifdef OSMP
                    auto stop_unsorted_inter = std::chrono::high_resolution_clock::now();
                    auto duration_unsorted_inter = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_unsorted_inter - start_unsorted_inter);
                    bin_search_time += duration_unsorted_inter.count();
#endif
                }
            } // close the unsorted section scan if gbf approved. If gbf returned false, we directly come here to the sorted section
              // next check for sorted section

            // to have the ability to turn off the zonemaps, let us initialize this result to true always.
            // this way, if we have zonemaps turned off, we will always go inside the following if condition
            // if zonemaps are used, then this flag will be set based on our condition
            bool within_sorted_section_range = true;

#ifdef ZONES
            within_sorted_section_range = previous_boundary > 0 && (key >= sorted_min && key <= sorted_max);
            counters.sorted_zonemap_queries += 1;
#endif

            if (within_sorted_section_range)
            {
                counters.sorted_zonemap_positive += 1;
                counters.num_bin_scan += 1;
#ifdef OSMP
                auto start_bin = std::chrono::high_resolution_clock::now();
#endif
                // can do binary search
                // bool flag = searchKey(key);
                flag = false;
                int f = interpolationSearchKey(key);
                if (f < 0)
                    flag = false;
                else
                    flag = true;
#ifdef OSMP
                auto stop_bin = std::chrono::high_resolution_clock::now();
                auto duration_bin = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_bin - start_bin);
                bin_search_time += duration_bin.count();
#endif
                if (flag)
                {

                    counters.num_bin_found += 1;

                    return true;
                }
            }
        }
        return false;
    }

    int findFirstGreaterThan(_Key key, int low, int high)
    {
        int ans = -1;

        while (low <= high)
        {

            // int pos = low + ((double)(high - low) / (osmBuffer[high].first - osmBuffer[low].first) * (key - osmBuffer[low].first));
            int pos = (low + high) / 2;

            if (osmBuffer[pos].first < key)
            {
                low = pos + 1;
            }
            else
            {
                ans = pos;
                high = pos - 1;
            }
        }
        return ans;
    }

    int findFirstStrictlyGreaterThan(_Key key, int low, int high)
    {
        int ans = -1;

        while (low <= high)
        {

            // int pos = low + ((double)(high - low) / (osmBuffer[high].first - osmBuffer[low].first) * (key - osmBuffer[low].first));
            int pos = (low + high) / 2;

            if (osmBuffer[pos].first <= key)
            {
                low = pos + 1;
            }
            else
            {
                ans = pos;
                high = pos - 1;
            }
        }
        return ans;
    }

    void merge(std::vector<std::pair<_Key, _Value>> ar1, std::vector<std::pair<_Key, _Value>> ar2, int m, int n, std::vector<std::pair<_Key, _Value>> ar3)
    {
        int i = 0, j = 0, k = ar3.size();

        while (i < m && j < n)
        {
            if (ar1[i].first < ar2[j].first)
            {
                ar3.push_back(ar1.at(i));
                i++;
            }
            else
            {
                ar3.push_back(ar2.at(j));
                j++;
            }
        }

        while (i < m)
        {
            ar3.push_back(ar1.at(i));
            i++;
        }

        while (j < n)
        {
            ar3.push_back(ar2.at(j));
            j++;
        }
    }

    std::vector<std::pair<_Key, _Value>> merge(std::vector<std::pair<_Key, _Value>> array, int const left, int const mid, int const right)
    {
        auto const subArrayOne = mid - left + 1;
        auto const subArrayTwo = right - mid;

        // Create temp arrays
        auto *leftArray = new std::pair<_Key, _Value>[subArrayOne],
             *rightArray = new std::pair<_Key, _Value>[subArrayTwo];

        // Copy data to temp arrays leftArray[] and rightArray[]
        for (auto i = 0; i < subArrayOne; i++)
            leftArray[i] = array.at(left + i);
        for (auto j = 0; j < subArrayTwo; j++)
            rightArray[j] = array.at(mid + 1 + j);

        auto indexOfSubArrayOne = 0,   // Initial index of first sub-array
            indexOfSubArrayTwo = 0;    // Initial index of second sub-array
        int indexOfMergedArray = left; // Initial index of merged array

        // Merge the temp arrays back into array[left..right]
        while (indexOfSubArrayOne < subArrayOne && indexOfSubArrayTwo < subArrayTwo)
        {
            if (leftArray[indexOfSubArrayOne] <= rightArray[indexOfSubArrayTwo])
            {
                array.at(indexOfMergedArray) = leftArray[indexOfSubArrayOne];
                indexOfSubArrayOne++;
            }
            else
            {
                array.at(indexOfMergedArray) = rightArray[indexOfSubArrayTwo];
                indexOfSubArrayTwo++;
            }
            indexOfMergedArray++;
        }
        // Copy the remaining elements of
        // left[], if there are any
        while (indexOfSubArrayOne < subArrayOne)
        {
            if (indexOfMergedArray < array.size())
                array.at(indexOfMergedArray) = leftArray[indexOfSubArrayOne];
            else
                array.push_back(leftArray[indexOfSubArrayOne]);
            indexOfSubArrayOne++;
            indexOfMergedArray++;
        }
        // Copy the remaining elements of
        // right[], if there are any
        while (indexOfSubArrayTwo < subArrayTwo)
        {
            if (indexOfMergedArray < array.size())
                array[indexOfMergedArray] = rightArray[indexOfSubArrayTwo];
            else
                array.push_back(rightArray[indexOfSubArrayTwo]);
            indexOfSubArrayTwo++;
            indexOfMergedArray++;
        }
        return array;
    }

    std::vector<std::pair<_Key, _Value>> rangeQuery(_Key low, _Key high)
    {
        std::vector<std::pair<_Key, _Value>> result;
        // first check the buffer's ranges
        if (!(high < buf_min) && !(low > buf_max))
        {
            // first check if we have range_sorted set to false
            int last_boundary = range_sorted_boundary;
            if (!range_sorted)
            {

                if (atleast_one_subsorted)
                {
                    // if we have query-sorted components, we sort from the last sorted component to current
                    last_boundary = subsorted_boundaries[subsorted_size - 1];
                }
                else
                {
                    // if we don't have query-sorted components, we sort from previous_boundary
                    last_boundary = previous_boundary;
                }

                // first sort the unsorted block starting from last_boundary+1
                int start_index = (num_per_zone * (last_boundary + 1));
                // sort from start_index to osmBufSize
                std::stable_sort(osmBuffer + start_index, osmBuffer + osmBufSize);
                range_sorted = true;
                range_sorted_boundary = last_boundary;
            }

            // since our last part is also going to be sorted, we can do smart search
            int start_index = (num_per_zone * (range_sorted_boundary + 1));
            int end_block = 0;
            if (!(high < osmBuffer[start_index].first) && !(low > osmBuffer[osmBufSize - 1].first))
            {
                int end_of_block = osmBufSize - 1;
                // interpolation search for first key greater than equal to low
                int start = findFirstGreaterThan(low, 0, end_of_block);

                // interpolation search for first key strictly greater than high
                int end = end_of_block;
                if (high < osmBuffer[end_of_block].first)
                {
                    end = findFirstStrictlyGreaterThan(high, 0, end_of_block) - 1; // first key > high - 1 will be included
                }

                // include all elements from start to end
                result.insert(result.end(), &osmBuffer[start], &osmBuffer[end + 1]);
            }

            last_boundary = previous_boundary;
            // check if we have subsorted blocks
            if (atleast_one_subsorted)
            {
                // for every subsorted block check its ranges
                int num_interpolation_blocks = subsorted_size;

                // we perform the interpolation search from the rightmost to the leftmost block
                // this is because rightmost block will contain the latest information
                int start_b = subsorted_size - 1;
                int end_b = previous_boundary;
                while (num_interpolation_blocks > 0)
                {
                    // initialize the boundaries of each block
                    int right_boundary, left_boundary;
                    right_boundary = subsorted_boundaries[start_b];
                    int start_i;
                    if (start_b - 1 < 0)
                    {
                        left_boundary = previous_boundary + 1;
                        start_i = num_per_zone * (left_boundary);
                    }
                    else
                    {
                        left_boundary = subsorted_boundaries[start_b - 1];
                        start_i = num_per_zone * (left_boundary + 1);
                    }

                    // now that we have defined the boundaries in terms of zone ids, we need to calculate exact indexes

                    int end_i = num_per_zone * (right_boundary) + num_per_zone - 1;

                    // lets do some basic sanity checks
                    assert(end_i < osmBufSize);
                    assert(start_i < end_i);

                    // we have start and end boundaries, these will form the zonemap for this block
                    // check if our range falls within this block
                    // if (low >= start_i && low <= end_i)
                    if (!(high < osmBuffer[start_i].first) && !(low > osmBuffer[end_i].first))
                    {
                        // interpolation search for first key greater than equal to low
                        int start = findFirstGreaterThan(low, start_i, end_i);

                        // interpolation search for first key strictly greater than high
                        int end = end_i;
                        if (high < osmBuffer[end_i].first)
                        {
                            end = findFirstStrictlyGreaterThan(high, start_i, end_i) - 1; // first key > high - 1 will be included
                        }

                        // include all elements from start to end
                        std::vector<std::pair<_Key, _Value>> res;
                        int old_size = result.size();
                        result.insert(result.end(), &osmBuffer[start], &osmBuffer[end + 1]);

                        // now until old_size is one sorted run and after that to new_size is next sorted run
                        // we can merge it
                        result = merge(result, 0, old_size - 1, result.size() - 1);
                    }

                    // for next iteration, start_b becomes start_b - 1
                    start_b -= 1;
                    num_interpolation_blocks -= 1;

                    if (start_b < 0 || num_interpolation_blocks < 0)
                        break;
                }
                last_boundary = subsorted_boundaries[subsorted_size - 1];
            }

            // get elements from sorted section until previous boundary (included)

            // check if within range of sorted section

            // if (low >= sorted_min && low <= sorted_max)
            if (!(high < sorted_min) && !(low > sorted_max))
            {
                int end_of_block = (num_per_zone * (previous_boundary + 1)) - 1;
                // interpolation search for first key greater than equal to low
                int start = findFirstGreaterThan(low, 0, end_of_block);

                // interpolation search for first key strictly greater than high
                int end = end_of_block;
                if (high < osmBuffer[end_of_block].first)
                {
                    end = findFirstStrictlyGreaterThan(high, 0, end_of_block) - 1; // first key > high - 1 will be included
                }

                // include all elements from start to end
                // std::vector<std::pair<_Key, _Value>> res;
                int old_size = result.size();
                result.insert(result.end(), &osmBuffer[start], &osmBuffer[end + 1]);

                // now until old_size is one sorted run and after that to new_size is next sorted run
                // we can merge it
                result = merge(result, 0, old_size - 1, result.size() - 1);
            }

        }

        return result;
    }
};
#endif
