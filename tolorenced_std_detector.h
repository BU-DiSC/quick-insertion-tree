#ifndef TOLORENCED_STD_H
#define TOLORENCED_STD_H

#include "outlier_detector.h"

template<typename key_type>
class TolorencedStdDetector : public OutlierDetector<key_type>
{

    // The user defined number of std that used for calculating acceptable range
    long long num_stdev;

    // The total number of entry in the sorted tree. Used for calculating standard deviation
    long long n;

    // The sum of every inserted keys in the sorted tree. Used for calculating standard deviation
    long long sum;

    // The sum of square of every inserted keys in the sorted tree. Used for calculating standard deviation
    long long sum_square;



    // Current calculated standard deviation
    long double std;

    // tolorenced standard deviation. calculated by std * tolorance_factor
    long double toloranced_std;
    
    // Tolorance factor to modify standard deviation in order to determine whether some key is outlier or not
    float tolorance_factor;



    // int value which denote the number of nodes we are using to calculate standard deviation
    int k;

    // Next index to update sums and sumSqs buffer, used for creating a circular array
    int next_idx;

    // Current number of nodes
    int num_of_nodes;

    // A vector storing every leaf size in sorted tree
    std::vector<int> leaf_size;

    // Sums of keys of each node
    std::vector<long long> sums_of_keys;

    // Sums of squared keys of each node
    std::vector<long long> sums_of_squares;

public:

    TolorencedStdDetector(float _tolerance_factor, float _num_stdev, int _k) : tolorance_factor(_tolerance_factor), num_stdev(_num_stdev), k(_k)
    {
        n = 0;
        sum = 0;
        sum_square = 0;

        std = 0;
        toloranced_std = 0;

        if (k > 0)
        {
            next_idx = 0;
            leaf_size.resize(k);
            sums_of_keys.resize(k);
            sums_of_squares.resize(k);
        }
    }

    void update(key_type key)
    {
        // update count, sum, and sum_square based on the new key if the new key is about to insert into the sorted tree
        n++;
        sum+=key;
        sum_square+=(long long)key * (long long)key;
    }

    void update_tolorance_factor()
    {
        // TODO: come up with some way to update tolorance factor in runtime

    }

    void update(int size, long long* stats)
    {
        // update function for last k nodes std outlier

        // remove the poped node from count, sum, and sum_square
        n -= leaf_size[next_idx];
        sum -= sums_of_keys[next_idx];
        sum_square -= sums_of_squares[next_idx];

        // update buffer
        leaf_size[next_idx] = size;
        sums_of_keys[next_idx] = stats[0];
        sums_of_squares[next_idx] = stats[1];

        // update the new node to count, sum, and sum_square
        n += size;
        sum += stats[0];
        sum_square += stats[1];

        // update the next_idx that indicates the next node to pop
        next_idx = (++next_idx) % k;
    }

    bool is_outlier(key_type key, int tree_size)
    {

        // if ((tree_size < 30) || (long double)key <= (long double)sum / n + num_stdev * toloranced_std)
        // {
        //     update(key);
        //     update_tolorance_factor();
        //     // calculating the new standard deviation
        //     long double x = (long double) sum_square / n;
        //     long double y = (long double) sum / n * (long double) sum / n;
        //     std = (long double) sqrt(x - y);
        //     toloranced_std = (long double) std * tolorance_factor;
        //     return false;
        // }

        if (n < 30)
        {
            if (k <= 0) update(key);
            return false;
        }

        // calculating standard deviation
        long double x = (long double) sum_square / n;
        long double y = (long double) sum / n * (long double) sum / n;
        std = (long double) sqrt(x - y);
        toloranced_std = tolorance_factor * std;

        if (key <= (long double) sum / n + num_stdev * toloranced_std)
        {
            if (k <= 0) update(key);

            return false;
        }

        return true;

    }

};

#endif