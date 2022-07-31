#ifndef TOLORENCED_STD_H
#define TOLORENCED_STD_H

#include "outlier_detector.h"

template<typename key_type>
class TolorencedStdDetector : public OutlierDetector<key_type>
{

    // The user defined number of std that used for calculating acceptable range
    float num_stdev;

    // The total number of entry in the sorted tree. Used for calculating standard deviation
    long long n;

    // The sum of every inserted keys in the sorted tree. Used for calculating standard deviation
    long long sum;

    // The sum of square of every inserted keys in the sorted tree. Used for calculating standard deviation
    long long sum_square;

    // Current maximum standard deviation
    long long max_sd;

    // Current calculated standard deviation
    long double std;

    // tolorenced standard deviation. calculated by std * tolorance_factor
    long double toloranced_std;
    
    // Tolorance factor to modify standard deviation in order to determine whether some key is outlier or not
    float tolorance_factor;

public:

    TolorencedStdDetector(float _tolerance_factor, float _num_stdev) : tolorance_factor(_tolerance_factor), num_stdev(_num_stdev)
    {
        n = 0;
        sum = 0;
        sum_square = 0;
        max_sd = 0;
        std = 0;
        toloranced_std = 0;
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

    bool is_outlier(key_type key, int tree_size)
    {

        if ((tree_size < 30) || (long double)key <= (long double)sum / n + num_stdev * toloranced_std)
        {
            update(key);
            update_tolorance_factor();
            // calculating the new standard deviation
            long double x = (long double) sum_square / n;
            long double y = (long double) sum / n * (long double) sum / n;
            std = (long double) sqrt(x - y);
            toloranced_std = (long double) std * tolorance_factor;
            return false;
        }

        return true;

    }

};

#endif