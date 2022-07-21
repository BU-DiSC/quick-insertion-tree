#ifndef STDEV_DETECTOR_H
 #define STDEV_DETECTOR_H

 #include "outlier_detector.h"

 template<typename key_type>
 class StdevDetector : public OutlierDetector<key_type>
 {
     float num_stdev;
     long long n;
     long long sum;
     long long sumSq;
     long long max_sd;
     long long sd;

     public:

     StdevDetector(float _num_stdev)
     {
        num_stdev = _num_stdev;
        n=0;
        sum = 0;
        sumSq = 0;
        max_sd = 0;
        sd = 0;

     }

     void updateStdev(key_type key){
         n++;
         sum+=key;
         sumSq += (long long)key* (long long)key;
     }
    
    bool is_outlier(key_type key, int tree_size)
    {
        if ((tree_size < 30) || (long long)key <= (long long)sum / n + num_stdev * sd)
        {
            updateStdev(key);
            long long first = (long long) sumSq/n;
            long long second = (long long)sum/n*sum/n;
            sd = (long long)sqrt(first - second);
            std::cout<<"insert key : "<< key << "    sd: " << sd << "   range = " << (long long)sum / n + num_stdev * sd << std::endl;
            return false;
        }
        std::cout << "key beyond range: "<< key << std::endl;

        return true;
    }

    // bool is_outlier(key_type key, int tree_size)
    // {

    //     long long sd = 0;

    //     if ((tree_size < 30) || (long long)key <= (long long)sum / n + num_stdev * max_sd)
    //     {
    //         updateStdev(key);
    //         if (tree_size > 2)
    //         {
    //             // sd = sumSq/(n-1) - sum /n*sum/(n-1);
    //             long long first = (long long) sumSq/n;
    //             long long second = (long long)sum/n*sum/n;
    //             sd = (long long)sqrt(first - second);
               
    //             if (sd > max_sd)
    //             {
    //                 max_sd = sd;
    //             }
    //         }
    //         // std::cout<<"inset key : "<< key << "    sd: " << sd <<"    mean: " << sum/n << "     range: "<< sum/n+num_stdev*sd<< std::endl;
    //         std::cout << "insert key : " << key << "    max_sd: " << max_sd << "    sd: " << sd << "     range: " << (long long)sum / n + num_stdev * max_sd << std::endl;
    //         return false;
    //     }
        
    //     std::cout << "skipped key : "<< key << std::endl;


    //     return true;
    // }



 };

 #endif 