#ifndef ADAPTIVE_INCLUDE
#define ADAPTIVE_INCLUDE

#include <iostream>
#include "minheap.h"

bool adaptive_sort(std::pair<int, int> R[], int n, int k, int l, std::pair<int, int> OUT[])
{
    MinHeap S(n), G(n);
    std::pair<int, int> *TMP = new std::pair<int, int>[n];
    int i_read, i_write;

    //insert first k+l+1 tuples from R into S
    for (int i = 0; i < k + l; i++)
    {
        bool flag = S.insertKey(R[i]);
        if (!flag)
        {
            return false;
        }
    }

    i_write = 0;

    //first pass
    for (i_read = S.size(); i_read < n; i_read++)
    {
        if (S.size() == 0)
        {
            //Fail state
            std::cout << "Fail State Reached!" << std::endl;
            return 0;
        }

        // get minimum element of heap
        //note extractMin() also removes element from heap
        // so this essentially performs S <- (S \ {last_written})
        std::pair<int, int> last_written = S.extractMin();

        //TMP.push_back(last_written);
        TMP[i_write] = last_written;

        i_write++;

        if (R[i_read].first >= last_written.first)
        {
            S.insertKey(R[i_read]);
        }
        else
        {
            G.insertKey(R[i_read]);
        }
    }

    //append all tuples in S to TMP in sorted order
    int s_size = S.size();

    for (int i = 0; i < s_size; i++)
    {
        //extractMin() of S will get the minimum element at every run
        //and also remove the element from the heap
        //TMP.push_back(S.extractMin());
        TMP[i_write++] = S.extractMin();
    }

    i_write = 0;
    int g_size = G.size();

    for (i_read = 0; i_read < n - G.size(); i_read++)
    {
        //second pass
        std::pair<int, int> x = G.getMin();
        std::pair<int, int> tmp = TMP[i_read];
        if (x.first > TMP[i_read].first || G.size() == 0)
        {
            //OUT.push_back(TMP[i_read]);
            OUT[i_write] = TMP[i_read];
        }
        else
        {

            // OUT.push_back(x);
            OUT[i_write] = x;

            // do G <- (G\{x}) U {TMP[i_read]}
            G.extractMin(); // ( G\{x} )
            G.insertKey(TMP[i_read]);
        }

        i_write++;
    }

    g_size = G.size();

    for (int i = 0; i < g_size; i++)
    {
        // OUT.push_back(G.extractMin());
        OUT[i_write++] = G.extractMin();
    }

    return true;
}

bool adaptive_sort(std::pair<long, long> R[], long n, long k, long l, std::pair<long, long> OUT[])
{
    MinHeap S(n), G(n);
    std::pair<long, long> *TMP = new std::pair<long, long>[n];
    long i_read, i_write;

    //insert first k+l+1 tuples from R into S
    for (long i = 0; i < k + l; i++)
    {
        bool flag = S.insertKey(R[i]);
        if (!flag)
        {
            return false;
        }
    }

    i_write = 0;

    //first pass
    for (i_read = S.size(); i_read < n; i_read++)
    {
        if (S.size() == 0)
        {
            //Fail state
            std::cout << "Fail State Reached!" << std::endl;
            return 0;
        }

        // get minimum element of heap
        //note extractMin() also removes element from heap
        // so this essentially performs S <- (S \ {last_written})
        std::pair<long, long> last_written = S.extractMin();

        //TMP.push_back(last_written);
        TMP[i_write] = last_written;

        i_write++;

        if (R[i_read].first >= last_written.first)
        {
            S.insertKey(R[i_read]);
        }
        else
        {
            G.insertKey(R[i_read]);
        }
    }

    //append all tuples in S to TMP in sorted order
    long s_size = S.size();

    for (long i = 0; i < s_size; i++)
    {
        //extractMin() of S will get the minimum element at every run
        //and also remove the element from the heap
        //TMP.push_back(S.extractMin());
        TMP[i_write++] = S.extractMin();
    }

    i_write = 0;
    long g_size = G.size();

    for (i_read = 0; i_read < n - G.size(); i_read++)
    {
        //second pass
        std::pair<long, long> x = G.getMin();
        std::pair<long, long> tmp = TMP[i_read];
        if (x.first > TMP[i_read].first || G.size() == 0)
        {
            //OUT.push_back(TMP[i_read]);
            OUT[i_write] = TMP[i_read];
        }
        else
        {

            // OUT.push_back(x);
            OUT[i_write] = x;

            // do G <- (G\{x}) U {TMP[i_read]}
            G.extractMin(); // ( G\{x} )
            G.insertKey(TMP[i_read]);
        }

        i_write++;
    }

    g_size = G.size();

    for (long i = 0; i < g_size; i++)
    {
        // OUT.push_back(G.extractMin());
        OUT[i_write++] = G.extractMin();
    }

    return true;
}

#endif