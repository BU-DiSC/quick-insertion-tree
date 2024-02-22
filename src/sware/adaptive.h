#ifndef ADAPTIVE_INCLUDE
#define ADAPTIVE_INCLUDE

#include <iostream>

#include "minheap.h"

template <typename key_type>
bool adaptive_sort(std::pair<key_type, key_type> R[], uint n, int k, int l,
                   std::pair<key_type, key_type> OUT[]) {
    MinHeap S(n), G(n);
    std::pair<key_type, key_type> *TMP = new std::pair<key_type, key_type>[n];
    int i_read, i_write;

    // insert first k+l+1 tuples from R into S
    for (int i = 0; i < k + l; i++) {
        bool flag = S.insertKey(R[i]);
        if (!flag) {
            return false;
        }
    }

    i_write = 0;

    // first pass
    for (i_read = S.size(); i_read < n; i_read++) {
        if (S.size() == 0) {
            // Fail state
            std::cout << "Fail State Reached!" << std::endl;
            return 0;
        }

        // get minimum element of heap
        // note extractMin() also removes element from heap
        // so this essentially performs S <- (S \ {last_written})
        std::pair<key_type, key_type> last_written = S.extractMin();

        // TMP.push_back(last_written);
        TMP[i_write] = last_written;

        i_write++;

        if (R[i_read].first >= last_written.first) {
            S.insertKey(R[i_read]);
        } else {
            G.insertKey(R[i_read]);
        }
    }

    // append all tuples in S to TMP in sorted order
    int s_size = S.size();

    for (int i = 0; i < s_size; i++) {
        // extractMin() of S will get the minimum element at every run
        // and also remove the element from the heap
        // TMP.push_back(S.extractMin());
        TMP[i_write++] = S.extractMin();
    }

    i_write = 0;
    int g_size = G.size();

    for (i_read = 0; i_read < n - G.size(); i_read++) {
        // second pass
        std::pair<key_type, key_type> x = G.getMin();
        std::pair<key_type, key_type> tmp = TMP[i_read];
        if (x.first > TMP[i_read].first || G.size() == 0) {
            // OUT.push_back(TMP[i_read]);
            OUT[i_write] = TMP[i_read];
        } else {
            // OUT.push_back(x);
            OUT[i_write] = x;

            // do G <- (G\{x}) U {TMP[i_read]}
            G.extractMin();  // ( G\{x} )
            G.insertKey(TMP[i_read]);
        }

        i_write++;
    }

    g_size = G.size();

    for (int i = 0; i < g_size; i++) {
        // OUT.push_back(G.extractMin());
        OUT[i_write++] = G.extractMin();
    }

    return true;
}

bool adaptive_sort(std::pair<unsigned long, unsigned long> R[], unsigned long n,
                   unsigned long k, unsigned long l,
                   std::pair<unsigned long, unsigned long> OUT[]) {
    MinHeap S(n), G(n);
    std::pair<unsigned long, unsigned long> *TMP =
        new std::pair<unsigned long, unsigned long>[n];
    unsigned long i_read, i_write;

    // insert first k+l+1 tuples from R into S
    for (unsigned long i = 0; i < k + l; i++) {
        bool flag = S.insertKey(R[i]);
        if (!flag) {
            return false;
        }
    }

    i_write = 0;

    // first pass
    for (i_read = S.size(); i_read < n; i_read++) {
        if (S.size() == 0) {
            // Fail state
            std::cout << "Fail State Reached!" << std::endl;
            return 0;
        }

        // get minimum element of heap
        // note extractMin() also removes element from heap
        // so this essentially performs S <- (S \ {last_written})
        std::pair<unsigned long, unsigned long> last_written = S.extractMin();

        // TMP.push_back(last_written);
        TMP[i_write] = last_written;

        i_write++;

        if (R[i_read].first >= last_written.first) {
            S.insertKey(R[i_read]);
        } else {
            G.insertKey(R[i_read]);
        }
    }

    // append all tuples in S to TMP in sorted order
    unsigned long s_size = S.size();

    for (unsigned long i = 0; i < s_size; i++) {
        // extractMin() of S will get the minimum element at every run
        // and also remove the element from the heap
        // TMP.push_back(S.extractMin());
        TMP[i_write++] = S.extractMin();
    }

    i_write = 0;
    unsigned long g_size = G.size();

    for (i_read = 0; i_read < n - G.size(); i_read++) {
        // second pass
        std::pair<unsigned long, unsigned long> x = G.getMin();
        std::pair<unsigned long, unsigned long> tmp = TMP[i_read];
        if (x.first > TMP[i_read].first || G.size() == 0) {
            // OUT.push_back(TMP[i_read]);
            OUT[i_write] = TMP[i_read];
        } else {
            // OUT.push_back(x);
            OUT[i_write] = x;

            // do G <- (G\{x}) U {TMP[i_read]}
            G.extractMin();  // ( G\{x} )
            G.insertKey(TMP[i_read]);
        }

        i_write++;
    }

    g_size = G.size();

    for (unsigned long i = 0; i < g_size; i++) {
        // OUT.push_back(G.extractMin());
        OUT[i_write++] = G.extractMin();
    }

    return true;
}

#endif