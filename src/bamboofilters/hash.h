#ifndef HASH_H
#define HASH_H

#include <cstdint>
#include <cstring>

static void mix(uint32_t &a, uint32_t &b, uint32_t &c) {
    a -= b;
    a -= c;
    a ^= (c >> 13);
    b -= c;
    b -= a;
    b ^= (a << 8);
    c -= a;
    c -= b;
    c ^= (b >> 13);
    a -= b;
    a -= c;
    a ^= (c >> 12);
    b -= c;
    b -= a;
    b ^= (a << 16);
    c -= a;
    c -= b;
    c ^= (b >> 5);
    a -= b;
    a -= c;
    a ^= (c >> 3);
    b -= c;
    b -= a;
    b ^= (a << 10);
    c -= a;
    c -= b;
    c ^= (b >> 15);
}

namespace hash {

    uint32_t BOBHash(const void *buf, size_t len, uint32_t primeNum) {
        const char *str = (const char *) buf;
        //register ub4 a,b,c,len;
        uint32_t a, b, c;
        /* Set up the internal state */
        //len = length;
        a = b = 0x9e3779b9; /* the golden ratio; an arbitrary value */

        c = primeNum; /* the previous hash value */

        /*---------------------------------------- handle most of the key */
        while (len >= 12) {
            a += (str[0] + ((uint32_t) str[1] << 8) + ((uint32_t) str[2] << 16) +
                  ((uint32_t) str[3] << 24));
            b += (str[4] + ((uint32_t) str[5] << 8) + ((uint32_t) str[6] << 16) +
                  ((uint32_t) str[7] << 24));
            c += (str[8] + ((uint32_t) str[9] << 8) + ((uint32_t) str[10] << 16) +
                  ((uint32_t) str[11] << 24));
            mix(a, b, c);
            str += 12;
            len -= 12;
        }

        /*------------------------------------- handle the last 11 bytes */
        c += len;
        switch (len) /* all the case statements fall through */
        {
            case 11:
                c += ((uint32_t) str[10] << 24);
            case 10:
                c += ((uint32_t) str[9] << 16);
            case 9:
                c += ((uint32_t) str[8] << 8);
                /* the first byte of c is reserved for the length */
            case 8:
                b += ((uint32_t) str[7] << 24);
            case 7:
                b += ((uint32_t) str[6] << 16);
            case 6:
                b += ((uint32_t) str[5] << 8);
            case 5:
                b += str[4];
            case 4:
                a += ((uint32_t) str[3] << 24);
            case 3:
                a += ((uint32_t) str[2] << 16);
            case 2:
                a += ((uint32_t) str[1] << 8);
            case 1:
                a += str[0];
                /* case 0: nothing left to add */
        }
        mix(a, b, c);
        /*-------------------------------------------- report the result */
        return c;
    }

    uint32_t MurmurHash2(const void *key, size_t len, uint32_t seed) {
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.

        const uint32_t m = 0x5bd1e995;
        const int r = 24;

        // Initialize the hash to a 'random' value

        uint32_t h = seed ^ len;

        // Mix 4 bytes at a time into the hash

        const auto *data = (const unsigned char *) key;

        while (len >= 4) {
            uint32_t k = *(uint32_t *) data;

            k *= m;
            k ^= k >> r;
            k *= m;

            h *= m;
            h ^= k;

            data += 4;
            len -= 4;
        }

        // Handle the last few bytes of the input array

        switch (len) {
            case 3:
                h ^= data[2] << 16;
            case 2:
                h ^= data[1] << 8;
            case 1:
                h ^= data[0];
                h *= m;
        }

        // Do a few final mixes of the hash to ensure the last few
        // bytes are well-incorporated.

        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;

        return h;
    }

}

#endif
