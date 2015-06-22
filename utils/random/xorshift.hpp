#ifndef MEMGRAPH_UTILS_RANDOM_XORSHIFT_HPP
#define MEMGRAPH_UTILS_RANDOM_XORSHIFT_HPP

#include <cstdlib>
#include <random>

namespace xorshift
{
    static uint64_t x, y, z;

    void init()
    {
        // use a slow, more complex rnd generator to initialize a fast one
        // make sure to call this before requesting any random numbers!
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<unsigned long long> dist;

        x = dist(gen);
        y = dist(gen);
        z = dist(gen);
    }

    uint64_t next()
    {
        // period 2^96 - 1
        uint64_t t;

        x ^= x << 16;
        x ^= x >> 5;
        x ^= x << 1;

        t = x, x = y, y = z;

        return t ^ x ^ y;
    }
}

#endif
