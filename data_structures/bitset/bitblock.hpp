#ifndef MEMGRAPH_DATA_STRUCTURES_BITBLOCK_HPP
#define MEMGRAPH_DATA_STRUCTURES_BITBLOCK_HPP

#include <cstdlib>
#include <cassert>
#include <atomic>
#include <unistd.h>

template<class block_t = uint8_t,
         size_t N = 1>
struct BitBlock
{
    BitBlock() : block(0) {}

    static constexpr size_t bits = sizeof(block_t) * 8;
    static constexpr size_t size = bits / N;

    // e.g. if N = 2,
    // mask = 11111111 >> 6 = 00000011
    static constexpr block_t mask = (block_t)(-1) >> (bits - N);

    uint8_t at(size_t n)
    {
        assert(n < size);

        block_t b = block.load(std::memory_order_relaxed);
        return (b >> n * N) & mask;
    }

    // caution! this method assumes that the value on the sub-block n is 0..0!
    void set(size_t n, block_t value)
    {
        assert(n < size);
        assert(value < (1UL << N));
        block_t b, new_value;

        while(true)
        {
            b = block.load(std::memory_order_relaxed);
            new_value = b | (value << n * N);
        
            if(block.compare_exchange_weak(b, new_value,
                                           std::memory_order_release,
                                           std::memory_order_relaxed))
            {
                break;
            }

            // reduces contention and works better than pure while
            usleep(250);
        }
    }

    void clear(size_t n)
    {
        assert(n < size);
        block_t b, new_value;

        while(true)
        {
            b = block.load(std::memory_order_relaxed);
            new_value = b & ~(mask << n * N);
            
            if(block.compare_exchange_weak(b, new_value,
                                           std::memory_order_release,
                                           std::memory_order_relaxed))
            {
                break;
            }

            // reduces contention and works better than pure while
            usleep(250);
        }
    }

    std::atomic<block_t> block;
};

#endif
