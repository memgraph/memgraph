#ifndef MEMGRAPH_DATA_STRUCTURES_BITSET_DYNAMIC_BITSET_HPP
#define MEMGRAPH_DATA_STRUCTURES_BITSET_DYNAMIC_BITSET_HPP

#include <vector>
#include <mutex>
#include <unistd.h>

#include "sync/spinlock.hpp"
#include "bitblock.hpp"

template <class block_t,
          size_t N,
          class lock_t>
class DynamicBitset
{
    using Block = BitBlock<block_t, N>;

public:
    DynamicBitset(size_t n) : data(container_size(n)) {}

    void resize(size_t n)
    {
        auto guard = acquire();
        data.resize(container_size(n));
    }

    size_t size() const
    {
        return data.size();
    }

    block_t at(size_t n)
    {
        return data[n / Block::size].at(n % Block::size);
    }

    void set(size_t n, block_t value)
    {
        data[n / Block::size].set(n % Block::size, value);
    }

private:

    std::unique_lock<lock_t> acquire()
    {
        return std::unique_lock<lock_t>(lock);
    }

    size_t container_size(size_t num_elems)
    {
        return (num_elems + N - 1) / N;
    }
    
    std::vector<Block> data;

    lock_t lock;
};

#endif
