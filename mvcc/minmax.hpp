#ifndef MEMGRAPH_MVCC_MINMAX_HPP
#define MEMGRAPH_MVCC_MINMAX_HPP

#include <atomic>

namespace mvcc
{

template <class T>
class MinMax
{
public:
    MinMax() : min_(0), max_(0) {}

    MinMax(T min, T max) : min_(min), max_(max) {}

    T min()
    {
        return min_.load(std::memory_order_relaxed);
    }

    void min(T value)
    {
        min_.store(value, std::memory_order_relaxed);
    }

    T max()
    {
        return max_.load(std::memory_order_relaxed);
    }

    void max(T value)
    {
        max_.store(value, std::memory_order_relaxed);
    }

private:
    std::atomic<T> min_, max_;
};

}

#endif
