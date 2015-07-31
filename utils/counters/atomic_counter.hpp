#ifndef MEMGRAPH_UTILS_COUNTERS_ATOMIC_COUNTER_HPP
#define MEMGRAPH_UTILS_COUNTERS_ATOMIC_COUNTER_HPP

#include <atomic>

template <class T>
class AtomicCounter
{
public:
    AtomicCounter(T initial) : counter(initial) {}

    T next()
    {
        return counter.fetch_add(1, std::memory_order_relaxed);
    }

private:
    std::atomic<T> counter;
};

#endif
