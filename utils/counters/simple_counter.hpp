#ifndef MEMGRAPH_UTILS_COUNTERS_SIMPLE_COUNTER_HPP
#define MEMGRAPH_UTILS_COUNTERS_SIMPLE_COUNTER_HPP

template <class T>
class SimpleCounter
{
public:
    SimpleCounter(T initial) : counter(initial) {}

    T next()
    {
        return ++counter;
    }

    T count()
    {
        return counter;
    }

private:
    T counter;
};

#endif
