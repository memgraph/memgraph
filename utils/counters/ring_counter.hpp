#pragma once

#include <cstdlib>

class RingCounter
{
public:
    RingCounter(size_t n, size_t initial = 0)
        : n(n), counter(initial) {}

    size_t operator++()
    {
        counter = (counter + 1) % n;
        return counter;
    }

    size_t operator++(int)
    {
        auto value = counter;
        ++counter;
        return value;
    }

    operator size_t() const { return counter; }

private:
    size_t n, counter;
};
