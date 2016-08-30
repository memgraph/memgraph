#pragma once

// Represents number of to be returned elements from iterator. Where acutal
// number is probably somwhere in [min,max].
class Count
{

public:
    Count(size_t exact) : min(exact), max(exact) {}

    Count(size_t min, size_t max) : min(min), max(max) {}

    Count &min_zero()
    {
        min = 0;
        return *this;
    }

    size_t min;
    size_t max;
};
