#pragma once

#include "utils/numerics/saturate.hpp"
#include "utils/total_ordering.hpp"

// Represents number of to be returned elements from iterator. Where acutal
// number is probably somwhere in [min,max].
class Count : public TotalOrdering<Count>
{

public:
    Count(size_t exact) : min(exact), max(exact) {}

    Count(size_t min, size_t max) : min(min), max(max) {}

    Count min_zero() const { return Count(0, max); }

    size_t avg() const { return ((max - min) >> 1) + min; }

    friend constexpr bool operator<(const Count &lhs, const Count &rhs)
    {
        return lhs.avg() < rhs.avg();
    }

    friend constexpr bool operator==(const Count &lhs, const Count &rhs)
    {
        return lhs.avg() == rhs.avg();
    }

    friend Count operator+(const Count &lhs, const Count &rhs)
    {
        return Count(num::saturating_add(lhs.min, rhs.min),
                     num::saturating_add(lhs.max, rhs.max));
    }

    size_t min;
    size_t max;
};
