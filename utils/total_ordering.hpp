#pragma once

template <class Derived>
struct TotalOrdering
{
    friend constexpr bool operator!=(const Derived& a, const Derived& b)
    {
        return !(a == b);
    }

    friend constexpr bool operator<=(const Derived& a, const Derived& b)
    {
        return a < b || a == b;
    }

    friend constexpr bool operator>(const Derived& a, const Derived& b)
    {
        return !(a <= b);
    }

    friend constexpr bool operator>=(const Derived& a, const Derived& b)
    {
        return !(a < b);
    }
};
