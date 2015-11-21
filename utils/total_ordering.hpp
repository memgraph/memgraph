#pragma once

template <class Derived>
struct TotalOrdering
{
    friend bool operator!=(const Derived& a, const Derived& b)
    {
        return !(a == b);
    }

    friend bool operator<=(const Derived& a, const Derived& b)
    {
        return a < b || a == b;
    }

    friend bool operator>(const Derived& a, const Derived& b)
    {
        return !(a <= b);
    }

    friend bool operator>=(const Derived& a, const Derived& b)
    {
        return !(a < b);
    }
};
