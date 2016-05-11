#pragma once

template <class Derived>
struct Modulo
{
    friend Derived operator%(const Derived& lhs, const Derived& rhs)
    {
        return Derived(lhs.value % rhs.value);
    }
};
