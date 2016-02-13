#pragma once

template <class Derived>
struct MathOperations
{
    friend Derived operator+(const Derived& lhs, const Derived& rhs)
    {
        return Derived(lhs.value + rhs.value);
    }

    friend Derived operator-(const Derived& lhs, const Derived& rhs)
    {
        return Derived(lhs.value - rhs.value);
    }

    friend Derived operator*(const Derived& lhs, const Derived& rhs)
    {
        return Derived(lhs.value * rhs.value);
    }

    friend Derived operator/(const Derived& lhs, const Derived& rhs)
    {
        return Derived(lhs.value / rhs.value);
    }
};
