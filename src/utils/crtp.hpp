#pragma once

// a helper class for implementing static casting to a derived class using the
// curiously recurring template pattern

template <class Derived>
struct Crtp
{
    Derived& derived()
    {
        return *static_cast<Derived*>(this);
    }

    const Derived& derived() const
    {
        return *static_cast<const Derived*>(this);
    }
};
