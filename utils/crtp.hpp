#ifndef MEMGRAPH_UTILS_CRTP_HPP
#define MEMGRAPH_UTILS_CRTP_HPP

// a helper class for implementing static casting to a derived class using the
// curiously recurring template pattern

template <class Derived>
struct Crtp
{
    Derived& derived()
    {
        return *static_cast<Derived*>(this);
    }
};

#endif
