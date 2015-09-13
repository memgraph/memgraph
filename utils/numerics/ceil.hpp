#ifndef MEMGRAPH_UTILS_NUMERICS_COMMON_HPP
#define MEMGRAPH_UTILS_NUMERICS_COMMON_HPP

#include <type_traits>

namespace num
{

template <class T,
          typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
T iceil(T x, T y)
{
    // this may seem inefficient, but on x86_64, when you already perform
    // division (x / y) the remainder is already computed and therefore x % y
    // is basically free!
    return x / y + (x % y != 0);
}

}

#endif
