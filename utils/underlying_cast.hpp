#ifndef MEMGRAPH_UTILS_UNDERLYING_CAST_HPP
#define MEMGRAPH_UTILS_UNDERLYING_CAST_HPP

#include <type_traits>

template <typename T>
constexpr typename std::underlying_type<T>::type underlying_cast(T e) {
    return static_cast<typename std::underlying_type<T>::type>(e);
}


#endif
