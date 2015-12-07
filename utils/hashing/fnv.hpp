#pragma once

#include <cstdlib>
#include <string>

#include "utils/platform.hpp"

#include "fnv32.hpp"
#include "fnv64.hpp"

// fnv1a is recommended so use it as a default implementation. also use the
// platform specific version of the function

#ifdef MEMGRAPH64

template <class T>
uint64_t fnv(const T& data)
{
    return fnv1a64<T>(data);
}

#elif

template <class T>
uint32_t fnv(const T& data)
{
    return fnv1a32<T>(data);
}

#endif
 
