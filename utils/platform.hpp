#ifndef MEMGRAPH_UTILS_PLATFORM_HPP
#define MEMGRAPH_UTILS_PLATFORM_HPP

#include <cstdint>

// is there a better way?
#if UINTPTR_MAX == 0xffffffff
#define MEMGRAPH32
#elif UINTPTR_MAX == 0xffffffffffffffff
#define MEMGRAPH64
#else
#error Unrecognized platform (neither 32 or 64)
#endif


#endif
