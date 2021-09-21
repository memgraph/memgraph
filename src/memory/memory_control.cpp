#include "memory_control.hpp"

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace memory {

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define STRINGIFY_HELPER(x) #x
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define STRINGIFY(x) STRINGIFY_HELPER(x)

void PurgeUnusedMemory() {
#if USE_JEMALLOC
  mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
#endif
}

#undef STRINGIFY
#undef STRINGIFY_HELPER
}  // namespace memory
