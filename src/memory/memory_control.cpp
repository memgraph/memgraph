#include "memory_control.hpp"

namespace memory {
void PurgeUnusedMemory() {
  char tmp[32];
  unsigned narenas = 0;
  size_t sz = sizeof(unsigned);
  if (!mallctl("arenas.narenas", &narenas, &sz, nullptr, 0)) {
    sprintf(tmp, "arena.%d.purge", narenas);
    if (!mallctl(tmp, nullptr, nullptr, nullptr, 0)) return;
  }
}
}  // namespace memory
