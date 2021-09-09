#include "purge.hpp"

namespace utils {
void Purge() {
  char tmp[32];
  unsigned narenas = 0;
  size_t sz = sizeof(unsigned);
  if (!mallctl("arenas.narenas", &narenas, &sz, NULL, 0)) {
    sprintf(tmp, "arena.%d.purge", narenas);
    if (!mallctl(tmp, NULL, 0, NULL, 0)) return;
  }
}
}  // namespace utils
