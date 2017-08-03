#include "stdint.h"

inline int is_marked(long long i) {
  return ((i & 0x1L)>0);
}

inline long long get_unmarked(long long i) {
  return i & ~0x1L;
}

inline long long get_marked(long long i) {
  return i | 0x1L;
}

