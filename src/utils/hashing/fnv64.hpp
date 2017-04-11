#pragma once

#include <cstdlib>
#include <string>

namespace {

#define OFFSET_BASIS64 14695981039346656037u
#define FNV_PRIME64 1099511628211u

__attribute__((unused)) uint64_t fnv64(const std::string &s) {
  uint64_t hash = OFFSET_BASIS64;

  for (size_t i = 0; i < s.size(); ++i)
    hash = (hash * FNV_PRIME64) xor (uint64_t) s[i];

  return hash;
}

__attribute__((unused)) uint64_t fnv1a64(const std::string &s) {
  uint64_t hash = OFFSET_BASIS64;

  for (size_t i = 0; i < s.size(); ++i)
    hash = (hash xor (uint64_t) s[i]) * FNV_PRIME64;

  return hash;
}
}
