#pragma once

#include <cstdlib>
#include <string>

namespace {

#define OFFSET_BASIS32 2166136261u
#define FNV_PRIME32 16777619u

__attribute__((unused)) uint32_t fnv32(const std::string &s) {
  uint32_t hash = OFFSET_BASIS32;

  for (size_t i = 0; i < s.size(); ++i)
    hash = (hash * FNV_PRIME32) xor (uint32_t) s[i];

  return hash;
}

__attribute__((unused)) uint32_t fnv1a32(const std::string &s) {
  uint32_t hash = OFFSET_BASIS32;

  for (size_t i = 0; i < s.size(); ++i)
    hash = (hash xor (uint32_t) s[i]) * FNV_PRIME32;

  return hash;
}
}
