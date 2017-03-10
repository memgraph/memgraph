#pragma once

#include <cstdlib>
#include <string>

namespace {

#define OFFSET_BASIS32 2166136261u
#define FNV_PRIME32 16777619u

uint32_t fnv32(const unsigned char* const data, size_t n) {
  uint32_t hash = OFFSET_BASIS32;

  for (size_t i = 0; i < n; ++i)
    hash = (hash * FNV_PRIME32) xor (uint32_t) data[i];

  return hash;
}

template <class T>
uint32_t fnv32(const T& data) {
  return fnv32(&data, sizeof(data));
}

template <>
__attribute__((unused)) uint32_t fnv32(const std::string& data) {
  return fnv32((const unsigned char*)data.c_str(), data.size());
}

uint32_t fnv1a32(const unsigned char* const data, size_t n) {
  uint32_t hash = OFFSET_BASIS32;

  for (size_t i = 0; i < n; ++i)
    hash = (hash xor (uint32_t) data[i]) * FNV_PRIME32;

  return hash;
}

template <class T>
uint32_t fnv1a32(const T& data) {
  return fnv1a32(&data, sizeof(data));
}

template <>
__attribute__((unused)) uint32_t fnv1a32(const std::string& data) {
  return fnv1a32((const unsigned char*)data.c_str(), data.size());
}
}
