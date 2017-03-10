#pragma once

#include <cstdlib>
#include <string>

namespace {

#define OFFSET_BASIS64 14695981039346656037u
#define FNV_PRIME64 1099511628211u

uint64_t fnv64(const unsigned char* const data, size_t n) {
  uint64_t hash = OFFSET_BASIS64;

  for (size_t i = 0; i < n; ++i)
    hash = (hash * FNV_PRIME64) xor (uint64_t) data[i];

  return hash;
}

template <class T>
uint64_t fnv64(const T& data) {
  return fnv64(&data, sizeof(data));
}

template <>
__attribute__((unused)) uint64_t fnv64(const std::string& data) {
  return fnv64((const unsigned char*)data.c_str(), data.size());
}

uint64_t fnv1a64(const unsigned char* const data, size_t n) {
  uint64_t hash = OFFSET_BASIS64;

  for (size_t i = 0; i < n; ++i)
    hash = (hash xor (uint64_t) data[i]) * FNV_PRIME64;

  return hash;
}

template <class T>
uint64_t fnv1a64(const T& data) {
  return fnv1a64(&data, sizeof(data));
}

template <>
__attribute__((unused)) uint64_t fnv1a64(const std::string& data) {
  return fnv1a64((const unsigned char*)data.c_str(), data.size());
}
}
