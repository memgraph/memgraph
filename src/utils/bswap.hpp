#pragma once

#include <cstdint>
#include <cstdlib>

#include <byteswap.h>

namespace utils {

template <class T>
inline T Bswap(T value);

template <>
inline int16_t Bswap<int16_t>(int16_t value) {
  return __bswap_16(value);
}

template <>
inline uint16_t Bswap<uint16_t>(uint16_t value) {
  return __bswap_16(value);
}

template <>
inline int32_t Bswap<int32_t>(int32_t value) {
  return __bswap_32(value);
}

template <>
inline uint32_t Bswap<uint32_t>(uint32_t value) {
  return __bswap_32(value);
}

template <>
inline int64_t Bswap<int64_t>(int64_t value) {
  return __bswap_64(value);
}

template <>
inline uint64_t Bswap<uint64_t>(uint64_t value) {
  return __bswap_64(value);
}

}  // namespace utils
