#pragma once

#include <cstdint>
#include <cstdlib>

#include <byteswap.h>

template <class T>
inline T bswap(T value);

template<>
inline int16_t bswap<int16_t>(int16_t value)
{
    return __bswap_16(value);
}

template<>
inline uint16_t bswap<uint16_t>(uint16_t value)
{
    return __bswap_16(value);
}

template<>
inline int32_t bswap<int32_t>(int32_t value)
{
    return __bswap_32(value);
}

template<>
inline uint32_t bswap<uint32_t>(uint32_t value)
{
    return __bswap_32(value);
}

template<>
inline int64_t bswap<int64_t>(int64_t value)
{
    return __bswap_64(value);
}

template<>
inline uint64_t bswap<uint64_t>(uint64_t value)
{
    return __bswap_64(value);
}
