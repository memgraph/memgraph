// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#else
#include <endian.h>
#endif

#include "utils/cast.hpp"

namespace memgraph::utils {

#ifdef __APPLE__
// macOS uses libkern/OSByteOrder.h
// Note: macOS is little-endian, so host-to-little and little-to-host are no-ops
inline uint8_t HostToLittleEndian(uint8_t value) { return value; }
inline uint16_t HostToLittleEndian(uint16_t value) { return OSSwapHostToLittleInt16(value); }
inline uint32_t HostToLittleEndian(uint32_t value) { return OSSwapHostToLittleInt32(value); }
inline uint64_t HostToLittleEndian(uint64_t value) { return OSSwapHostToLittleInt64(value); }
inline int8_t HostToLittleEndian(int8_t value) { return value; }
inline int16_t HostToLittleEndian(int16_t value) { return MemcpyCast<int16_t>(OSSwapHostToLittleInt16(MemcpyCast<uint16_t>(value))); }
inline int32_t HostToLittleEndian(int32_t value) { return MemcpyCast<int32_t>(OSSwapHostToLittleInt32(MemcpyCast<uint32_t>(value))); }
inline int64_t HostToLittleEndian(int64_t value) { return MemcpyCast<int64_t>(OSSwapHostToLittleInt64(MemcpyCast<uint64_t>(value))); }

inline uint8_t LittleEndianToHost(uint8_t value) { return value; }
inline uint16_t LittleEndianToHost(uint16_t value) { return OSSwapLittleToHostInt16(value); }
inline uint32_t LittleEndianToHost(uint32_t value) { return OSSwapLittleToHostInt32(value); }
inline uint64_t LittleEndianToHost(uint64_t value) { return OSSwapLittleToHostInt64(value); }
inline int8_t LittleEndianToHost(int8_t value) { return value; }
inline int16_t LittleEndianToHost(int16_t value) { return MemcpyCast<int16_t>(OSSwapLittleToHostInt16(MemcpyCast<uint16_t>(value))); }
inline int32_t LittleEndianToHost(int32_t value) { return MemcpyCast<int32_t>(OSSwapLittleToHostInt32(MemcpyCast<uint32_t>(value))); }
inline int64_t LittleEndianToHost(int64_t value) { return MemcpyCast<int64_t>(OSSwapLittleToHostInt64(MemcpyCast<uint64_t>(value))); }

inline uint8_t HostToBigEndian(uint8_t value) { return value; }
inline uint16_t HostToBigEndian(uint16_t value) { return OSSwapHostToBigInt16(value); }
inline uint32_t HostToBigEndian(uint32_t value) { return OSSwapHostToBigInt32(value); }
inline uint64_t HostToBigEndian(uint64_t value) { return OSSwapHostToBigInt64(value); }
inline int8_t HostToBigEndian(int8_t value) { return value; }
inline int16_t HostToBigEndian(int16_t value) { return MemcpyCast<int16_t>(OSSwapHostToBigInt16(MemcpyCast<uint16_t>(value))); }
inline int32_t HostToBigEndian(int32_t value) { return MemcpyCast<int32_t>(OSSwapHostToBigInt32(MemcpyCast<uint32_t>(value))); }
inline int64_t HostToBigEndian(int64_t value) { return MemcpyCast<int64_t>(OSSwapHostToBigInt64(MemcpyCast<uint64_t>(value))); }

inline uint8_t BigEndianToHost(uint8_t value) { return value; }
inline uint16_t BigEndianToHost(uint16_t value) { return OSSwapBigToHostInt16(value); }
inline uint32_t BigEndianToHost(uint32_t value) { return OSSwapBigToHostInt32(value); }
inline uint64_t BigEndianToHost(uint64_t value) { return OSSwapBigToHostInt64(value); }
inline int8_t BigEndianToHost(int8_t value) { return value; }
inline int16_t BigEndianToHost(int16_t value) { return MemcpyCast<int16_t>(OSSwapBigToHostInt16(MemcpyCast<uint16_t>(value))); }
inline int32_t BigEndianToHost(int32_t value) { return MemcpyCast<int32_t>(OSSwapBigToHostInt32(MemcpyCast<uint32_t>(value))); }
inline int64_t BigEndianToHost(int64_t value) { return MemcpyCast<int64_t>(OSSwapBigToHostInt64(MemcpyCast<uint64_t>(value))); }
#else
// Linux uses endian.h
inline uint8_t HostToLittleEndian(uint8_t value) { return value; }
inline uint16_t HostToLittleEndian(uint16_t value) { return htole16(value); }
inline uint32_t HostToLittleEndian(uint32_t value) { return htole32(value); }
inline uint64_t HostToLittleEndian(uint64_t value) { return htole64(value); }
inline int8_t HostToLittleEndian(int8_t value) { return value; }
inline int16_t HostToLittleEndian(int16_t value) { return MemcpyCast<int16_t>(htole16(MemcpyCast<uint16_t>(value))); }
inline int32_t HostToLittleEndian(int32_t value) { return MemcpyCast<int32_t>(htole32(MemcpyCast<uint32_t>(value))); }
inline int64_t HostToLittleEndian(int64_t value) { return MemcpyCast<int64_t>(htole64(MemcpyCast<uint64_t>(value))); }

inline uint8_t LittleEndianToHost(uint8_t value) { return value; }
inline uint16_t LittleEndianToHost(uint16_t value) { return le16toh(value); }
inline uint32_t LittleEndianToHost(uint32_t value) { return le32toh(value); }
inline uint64_t LittleEndianToHost(uint64_t value) { return le64toh(value); }
inline int8_t LittleEndianToHost(int8_t value) { return value; }
inline int16_t LittleEndianToHost(int16_t value) { return MemcpyCast<int16_t>(le16toh(MemcpyCast<uint16_t>(value))); }
inline int32_t LittleEndianToHost(int32_t value) { return MemcpyCast<int32_t>(le32toh(MemcpyCast<uint32_t>(value))); }
inline int64_t LittleEndianToHost(int64_t value) { return MemcpyCast<int64_t>(le64toh(MemcpyCast<uint64_t>(value))); }

inline uint8_t HostToBigEndian(uint8_t value) { return value; }
inline uint16_t HostToBigEndian(uint16_t value) { return htobe16(value); }
inline uint32_t HostToBigEndian(uint32_t value) { return htobe32(value); }
inline uint64_t HostToBigEndian(uint64_t value) { return htobe64(value); }
inline int8_t HostToBigEndian(int8_t value) { return value; }
inline int16_t HostToBigEndian(int16_t value) { return MemcpyCast<int16_t>(htobe16(MemcpyCast<uint16_t>(value))); }
inline int32_t HostToBigEndian(int32_t value) { return MemcpyCast<int32_t>(htobe32(MemcpyCast<uint32_t>(value))); }
inline int64_t HostToBigEndian(int64_t value) { return MemcpyCast<int64_t>(htobe64(MemcpyCast<uint64_t>(value))); }

inline uint8_t BigEndianToHost(uint8_t value) { return value; }
inline uint16_t BigEndianToHost(uint16_t value) { return be16toh(value); }
inline uint32_t BigEndianToHost(uint32_t value) { return be32toh(value); }
inline uint64_t BigEndianToHost(uint64_t value) { return be64toh(value); }
inline int8_t BigEndianToHost(int8_t value) { return value; }
inline int16_t BigEndianToHost(int16_t value) { return MemcpyCast<int16_t>(be16toh(MemcpyCast<uint16_t>(value))); }
inline int32_t BigEndianToHost(int32_t value) { return MemcpyCast<int32_t>(be32toh(MemcpyCast<uint32_t>(value))); }
inline int64_t BigEndianToHost(int64_t value) { return MemcpyCast<int64_t>(be64toh(MemcpyCast<uint64_t>(value))); }
#endif

}  // namespace memgraph::utils
