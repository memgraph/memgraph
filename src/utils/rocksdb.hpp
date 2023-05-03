// Copyright 2023 Memgraph Ltd.
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

#include <cstdint>
#include <string>

#include <rocksdb/db.h>

namespace memgraph::utils {

/// TODO: (andi): This can potentially be a problem on big-endian machines.
inline void PutFixed64(std::string *dst, uint64_t value) {
  dst->append(const_cast<const char *>(reinterpret_cast<char *>(&value)), sizeof(value));
}

inline uint64_t DecodeFixed64(const char *ptr) {
  // Load the raw bytes
  uint64_t result;
  memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
  return result;
}

inline std::string StringTimestamp(uint64_t ts) {
  std::string ret;
  PutFixed64(&ret, ts);
  return ret;
}

inline uint64_t ExtractTimestampFromDeserializedUserKey(const rocksdb::Slice &user_key) {
  return DecodeFixed64(user_key.data_ + user_key.size_);
}

}  // namespace memgraph::utils
