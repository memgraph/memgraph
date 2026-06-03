// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/crc_accumulator.hpp"

#include "zlib.h"

namespace memgraph::utils {

CrcAccumulator::CrcAccumulator() : value_(crc32(0, Z_NULL, 0)) {}

void CrcAccumulator::Reset() { value_ = crc32(0, Z_NULL, 0); }

auto CrcAccumulator::Value() const -> uint32_t { return value_; }

void CrcAccumulator::Update(const unsigned char *bytes, uint32_t const len) { value_ = crc32(value_, bytes, len); }

auto CrcAccumulator::PatchByte(uint32_t const crc, uint32_t const t_delta, uint64_t const bytes_after) -> uint32_t {
  if (t_delta == 0) return crc;  // byte unchanged -> CRC unchanged

  // Propagate that contribution across the `bytes_after` unchanged bytes that follow. Appending N zero bytes is exactly
  // zlib's crc32_combine(crc, 0, N) operator, which advances the register in O(log N).
  return static_cast<uint32_t>(crc ^ crc32_combine(t_delta, 0, static_cast<z_off_t>(bytes_after)));
}

}  // namespace memgraph::utils
