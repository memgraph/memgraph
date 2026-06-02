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

}  // namespace memgraph::utils
