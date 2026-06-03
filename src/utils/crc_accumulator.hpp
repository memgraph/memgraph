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

#pragma once

#include <cstdint>

namespace memgraph::utils {

class CrcAccumulator {
 public:
  CrcAccumulator();

  ~CrcAccumulator() = default;
  CrcAccumulator(CrcAccumulator const &) = default;
  CrcAccumulator &operator=(CrcAccumulator const &) = default;
  CrcAccumulator(CrcAccumulator &&) = default;
  CrcAccumulator &operator=(CrcAccumulator &&) = default;

  void Reset();
  auto Value() const -> uint32_t;
  void Update(const unsigned char *bytes, uint32_t len);

  static auto PatchByte(uint32_t crc, uint32_t t_delta, uint64_t bytes_after) -> uint32_t;

  /// Self-checking CRC verification. `crc` is the running CRC accumulated over the whole input INCLUDING the stored CRC
  /// trailer that follows it. An intact "input ++ crc" stream reduces to a fixed CRC-32 residue, so this just compares
  /// against that residue. Returns true when the input verifies.
  static auto Verify(uint32_t crc) -> bool;

 private:
  uint32_t value_;
};

}  // namespace memgraph::utils
