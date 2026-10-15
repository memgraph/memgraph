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

  void Reset();
  auto Value() const -> uint32_t;
  void Update(const unsigned char *bytes, uint32_t len);

  static auto PatchByte(uint32_t crc, uint32_t t_delta, uint64_t bytes_after) -> uint32_t;

  /// Combine two independently-accumulated CRCs. Given crc(A) and crc(B), where B is `len_b` bytes long, returns
  /// crc(A ++ B) -- the CRC of A followed by B -- without re-reading A's bytes. Thin wrapper over zlib's crc32_combine.
  /// Used to compute a section CRC whose pieces are produced in separate passes (e.g. a fixed prefix plus back-patched
  /// bytes), where a single streaming accumulator would see the bytes in the wrong order.
  static auto Combine(uint32_t crc_a, uint32_t crc_b, uint64_t len_b) -> uint32_t;

  /// Self-checking CRC verification. `crc` is the running CRC accumulated over the whole input INCLUDING the stored CRC
  /// trailer that follows it. An intact "input ++ crc" stream reduces to a fixed CRC-32 residue, so this just compares
  /// against that residue. Returns true when the input verifies.
  static auto Verify(uint32_t crc) -> bool;

 private:
  uint32_t value_;
};

}  // namespace memgraph::utils
