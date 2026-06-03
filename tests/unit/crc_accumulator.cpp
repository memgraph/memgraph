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

#include <cstdint>
#include <random>
#include <vector>

#include "gtest/gtest.h"
#include "zlib.h"

#include "utils/crc_accumulator.hpp"

using memgraph::utils::CrcAccumulator;

namespace {
// Full recompute of a buffer's CRC, used as the oracle for PatchByte.
uint32_t FullCrc(std::vector<uint8_t> const &buf) {
  CrcAccumulator acc;
  acc.Update(buf.data(), static_cast<uint32_t>(buf.size()));
  return acc.Value();
}
}  // namespace

TEST(CrcAccumulator, Init) {
  CrcAccumulator acc;
  ASSERT_EQ(acc.Value(), crc32(0, Z_NULL, 0));
}

TEST(CrcAccumulator, Reset) {
  CrcAccumulator acc;
  acc.Reset();
  ASSERT_EQ(acc.Value(), crc32(0, Z_NULL, 0));
}

TEST(CrcAccumulator, Update) {
  CrcAccumulator acc;
  {
    std::string str = "hello";
    auto const *data = reinterpret_cast<const unsigned char *>(str.data());
    acc.Update(data, str.size());
    ASSERT_EQ(acc.Value(), crc32(0, data, str.size()));
  }

  {
    std::string str = "world";
    auto const *data = reinterpret_cast<const unsigned char *>(str.data());
    acc.Update(data, str.size());
    std::string full_str = "helloworld";
    auto const *full_data = reinterpret_cast<const unsigned char *>(full_str.data());
    ASSERT_EQ(acc.Value(), crc32(0, full_data, full_str.size()));
  }
}

TEST(CrcAccumulator, CopyPreservesState) {
  CrcAccumulator acc;
  {
    std::string str = "hello";
    auto const *data = reinterpret_cast<const unsigned char *>(str.data());
    acc.Update(data, str.size());
    ASSERT_EQ(acc.Value(), crc32(0, data, str.size()));
  }
  CrcAccumulator copied(acc);
  ASSERT_EQ(acc.Value(), copied.Value());
}

TEST(CrcAccumulator, MovePreservesState) {
  CrcAccumulator acc;
  {
    std::string str = "hello";
    auto const *data = reinterpret_cast<const unsigned char *>(str.data());
    acc.Update(data, str.size());
    ASSERT_EQ(acc.Value(), crc32(0, data, str.size()));
  }
  CrcAccumulator copied(std::move(acc));
  ASSERT_EQ(acc.Value(), copied.Value());
}

TEST(CrcAccumulator, PatchByteNoOp) {
  std::vector<uint8_t> buf{1, 2, 3, 4, 5};
  // delta == 0 means the byte did not change -> CRC stays the same.
  ASSERT_EQ(CrcAccumulator::PatchByte(FullCrc(buf), 0, buf.size() - 1), FullCrc(buf));
}

TEST(CrcAccumulator, PatchByteFirstAndLast) {
  std::vector<uint8_t> buf(64);
  for (size_t i = 0; i < buf.size(); ++i) buf[i] = static_cast<uint8_t>(i * 7 + 1);

  // Flip the first byte: all other bytes follow it.
  {
    auto patched = buf;
    patched[0] ^= 0xff;
    ASSERT_EQ(CrcAccumulator::PatchByte(FullCrc(buf), 0xff, buf.size() - 1), FullCrc(patched));
  }
  // Flip the last byte: nothing follows it (bytes_after == 0).
  {
    auto patched = buf;
    patched.back() ^= 0xff;
    ASSERT_EQ(CrcAccumulator::PatchByte(FullCrc(buf), 0xff, 0), FullCrc(patched));
  }
}

TEST(CrcAccumulator, PatchByteMatchesFullRecompute) {
  std::mt19937 rng{0xC0FFEE};
  std::uniform_int_distribution<int> byte_dist{0, 255};

  for (size_t len : {1U, 2U, 5U, 17U, 64U, 257U, 4096U, 100000U}) {
    std::vector<uint8_t> buf(len);
    for (auto &b : buf) b = static_cast<uint8_t>(byte_dist(rng));
    auto const original_crc = FullCrc(buf);

    // Patch every position for short buffers; sample positions for long ones.
    size_t const step = len <= 257 ? 1 : len / 64;
    for (size_t pos = 0; pos < len; pos += step) {
      auto const old_byte = buf[pos];
      auto const new_byte = static_cast<uint8_t>(byte_dist(rng));
      auto const delta = static_cast<uint8_t>(old_byte ^ new_byte);
      auto const bytes_after = len - pos - 1;

      auto patched = buf;
      patched[pos] = new_byte;
      EXPECT_EQ(CrcAccumulator::PatchByte(original_crc, delta, bytes_after), FullCrc(patched))
          << "len=" << len << " pos=" << pos;
    }
  }
}
