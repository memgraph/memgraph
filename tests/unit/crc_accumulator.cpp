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

// Raw register contribution of a single delta byte (== CRC table entry T[delta]); the input PatchByte expects.
uint32_t TDelta(uint8_t delta) {
  unsigned char const d = delta;
  unsigned char const zero = 0;
  return static_cast<uint32_t>(crc32(0, &d, 1) ^ crc32(0, &zero, 1));
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
  ASSERT_EQ(CrcAccumulator::PatchByte(FullCrc(buf), TDelta(0), buf.size() - 1), FullCrc(buf));
}

TEST(CrcAccumulator, PatchByteFirstAndLast) {
  std::vector<uint8_t> buf(64);
  for (size_t i = 0; i < buf.size(); ++i) buf[i] = static_cast<uint8_t>(i * 7 + 1);

  // Flip the first byte: all other bytes follow it.
  {
    auto patched = buf;
    patched[0] ^= 0xff;
    ASSERT_EQ(CrcAccumulator::PatchByte(FullCrc(buf), TDelta(0xff), buf.size() - 1), FullCrc(patched));
  }
  // Flip the last byte: nothing follows it (bytes_after == 0).
  {
    auto patched = buf;
    patched.back() ^= 0xff;
    ASSERT_EQ(CrcAccumulator::PatchByte(FullCrc(buf), TDelta(0xff), 0), FullCrc(patched));
  }
}

namespace {
// Append the CRC trailer the way Encoder::WriteCrc does: the 32-bit CRC stored as 8 little-endian bytes (CRC in the
// low 4, zero padded), then return the CRC accumulated over data ++ trailer.
uint32_t AccumulateWithTrailer(std::vector<uint8_t> data, uint32_t crc) {
  for (int i = 0; i < 8; ++i) data.push_back(static_cast<uint8_t>((static_cast<uint64_t>(crc) >> (8 * i)) & 0xFFU));
  return FullCrc(data);
}
}  // namespace

TEST(CrcAccumulator, VerifyAcceptsIntactStream) {
  std::mt19937 rng{0xBADC0DE};
  std::uniform_int_distribution<int> byte_dist{0, 255};

  for (size_t len : {0U, 1U, 7U, 64U, 4096U}) {
    std::vector<uint8_t> buf(len);
    for (auto &b : buf) b = static_cast<uint8_t>(byte_dist(rng));
    auto const crc = FullCrc(buf);

    // Genuine "data ++ crc" reduces to the residue and verifies; a corrupted trailer or body does not.
    EXPECT_TRUE(CrcAccumulator::Verify(AccumulateWithTrailer(buf, crc)));
    EXPECT_FALSE(CrcAccumulator::Verify(AccumulateWithTrailer(buf, crc ^ 0x1U)));
    auto corrupted = buf;
    corrupted.push_back(0);  // change the body but keep the stale CRC
    EXPECT_FALSE(CrcAccumulator::Verify(AccumulateWithTrailer(corrupted, crc)));
  }
}

TEST(CrcAccumulator, VerifyResidueConstant) {
  // The accumulated CRC over an intact "data ++ uint64-LE(crc)" stream is the fixed 8-byte-append residue.
  std::vector<uint8_t> buf{9, 8, 7, 6, 5, 4, 3, 2, 1};
  ASSERT_EQ(AccumulateWithTrailer(buf, FullCrc(buf)), 0x6522DF69U);
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
      EXPECT_EQ(CrcAccumulator::PatchByte(original_crc, TDelta(delta), bytes_after), FullCrc(patched))
          << "len=" << len << " pos=" << pos;
    }
  }
}
