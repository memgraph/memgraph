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

#include "gtest/gtest.h"
#include "zlib.h"

#include "utils/crc_accumulator.hpp"

using memgraph::utils::CrcAccumulator;

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
