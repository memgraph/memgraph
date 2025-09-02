// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "bolt_common.hpp"
#include "communication/buffer.hpp"

inline constexpr const int SIZE = 4096;
uint8_t data[SIZE];

using memgraph::communication::Buffer;

struct CommunicationBuffer : ::testing::Test {
  // In newer gtest library (1.8.1+) this is changed to SetUpTestSuite
  static void SetUpTestCase() { InitializeData(data, SIZE); }
};

TEST_F(CommunicationBuffer, AllocateAndWritten) {
  Buffer buffer;
  auto sb = buffer.write_end()->GetBuffer();

  memcpy(sb.data, data, 1000);
  buffer.write_end()->Written(1000);

  ASSERT_EQ(buffer.read_end()->size(), 1000);

  uint8_t *tmp = buffer.read_end()->data();
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);
}

TEST_F(CommunicationBuffer, Shift) {
  Buffer buffer;
  auto sb = buffer.write_end()->GetBuffer();

  memcpy(sb.data, data, 1000);
  buffer.write_end()->Written(1000);

  sb = buffer.write_end()->GetBuffer();
  memcpy(sb.data, data + 1000, 1000);
  buffer.write_end()->Written(1000);

  ASSERT_EQ(buffer.read_end()->size(), 2000);

  uint8_t *tmp = buffer.read_end()->data();
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);

  buffer.read_end()->Shift(1000);
  ASSERT_EQ(buffer.read_end()->size(), 1000);
  tmp = buffer.read_end()->data();

  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i + 1000], tmp[i]);
}

TEST_F(CommunicationBuffer, Resize) {
  Buffer buffer;
  auto sb = buffer.write_end()->GetBuffer();

  buffer.read_end()->Resize(sb.len + 1000);

  auto sbn = buffer.write_end()->GetBuffer();
  ASSERT_EQ(sb.len + 1000, sbn.len);
}
