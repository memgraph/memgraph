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
#include "communication/bolt/v1/decoder/chunked_decoder_buffer.hpp"
#include "communication/buffer.hpp"

inline constexpr const int SIZE = 131072;
uint8_t data[SIZE];

using BufferT = memgraph::communication::Buffer;
using StreamBufferT = memgraph::io::network::StreamBuffer;
using DecoderBufferT = memgraph::communication::bolt::ChunkedDecoderBuffer<BufferT::ReadEnd>;
using ChunkStateT = memgraph::communication::bolt::ChunkState;

struct BoltBuffer : ::testing::Test {
  // In newer gtest library (1.8.1+) this is changed to SetUpTestSuite
  static void SetUpTestCase() { InitializeData(data, SIZE); }
};

TEST_F(BoltBuffer, CorrectChunk) {
  uint8_t tmp[2000];
  BufferT buffer;
  DecoderBufferT decoder_buffer(*buffer.read_end());
  StreamBufferT sb = buffer.write_end()->GetBuffer();

  sb.data[0] = 0x03;
  sb.data[1] = 0xe8;
  memcpy(sb.data + 2, data, 1000);
  sb.data[1002] = 0;
  sb.data[1003] = 0;
  buffer.write_end()->Written(1004);

  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Whole);
  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Done);

  ASSERT_EQ(decoder_buffer.Read(tmp, 1000), true);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);

  ASSERT_EQ(buffer.read_end()->size(), 0);
}

TEST_F(BoltBuffer, CorrectChunkTrailingData) {
  uint8_t tmp[2000];
  BufferT buffer;
  DecoderBufferT decoder_buffer(*buffer.read_end());
  StreamBufferT sb = buffer.write_end()->GetBuffer();

  sb.data[0] = 0x03;
  sb.data[1] = 0xe8;
  memcpy(sb.data + 2, data, 2002);
  sb.data[1002] = 0;
  sb.data[1003] = 0;
  buffer.write_end()->Written(2004);

  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Whole);
  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Done);

  ASSERT_EQ(decoder_buffer.Read(tmp, 1000), true);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);

  uint8_t *leftover = buffer.read_end()->data();
  ASSERT_EQ(buffer.read_end()->size(), 1000);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i + 1002], leftover[i]);
}

TEST_F(BoltBuffer, GraduallyPopulatedChunk) {
  uint8_t tmp[2000];
  BufferT buffer;
  DecoderBufferT decoder_buffer(*buffer.read_end());
  StreamBufferT sb = buffer.write_end()->GetBuffer();

  sb.data[0] = 0x03;
  sb.data[1] = 0xe8;
  buffer.write_end()->Written(2);

  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Partial);
    sb = buffer.write_end()->GetBuffer();
    memcpy(sb.data, data + 200 * i, 200);
    buffer.write_end()->Written(200);
  }

  sb = buffer.write_end()->GetBuffer();
  sb.data[0] = 0;
  sb.data[1] = 0;
  buffer.write_end()->Written(2);
  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Whole);
  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Done);

  ASSERT_EQ(decoder_buffer.Read(tmp, 1000), true);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);

  ASSERT_EQ(buffer.read_end()->size(), 0);
}

TEST_F(BoltBuffer, GraduallyPopulatedChunkTrailingData) {
  uint8_t tmp[2000];
  BufferT buffer;
  DecoderBufferT decoder_buffer(*buffer.read_end());
  StreamBufferT sb = buffer.write_end()->GetBuffer();

  sb.data[0] = 0x03;
  sb.data[1] = 0xe8;
  buffer.write_end()->Written(2);

  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Partial);
    sb = buffer.write_end()->GetBuffer();
    memcpy(sb.data, data + 200 * i, 200);
    buffer.write_end()->Written(200);
  }

  sb = buffer.write_end()->GetBuffer();
  sb.data[0] = 0;
  sb.data[1] = 0;
  buffer.write_end()->Written(2);

  sb = buffer.write_end()->GetBuffer();
  memcpy(sb.data, data, 1000);
  buffer.write_end()->Written(1000);

  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Whole);
  ASSERT_EQ(decoder_buffer.GetChunk(), ChunkStateT::Done);

  ASSERT_EQ(decoder_buffer.Read(tmp, 1000), true);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], tmp[i]);

  uint8_t *leftover = buffer.read_end()->data();
  ASSERT_EQ(buffer.read_end()->size(), 1000);
  for (int i = 0; i < 1000; ++i) EXPECT_EQ(data[i], leftover[i]);
}
