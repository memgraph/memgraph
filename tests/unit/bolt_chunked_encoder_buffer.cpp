// Copyright 2022 Memgraph Ltd.
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
#include "communication/bolt/v1/encoder/chunked_encoder_buffer.hpp"

// aliases
using BufferT = memgraph::communication::bolt::ChunkedEncoderBuffer<TestOutputStream>;

// constants
using memgraph::communication::bolt::kChunkHeaderSize;
using memgraph::communication::bolt::kChunkMaxDataSize;
using memgraph::communication::bolt::kChunkWholeSize;

// test data
inline constexpr const int kTestDataSize = 100000;
uint8_t test_data[kTestDataSize];

struct BoltChunkedEncoderBuffer : ::testing::Test {
  // In newer gtest library (1.8.1+) this is changed to SetUpTestSuite
  static void SetUpTestCase() { InitializeData(test_data, kTestDataSize); }
};

/**
 * Verifies a single chunk. The chunk should be constructed from a header
 * (chunk size) and data. The header is a two byte long number written in big
 * endian format. Data is an array of elements from test_data whose max size is
 * 0xFFFF.
 *
 * @param data pointer on data array (array of bytes)
 * @param size of data array
 * @param offset offset from the beginning of the test data
 */
void VerifyChunkOfTestData(uint8_t *data, int size, uint64_t offset = 0) {
  // first two bytes are size (big endian)
  uint8_t lower_byte = size & 0xFF;
  uint8_t higher_byte = (size & 0xFF00) >> 8;
  ASSERT_EQ(*data, higher_byte);
  ASSERT_EQ(*(data + 1), lower_byte);

  // in the data array should be size number of ones
  // the header is skipped
  for (auto i = 0; i < size; ++i) {
    ASSERT_EQ(data[i + kChunkHeaderSize], test_data[i + offset]);
  }
}

TEST_F(BoltChunkedEncoderBuffer, OneSmallChunk) {
  int size = 100;

  // initialize tested buffer
  TestOutputStream output_stream;
  BufferT buffer(output_stream);

  // write into buffer
  buffer.Write(test_data, size);
  buffer.Flush();

  // check the output array
  // the array should look like: [0, 100, first 100 bytes of test data]
  VerifyChunkOfTestData(output_stream.output.data(), size);
}

TEST_F(BoltChunkedEncoderBuffer, TwoSmallChunks) {
  int size1 = 100;
  int size2 = 200;

  // initialize tested buffer
  TestOutputStream output_stream;
  BufferT buffer(output_stream);

  // write into buffer
  buffer.Write(test_data, size1);
  buffer.Flush();
  buffer.Write(test_data + size1, size2);
  buffer.Flush();

  // check the output array
  // the output array should look like this:
  // [0, 100, first 100 bytes of test data] +
  // [0, 100, second 100 bytes of test data]
  auto data = output_stream.output.data();
  VerifyChunkOfTestData(data, size1);
  VerifyChunkOfTestData(data + kChunkHeaderSize + size1, size2, size1);
}

TEST_F(BoltChunkedEncoderBuffer, OneAndAHalfOfMaxChunk) {
  // initialize tested buffer
  TestOutputStream output_stream;
  BufferT buffer(output_stream);

  // write into buffer
  buffer.Write(test_data, kTestDataSize);
  buffer.Flush();

  // check the output array
  // the output array should look like this:
  // [0xFF, 0xFF, first 65535 bytes of test data,
  //  0x86, 0xA1, 34465 bytes of test data after the first 65535 bytes]
  auto output = output_stream.output.data();
  VerifyChunkOfTestData(output, kChunkMaxDataSize);
  VerifyChunkOfTestData(output + kChunkWholeSize, kTestDataSize - kChunkMaxDataSize, kChunkMaxDataSize);
}
