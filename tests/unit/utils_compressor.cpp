// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstring>

#include "gtest/gtest.h"
#include "utils/compressor.hpp"

TEST(ZlibCompressorTest, CompressDecompressTest) {
  memgraph::utils::ZlibCompressor *compressor = memgraph::utils::ZlibCompressor::GetInstance();

  const char *input_str = "Hello, Zlib Compression!";
  size_t input_size = std::strlen(input_str);
  std::vector<uint8_t> input_data(input_str, input_str + input_size);

  memgraph::utils::DataBuffer compressed = compressor->Compress(input_data.data(), input_size);
  EXPECT_GT(compressed.compressed_size, 0);
  EXPECT_EQ(compressed.original_size, input_size);

  memgraph::utils::DataBuffer decompressed = compressor->Decompress(compressed.data, compressed.compressed_size);
  EXPECT_EQ(decompressed.original_size, input_size);
  EXPECT_EQ(std::string_view(reinterpret_cast<char *>(decompressed.data), decompressed.original_size),
            std::string_view(input_str));
}

TEST(ZlibCompressorTest, CompressEmptyDataTest) {
  memgraph::utils::ZlibCompressor *compressor = memgraph::utils::ZlibCompressor::GetInstance();

  std::vector<uint8_t> input_data = {};

  memgraph::utils::DataBuffer compressed = compressor->Compress(input_data.data(), input_data.size());
  EXPECT_EQ(compressed.compressed_size, 0);
  EXPECT_EQ(compressed.original_size, 0);

  memgraph::utils::DataBuffer decompressed = compressor->Decompress(compressed.data, compressed.compressed_size);
  EXPECT_EQ(decompressed.compressed_size, 0);
  EXPECT_EQ(decompressed.original_size, 0);
}

TEST(ZlibCompressorTest, IsCompressedTest) {
  memgraph::utils::ZlibCompressor *compressor = memgraph::utils::ZlibCompressor::GetInstance();

  const char *input_str = "Hello, Zlib Compression!";
  size_t input_size = std::strlen(input_str);
  std::vector<uint8_t> input_data(input_str, input_str + input_size);

  memgraph::utils::DataBuffer compressed = compressor->Compress(input_data.data(), input_size);
  EXPECT_TRUE(compressor->IsCompressed(compressed.data, compressed.compressed_size));

  // Test with uncompressed data
  EXPECT_FALSE(compressor->IsCompressed(input_data.data(), input_data.size()));
}

TEST(ZlibCompressorTest, HandleDecompressionErrorTest) {
  memgraph::utils::ZlibCompressor *compressor = memgraph::utils::ZlibCompressor::GetInstance();

  // Create invalid compressed data
  std::vector<uint8_t> invalid_compressed_data = {0x00, 0x01, 0x02, 0x03};

  memgraph::utils::DataBuffer decompressed =
      compressor->Decompress(invalid_compressed_data.data(), invalid_compressed_data.size());
  EXPECT_EQ(decompressed.compressed_size, 0);
  EXPECT_EQ(decompressed.original_size, 0);
}

TEST(ZlibCompressorTest, LargeDataTest) {
  memgraph::utils::ZlibCompressor *compressor = memgraph::utils::ZlibCompressor::GetInstance();

  const size_t LARGE_DATA_SIZE = 100000;
  std::vector<uint8_t> input_data(LARGE_DATA_SIZE, 'A');

  memgraph::utils::DataBuffer compressed = compressor->Compress(input_data.data(), input_data.size());
  EXPECT_GT(compressed.compressed_size, 0);
  EXPECT_EQ(compressed.original_size, LARGE_DATA_SIZE);

  memgraph::utils::DataBuffer decompressed = compressor->Decompress(compressed.data, compressed.compressed_size);
  EXPECT_EQ(decompressed.original_size, LARGE_DATA_SIZE);
  EXPECT_EQ(std::string_view(reinterpret_cast<char *>(decompressed.data), decompressed.original_size),
            std::string_view(reinterpret_cast<char *>(input_data.data()), input_data.size()));
}
