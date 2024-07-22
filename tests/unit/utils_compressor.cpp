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

#include <cstdint>
#include <cstring>
#include <string>

#include "gtest/gtest.h"
#include "utils/compressor.hpp"

TEST(ZlibCompressorTest, CompressDecompressTest) {
  auto const *compressor = memgraph::utils::Compressor::GetInstance();

  const char *input_str = "Hello, Zlib Compression!";
  size_t input_size = std::strlen(input_str);

  auto compressed = compressor->Compress(std::span(reinterpret_cast<const uint8_t *>(input_str), input_size));
  EXPECT_TRUE(compressed);
  EXPECT_GT(compressed->view().size_bytes(), 0);
  EXPECT_EQ(compressed->original_size(), input_size);

  auto decompressed = compressor->Decompress(compressed->view(), input_size);
  EXPECT_TRUE(decompressed);
  EXPECT_EQ(decompressed->view().size_bytes(), input_size);
  EXPECT_EQ(std::string_view(reinterpret_cast<char *>(decompressed->view().data()), decompressed->view().size_bytes()),
            std::string_view(input_str));
}

TEST(ZlibCompressorTest, CompressEmptyDataTest) {
  auto const *compressor = memgraph::utils::Compressor::GetInstance();

  const char *input_str = "";

  auto compressed =
      compressor->Compress(std::span(reinterpret_cast<const uint8_t *>(input_str), std::strlen(input_str)));
  EXPECT_TRUE(compressed);
  EXPECT_EQ(compressed->view().size_bytes(), 0);
  EXPECT_EQ(compressed->original_size(), 0);

  auto decompressed = compressor->Decompress(compressed->view(), 0);
  EXPECT_TRUE(decompressed);
  EXPECT_EQ(decompressed->view().size_bytes(), 0);
}

TEST(ZlibCompressorTest, LargeDataTest) {
  auto const *compressor = memgraph::utils::Compressor::GetInstance();

  const size_t LARGE_DATA_SIZE = 100000;
  std::string input_data(LARGE_DATA_SIZE, 'A');

  auto compressed =
      compressor->Compress(std::span{reinterpret_cast<const uint8_t *>(input_data.data()), LARGE_DATA_SIZE});
  EXPECT_GT(compressed->view().size_bytes(), 0);
  EXPECT_EQ(compressed->original_size(), LARGE_DATA_SIZE);

  auto decompressed = compressor->Decompress(compressed->view(), LARGE_DATA_SIZE);
  EXPECT_EQ(std::string_view(reinterpret_cast<char *>(decompressed->view().data()), decompressed->view().size_bytes()),
            input_data);
}
