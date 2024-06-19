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

#pragma once

#include <sys/types.h>
#include <zlib.h>
#include <cstdint>
#include <string_view>
#include <vector>
#include "spdlog/spdlog.h"

struct CompressedBuffer {
  std::vector<uint8_t> data;
  size_t original_size;
};

class Compressor {
 public:
  Compressor() = default;
  virtual ~Compressor() {}

  Compressor(const Compressor &) = default;
  Compressor &operator=(const Compressor &) = default;
  Compressor(Compressor &&) = default;
  Compressor &operator=(Compressor &&) = default;

  virtual CompressedBuffer Compress(uint8_t *input, size_t input_size) = 0;

  virtual CompressedBuffer Decompress(uint8_t *compressed_data, size_t compressed_size) = 0;
};

class ZlibCompressor : public Compressor {
 public:
  ZlibCompressor() = default;
  CompressedBuffer Compress(uint8_t *input, size_t input_size) override {
    CompressedBuffer compressed;
    compressed.data.resize(compressBound(input_size));

    uLongf compressed_size = compressed.data.size();
    int result = compress(compressed.data.data(), &compressed_size, input, input_size);

    if (result != Z_OK) {
      // Handle compression error
      return {};
    }

    compressed.data.resize(compressed_size);
    compressed.original_size = input_size;

    return compressed;
  }

  CompressedBuffer Decompress(uint8_t *compressed_data, size_t compressed_size) override {
    CompressedBuffer decompressed;

    uLongf decompressed_size = compressed_size * 2;
    decompressed.data.resize(decompressed_size);
    // spdlog::info("Compressed data {}", std::string_view(reinterpret_cast<char *>(compressed_data), compressed_size));
    int result = uncompress(decompressed.data.data(), &decompressed_size, compressed_data, compressed_size);

    while (result == Z_BUF_ERROR) {
      // The decompressed buffer is not large enough, increase its size and retry
      decompressed_size *= 2;  // Double the buffer size
      decompressed.data.resize(decompressed_size);
      result = uncompress(decompressed.data.data(), &decompressed_size, compressed_data, compressed_size);
    }

    if (result != Z_OK) {
      // Handle decompression error
      return {};
    }

    decompressed.data.resize(decompressed_size);
    decompressed.original_size = decompressed_size;

    return decompressed;
  }
};
