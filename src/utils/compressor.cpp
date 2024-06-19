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

#include "utils/compressor.hpp"
#include <sys/types.h>
#include <zlib.h>
#include <cstdint>
#include <string_view>
#include <vector>
#include "spdlog/spdlog.h"

#define ZLIB_HEADER 0x78
#define ZLIB_LOW_COMPRESSION 0x01
#define ZLIB_FAST_COMPRESSION 0x5E
#define ZLIB_DEFAULT_COMPRESSION 0x9C
#define ZLIB_BEST_COMPRESSION 0xDA

namespace memgraph::utils {

CompressedBuffer ZlibCompressor::Compress(uint8_t *input, size_t input_size) {
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

CompressedBuffer ZlibCompressor::Decompress(uint8_t *compressed_data, size_t compressed_size) {
  CompressedBuffer decompressed;

  uLongf decompressed_size = compressed_size * 2;
  decompressed.data.resize(decompressed_size);
  spdlog::info("Compressed data {}", std::string_view(reinterpret_cast<char *>(compressed_data), compressed_size));
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

bool ZlibCompressor::IsCompressed(uint8_t *data, size_t size) const {
  bool first_byte = data[0] == ZLIB_HEADER;
  bool second_byte = data[1] == ZLIB_LOW_COMPRESSION || data[1] == ZLIB_FAST_COMPRESSION ||
                     data[1] == ZLIB_DEFAULT_COMPRESSION || data[1] == ZLIB_BEST_COMPRESSION;
  bool size_check = size > 2;
  return size_check && first_byte && second_byte;
}

ZlibCompressor *ZlibCompressor::instance_ = nullptr;

ZlibCompressor *ZlibCompressor::GetInstance() {
  if (instance_ == nullptr) {
    instance_ = new ZlibCompressor();
  }
  return instance_;
}

}  // namespace memgraph::utils
