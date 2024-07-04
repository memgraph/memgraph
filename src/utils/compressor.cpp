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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include "spdlog/spdlog.h"

#include "utils/compressor.hpp"

#define ZLIB_HEADER 0x78
#define ZLIB_LOW_COMPRESSION 0x01
#define ZLIB_FAST_COMPRESSION 0x5E
#define ZLIB_DEFAULT_COMPRESSION 0x9C
#define ZLIB_BEST_COMPRESSION 0xDA

namespace memgraph::utils {

DataBuffer ZlibCompressor::Compress(uint8_t *input, uint32_t original_size) {
  if (original_size == 0) {
    return {};
  }

  DataBuffer compressed_buffer;
  auto compress_bound = compressBound(original_size);
  auto *compressed_data = new uint8_t[compress_bound];

  int result = compress(compressed_data, &compress_bound, input, original_size);

  if (result != Z_OK) {
    // Handle compression error
    delete[] compressed_data;
    return {};
  }

  compressed_buffer.original_size = original_size;
  compressed_buffer.compressed_size = compress_bound;

  compressed_buffer.data = new uint8_t[compressed_buffer.compressed_size];
  memcpy(compressed_buffer.data, compressed_data, compressed_buffer.compressed_size);

  delete[] compressed_data;

  return compressed_buffer;
}

DataBuffer ZlibCompressor::Decompress(uint8_t *compressed_data, uint32_t compressed_size, uint32_t original_size) {
  DataBuffer decompressed_buffer;
  decompressed_buffer.compressed_size = compressed_size;
  decompressed_buffer.original_size = original_size;

  decompressed_buffer.data = new uint8_t[decompressed_buffer.original_size];

  int result = uncompress(decompressed_buffer.data, reinterpret_cast<uLongf *>(&decompressed_buffer.original_size),
                          compressed_data, compressed_size);

  if (result != Z_OK) {
    // Handle decompression error
    delete[] decompressed_buffer.data;
    return {};
  }

  return decompressed_buffer;
}

bool ZlibCompressor::IsCompressed(uint8_t *data, uint32_t size) const {
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
