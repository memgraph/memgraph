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
#include <cstring>

#include "utils/compressor.hpp"

#define ZLIB_HEADER 0x78
#define ZLIB_LOW_COMPRESSION 0x01
#define ZLIB_FAST_COMPRESSION 0x5E
#define ZLIB_DEFAULT_COMPRESSION 0x9C
#define ZLIB_BEST_COMPRESSION 0xDA

namespace memgraph::utils {

DataBuffer ZlibCompressor::Compress(uint8_t *input, size_t input_size) {
  if (input_size == 0) {
    return {};
  }

  DataBuffer compressed_buffer;
  auto compress_bound = compressBound(input_size);
  auto *compressed_data = new uint8_t[compress_bound];

  int result = compress(compressed_data, &compress_bound, input, input_size);

  if (result != Z_OK) {
    // Handle compression error
    return {};
  }

  compressed_buffer.original_size = input_size;
  compressed_buffer.compressed_size = compress_bound;

  // first 4 bytes are the original size of the data
  compressed_buffer.data = new uint8_t[compressed_buffer.compressed_size + sizeof(uint32_t)];
  memcpy(compressed_buffer.data, &compressed_buffer.original_size, sizeof(uint32_t));
  memcpy(compressed_buffer.data + sizeof(uint32_t), compressed_data, compressed_buffer.compressed_size);

  delete[] compressed_data;

  return compressed_buffer;
}

DataBuffer ZlibCompressor::Decompress(uint8_t *compressed_data, size_t compressed_size) {
  DataBuffer decompressed_buffer;
  decompressed_buffer.compressed_size = compressed_size;

  memcpy(&decompressed_buffer.original_size, compressed_data, sizeof(uint32_t));
  decompressed_buffer.data = new uint8_t[decompressed_buffer.original_size];

  int result = uncompress(decompressed_buffer.data, &decompressed_buffer.original_size,
                          compressed_data + sizeof(uint32_t), compressed_size - sizeof(uint32_t));

  if (result != Z_OK) {
    // Handle decompression error
    return {};
  }

  return decompressed_buffer;
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
