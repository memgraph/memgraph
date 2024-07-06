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

#include <sys/types.h>
#include <zlib.h>
#include <cstdint>
#include <cstring>
#include <utility>

#include "utils/compressor.hpp"

namespace memgraph::utils {

constexpr uint8_t kZlibHeader = 0x78;
constexpr uint8_t kZlibLowCompression = 0x01;
constexpr uint8_t kZlibFastCompression = 0x5E;
constexpr uint8_t kZlibDefaultCompression = 0x9C;
constexpr uint8_t kZlibBestCompression = 0xDA;

DataBuffer::DataBuffer(uint8_t *data, uint32_t compressed_size, uint32_t original_size)
    : data(data), compressed_size(compressed_size), original_size(original_size) {}

DataBuffer::~DataBuffer() { delete[] data; }

DataBuffer::DataBuffer(DataBuffer &&other) noexcept
    : data(std::exchange(other.data, nullptr)),
      compressed_size(std::exchange(other.compressed_size, 0)),
      original_size(std::exchange(other.original_size, 0)) {}

DataBuffer &DataBuffer::operator=(DataBuffer &&other) noexcept {
  if (this != &other) {
    delete[] data;
    data = std::exchange(other.data, nullptr);
    compressed_size = std::exchange(other.compressed_size, 0);
    original_size = std::exchange(other.original_size, 0);
  }
  return *this;
}

DataBuffer ZlibCompressor::Compress(const uint8_t *input, uint32_t original_size) {
  if (original_size == 0) {
    return {};
  }

  DataBuffer compressed_buffer;
  auto compress_bound = compressBound(original_size);
  auto *compressed_data = new uint8_t[compress_bound];

  const int result = compress(compressed_data, &compress_bound, input, original_size);

  if (result == Z_OK) {
    compressed_buffer.original_size = original_size;
    compressed_buffer.compressed_size = compress_bound;

    compressed_buffer.data = new uint8_t[compressed_buffer.compressed_size];
    memcpy(compressed_buffer.data, compressed_data, compressed_buffer.compressed_size);

    delete[] compressed_data;

    return compressed_buffer;
  }

  delete[] compressed_data;
  return {};
}

DataBuffer ZlibCompressor::Decompress(const uint8_t *compressed_data, uint32_t compressed_size,
                                      uint32_t original_size) {
  if (compressed_size == 0 || original_size == 0) {
    return {};
  }

  auto *data = new uint8_t[original_size];

  uLongf original_size_ = original_size;
  const int result = uncompress(data, &original_size_, compressed_data, compressed_size);

  if (result == Z_OK) return {data, compressed_size, original_size};

  delete[] data;
  return {};
}

bool ZlibCompressor::IsCompressed(const uint8_t *data, uint32_t size) const {
  const bool first_byte = data[0] == kZlibHeader;
  const bool second_byte = data[1] == kZlibLowCompression || data[1] == kZlibFastCompression ||
                           data[1] == kZlibDefaultCompression || data[1] == kZlibBestCompression;
  const bool size_check = size > 2;
  return size_check && first_byte && second_byte;
}

ZlibCompressor *ZlibCompressor::GetInstance() {
  if (instance_ == nullptr) {
    instance_ = new ZlibCompressor();
  }
  return instance_;
}

}  // namespace memgraph::utils
