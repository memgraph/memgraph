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
#include <memory>
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

DataBuffer::DataBuffer(std::unique_ptr<uint8_t[]> data, uint32_t compressed_size, uint32_t original_size)
    : data(std::move(data)), compressed_size(compressed_size), original_size(original_size) {}

DataBuffer::DataBuffer(DataBuffer &&other) noexcept
    : data(std::move(other.data)),
      compressed_size(std::exchange(other.compressed_size, 0)),
      original_size(std::exchange(other.original_size, 0)) {}

DataBuffer &DataBuffer::operator=(DataBuffer &&other) noexcept {
  if (this != &other) {
    data = std::move(other.data);
    compressed_size = std::exchange(other.compressed_size, 0);
    original_size = std::exchange(other.original_size, 0);
  }
  return *this;
}

DataBuffer ZlibCompressor::Compress(const uint8_t *input, uint32_t original_size) {
  if (original_size == 0 || input == nullptr) {
    return {};
  }

  DataBuffer compressed_buffer;
  auto compress_bound = compressBound(original_size);
  std::unique_ptr<uint8_t[]> compressed_data(new uint8_t[compress_bound]);
  auto compressed_bound_copy = compress_bound;

  const int result = compress(compressed_data.get(), &compress_bound, input, original_size);

  if (result == Z_OK) {
    std::unique_ptr<uint8_t[]> compressed_buffer_data;
    uint32_t compressed_size = compress_bound;
    if (compress_bound < compressed_bound_copy) {
      compressed_buffer_data = std::make_unique<uint8_t[]>(compress_bound);
      std::memcpy(compressed_buffer_data.get(), compressed_data.get(), compress_bound);
      return {std::move(compressed_buffer_data), compressed_size, original_size};
    }
    return {std::move(compressed_data), compressed_size, original_size};
  }
  return {};
}

DataBuffer ZlibCompressor::Decompress(const uint8_t *compressed_data, uint32_t compressed_size,
                                      uint32_t original_size) {
  if (compressed_size == 0 || original_size == 0 || compressed_data == nullptr) {
    return {};
  }

  std::unique_ptr<uint8_t[]> uncompressed_data(new uint8_t[original_size]);

  uLongf original_size_ = original_size;
  const int result = uncompress(uncompressed_data.get(), &original_size_, compressed_data, compressed_size);

  if (result == Z_OK) return {std::move(uncompressed_data), original_size, original_size};

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
