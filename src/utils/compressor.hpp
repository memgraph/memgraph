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
#include <vector>

#define ZLIB_HEADER 0x78
#define ZLIB_LOW_COMPRESSION 0x01
#define ZLIB_FAST_COMPRESSION 0x5E
#define ZLIB_DEFAULT_COMPRESSION 0x9C
#define ZLIB_BEST_COMPRESSION 0xDA

namespace memgraph::utils {

struct DataBuffer {
  uint8_t *data{nullptr};
  size_t compressed_size = 0;
  size_t original_size = 0;

  // Default constructor
  DataBuffer() = default;

  // Destructor
  ~DataBuffer() { delete[] data; }

  // Copy constructor
  DataBuffer(const DataBuffer &other) : compressed_size(other.compressed_size), original_size(other.original_size) {
    if (other.data) {
      data = new uint8_t[compressed_size];
      memcpy(data, other.data, compressed_size);
    }
  }

  // Copy assignment operator
  DataBuffer &operator=(const DataBuffer &other) {
    if (this == &other) {
      return *this;
    }

    // Clean up existing data
    delete[] data;

    compressed_size = other.compressed_size;
    original_size = other.original_size;
    data = nullptr;

    if (other.data) {
      data = new uint8_t[compressed_size];
      memcpy(data, other.data, compressed_size);
    }

    return *this;
  }

  // Move constructor
  DataBuffer(DataBuffer &&other) noexcept
      : data(other.data), compressed_size(other.compressed_size), original_size(other.original_size) {
    other.data = nullptr;
    other.compressed_size = 0;
    other.original_size = 0;
  }

  // Move assignment operator
  DataBuffer &operator=(DataBuffer &&other) noexcept {
    if (this != &other) {
      // Clean up existing data
      delete[] data;

      data = other.data;
      compressed_size = other.compressed_size;
      original_size = other.original_size;

      other.data = nullptr;
      other.compressed_size = 0;
      other.original_size = 0;
    }
    return *this;
  }
};

class Compressor {
 public:
  Compressor() = default;
  virtual ~Compressor() {}

  Compressor(const Compressor &) = default;
  Compressor &operator=(const Compressor &) = default;
  Compressor(Compressor &&) = default;
  Compressor &operator=(Compressor &&) = default;

  virtual DataBuffer Compress(uint8_t *input, size_t input_size) = 0;

  virtual DataBuffer Decompress(uint8_t *compressed_data, size_t compressed_size) = 0;

  virtual bool IsCompressed(uint8_t *data, size_t size) const = 0;
};

class ZlibCompressor : public Compressor {
 protected:
  ZlibCompressor() = default;

  static ZlibCompressor *instance_;

 public:
  static ZlibCompressor *GetInstance();

  void operator=(const ZlibCompressor &) = delete;

  ZlibCompressor(const ZlibCompressor &) = delete;
  ZlibCompressor(ZlibCompressor &&) = delete;
  ZlibCompressor &operator=(ZlibCompressor &&) = delete;

  DataBuffer Compress(uint8_t *input, size_t input_size) override;

  DataBuffer Decompress(uint8_t *compressed_data, size_t compressed_size) override;

  bool IsCompressed(uint8_t *data, size_t size) const override;
};

}  // namespace memgraph::utils
