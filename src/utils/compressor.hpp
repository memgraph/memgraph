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
#include <vector>

#define ZLIB_HEADER 0x78
#define ZLIB_LOW_COMPRESSION 0x01
#define ZLIB_FAST_COMPRESSION 0x5E
#define ZLIB_DEFAULT_COMPRESSION 0x9C
#define ZLIB_BEST_COMPRESSION 0xDA

namespace memgraph::utils {

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

  CompressedBuffer Compress(uint8_t *input, size_t input_size) override;

  CompressedBuffer Decompress(uint8_t *compressed_data, size_t compressed_size) override;

  bool IsCompressed(uint8_t *data, size_t size) const override;
};

}  // namespace memgraph::utils
