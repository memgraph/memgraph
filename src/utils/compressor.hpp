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

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <sys/types.h>
#include <zlib.h>
#include <array>
#include <memory>
#include <string_view>
#include "utils/enum.hpp"

namespace memgraph::flags {

using namespace std::string_view_literals;

inline constexpr std::array compression_level_mappings{std::pair{"low"sv, Z_BEST_SPEED},
                                                       std::pair{"mid"sv, Z_DEFAULT_COMPRESSION},
                                                       std::pair{"high"sv, Z_BEST_COMPRESSION}};

inline const std::string storage_property_store_compression_level_help_string =
    fmt::format("Compression level for storing properties. Allowed values: {}.",
                memgraph::utils::GetAllowedEnumValuesString(compression_level_mappings));

bool ValidStoragePropertyStoreCompressionLevel(std::string_view value);
int StoragePropertyStoreCompressionLevelToInt(std::string_view value);

}  // namespace memgraph::flags

namespace memgraph::utils {

struct DataBuffer {
  std::unique_ptr<uint8_t[]> data;
  uint32_t compressed_size = 0;
  uint32_t original_size = 0;

  // Default constructor
  DataBuffer() = default;

  DataBuffer(uint8_t *data, uint32_t compressed_size, uint32_t original_size);

  DataBuffer(std::unique_ptr<uint8_t[]> data, uint32_t compressed_size, uint32_t original_size);

  // Destructor
  ~DataBuffer() = default;

  // Copy constructor
  DataBuffer(const DataBuffer &other) = delete;

  // Copy assignment operator
  DataBuffer &operator=(const DataBuffer &other) = delete;

  // Move constructor
  DataBuffer(DataBuffer &&other) noexcept;

  // Move assignment operator
  DataBuffer &operator=(DataBuffer &&other) noexcept;
};

class Compressor {
 public:
  Compressor() = default;
  virtual ~Compressor() {}

  Compressor(const Compressor &) = default;
  Compressor &operator=(const Compressor &) = default;
  Compressor(Compressor &&) = default;
  Compressor &operator=(Compressor &&) = default;

  virtual DataBuffer Compress(const uint8_t *input, uint32_t original_size) = 0;

  virtual DataBuffer Decompress(const uint8_t *compressed_data, uint32_t compressed_size, uint32_t original_size) = 0;
};

class ZlibCompressor : public Compressor {
 private:
  ZlibCompressor() = default;

  // NOLINTNEXTLINE
  inline static ZlibCompressor *instance_;

 public:
  static ZlibCompressor *GetInstance();

  ~ZlibCompressor() override {
    if (instance_) {
      delete instance_;
      instance_ = nullptr;
    }
  }

  void operator=(const ZlibCompressor &) = delete;

  ZlibCompressor(const ZlibCompressor &) = delete;
  ZlibCompressor(ZlibCompressor &&) = delete;
  ZlibCompressor &operator=(ZlibCompressor &&) = delete;

  DataBuffer Compress(const uint8_t *input, uint32_t original_size) override;

  DataBuffer Decompress(const uint8_t *compressed_data, uint32_t compressed_size, uint32_t original_size) override;
};

}  // namespace memgraph::utils
