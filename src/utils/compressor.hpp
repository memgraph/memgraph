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

namespace memgraph::utils {
enum class CompressionLevel : uint8_t { LOW, MID, HIGH };

int CompressionLevelToZlibCompressionLevel(CompressionLevel level);
std::string_view CompressionLevelToString(CompressionLevel level);
}  // namespace memgraph::utils

namespace memgraph::flags {

using namespace std::string_view_literals;

inline constexpr std::array compression_level_mappings{std::pair{"low"sv, utils::CompressionLevel::LOW},
                                                       std::pair{"mid"sv, utils::CompressionLevel::MID},
                                                       std::pair{"high"sv, utils::CompressionLevel::HIGH}};

inline const std::string storage_property_store_compression_level_help_string =
    fmt::format("Compression level for storing properties. Allowed values: {}.",
                memgraph::utils::GetAllowedEnumValuesString(compression_level_mappings));

bool ValidStoragePropertyStoreCompressionLevel(std::string_view value);
utils::CompressionLevel ParseCompressionLevel();
}  // namespace memgraph::flags

namespace memgraph::utils {

struct CompressedBuffer {
  CompressedBuffer(std::unique_ptr<uint8_t[]> data, uint32_t compressed_size, uint32_t original_size)
      : data_(std::move(data)), compressed_size_(compressed_size), original_size_(original_size) {}

  CompressedBuffer() = default;
  CompressedBuffer(CompressedBuffer &&other) noexcept = default;
  CompressedBuffer &operator=(CompressedBuffer &&other) noexcept = default;

  auto original_size() const -> uint32_t { return original_size_; }

  auto view() -> std::span<uint8_t> { return std::span{data_.get(), compressed_size_}; }
  auto view() const -> std::span<uint8_t const> { return std::span{data_.get(), compressed_size_}; }

 private:
  std::unique_ptr<uint8_t[]> data_;
  uint32_t compressed_size_ = 0;
  uint32_t original_size_ = 0;
};

struct DecompressedBuffer {
  DecompressedBuffer(std::unique_ptr<uint8_t[]> data, uint32_t original_size)
      : data_(std::move(data)), original_size_(original_size) {}

  DecompressedBuffer() = default;
  DecompressedBuffer(DecompressedBuffer &&other) noexcept = default;
  DecompressedBuffer &operator=(DecompressedBuffer &&other) noexcept = default;

  auto view() -> std::span<uint8_t> { return std::span{data_.get(), original_size_}; }
  auto view() const -> std::span<uint8_t const> { return std::span{data_.get(), original_size_}; }

  void release() {
    data_.release();
    original_size_ = 0;
  }

 private:
  std::unique_ptr<uint8_t[]> data_;
  uint32_t original_size_ = 0;
};

struct Compressor {
  static auto GetInstance() -> Compressor const *;

  Compressor() = default;
  virtual ~Compressor() = default;

  virtual auto Compress(std::span<uint8_t const> uncompressed_data) const -> std::optional<CompressedBuffer> = 0;

  virtual auto Decompress(std::span<uint8_t const> compressed_data, uint32_t original_size) const
      -> std::optional<DecompressedBuffer> = 0;
};

struct ZlibCompressor : public Compressor {
  ZlibCompressor() = default;

  auto Compress(std::span<uint8_t const> uncompressed_data) const -> std::optional<CompressedBuffer> override;

  auto Decompress(std::span<uint8_t const> compressed_data, uint32_t original_size) const
      -> std::optional<DecompressedBuffer> override;
};

}  // namespace memgraph::utils
