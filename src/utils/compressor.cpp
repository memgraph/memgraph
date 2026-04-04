// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gflags/gflags.h>
#include <zconf.h>
#include <zlib.h>
#include <expected>
#include <iostream>
#include <limits>
#include <memory>

#include "utils/compressor.hpp"
#include "utils/flag_validation.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, misc-unused-parameters)
DEFINE_VALIDATED_string(storage_property_store_compression_level, "mid",
                        memgraph::flags::storage_property_store_compression_level_help_string.c_str(),
                        { return memgraph::flags::ValidStoragePropertyStoreCompressionLevel(value); });

namespace memgraph::flags {

bool ValidStoragePropertyStoreCompressionLevel(std::string_view value) {
  if (const auto result = memgraph::utils::IsValidEnumValueString(value, compression_level_mappings);
      !result.has_value()) {
    const auto error = result.error();
    switch (error) {
      case memgraph::utils::ValidationError::EmptyValue: {
        std::cout << "Compression level cannot be empty." << '\n';
        break;
      }
      case memgraph::utils::ValidationError::InvalidValue: {
        std::cout << "Invalid value for compression level. Allowed values: "
                  << memgraph::utils::GetAllowedEnumValuesString(compression_level_mappings) << '\n';
        break;
      }
    }
    return false;
  }

  return true;
}

utils::CompressionLevel ParseCompressionLevel() {
  return memgraph::utils::StringToEnum<utils::CompressionLevel>(FLAGS_storage_property_store_compression_level,
                                                                compression_level_mappings)
      .value();
}

}  // namespace memgraph::flags

namespace memgraph::utils {

int CompressionLevelToZlibCompressionLevel(CompressionLevel level) {
  switch (level) {
    case CompressionLevel::LOW:
      return Z_BEST_SPEED;
    case CompressionLevel::MID:
      return Z_DEFAULT_COMPRESSION;
    case CompressionLevel::HIGH:
      return Z_BEST_COMPRESSION;
  }
  return Z_DEFAULT_COMPRESSION;
}

std::string_view CompressionLevelToString(CompressionLevel level) {
  switch (level) {
    case CompressionLevel::LOW:
      return "low";
    case CompressionLevel::MID:
      return "mid";
    case CompressionLevel::HIGH:
      return "high";
  }
  return "mid";
}

auto ZlibCompressor::Compress(std::span<uint8_t const> uncompressed_data) const -> std::optional<CompressedBuffer> {
  if (uncompressed_data.empty()) {
    return CompressedBuffer{nullptr, 0, 0};
  }

  // TODO why uint32_t limit here? (why does compression/decompression care...why not size_t)
  if (std::numeric_limits<uint32_t>::max() < uncompressed_data.size_bytes()) {
    return std::nullopt;
  }
  auto original_size = static_cast<uint32_t>(uncompressed_data.size_bytes());

  // this is an estimate on what size we expect compression to be
  auto const compress_bound = compressBound(original_size);
  auto const buffer_size = static_cast<uint32_t>(compress_bound);

  // Use DB-aware allocator and wrap in RAII struct immediately.
  // If we return early, result's destructor handles the cleanup.
  memgraph::memory::DbAwareAllocator<uint8_t> alloc;
  CompressedBuffer result(alloc.allocate(buffer_size), buffer_size, original_size);

  auto compression_level =
      CompressionLevelToZlibCompressionLevel(static_cast<CompressionLevel>(memgraph::flags::ParseCompressionLevel()));

  auto actual_size = compress_bound;
  const int z_res =
      compress2(result.view().data(), &actual_size, uncompressed_data.data(), original_size, compression_level);

  if (z_res != Z_OK) {
    return std::nullopt;
  }

  if (actual_size == compress_bound) {
    return result;
  }

  // If actual size is smaller, we create a new buffer of the exact size and move the data.
  // Note: result.release() ensures the old large buffer is now managed by us or freed.
  auto new_buffer_size = static_cast<uint32_t>(actual_size);
  CompressedBuffer final_buffer(alloc.allocate(new_buffer_size), new_buffer_size, original_size);
  std::copy_n(result.view().data(), new_buffer_size, final_buffer.view().data());

  return final_buffer;
}

auto ZlibCompressor::Decompress(std::span<uint8_t const> compressed_data, uint32_t original_size) const
    -> std::optional<DecompressedBuffer> {
  if (compressed_data.empty() || original_size == 0) {
    return DecompressedBuffer{nullptr, 0};
  }

  memgraph::memory::DbAwareAllocator<uint8_t> alloc;
  DecompressedBuffer result(alloc.allocate(original_size), original_size);

  // needed correct type to avoid UB in `uncompress` call
  uLongf original_size_tmp = original_size;
  auto const z_res =
      uncompress(result.view().data(), &original_size_tmp, compressed_data.data(), compressed_data.size_bytes());

  if (z_res != Z_OK) return std::nullopt;

  return result;
}

auto Compressor::GetInstance() -> Compressor const * {
  static std::unique_ptr<Compressor> const instance = std::make_unique<ZlibCompressor>();
  return instance.get();
}

}  // namespace memgraph::utils
