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

#include <zlib.h>
#include <cstdint>
#include <cstring>
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
      result.HasError()) {
    const auto error = result.GetError();
    switch (error) {
      case memgraph::utils::ValidationError::EmptyValue: {
        std::cout << "Compression level cannot be empty." << std::endl;
        break;
      }
      case memgraph::utils::ValidationError::InvalidValue: {
        std::cout << "Invalid value for compression level. Allowed values: "
                  << memgraph::utils::GetAllowedEnumValuesString(compression_level_mappings) << std::endl;
        break;
      }
    }
    return false;
  }

  return true;
}

int StoragePropertyStoreCompressionLevelToInt(std::string_view value) {
  return memgraph::utils::StringToEnum<int>(value, compression_level_mappings).value();
}

}  // namespace memgraph::flags

namespace memgraph::utils {

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

  auto compress_bound = compressBound(original_size);
  std::unique_ptr<uint8_t[]> compressed_data(new uint8_t[compress_bound]);
  const auto compressed_bound_copy = compress_bound;

  const int result = compress2(
      compressed_data.get(), &compress_bound, input, original_size,
      memgraph::flags::StoragePropertyStoreCompressionLevelToInt(FLAGS_storage_property_store_compression_level));

  if (result == Z_OK) {
    std::unique_ptr<uint8_t[]> compressed_buffer_data;
    const uint32_t compressed_size = compress_bound;
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

ZlibCompressor *ZlibCompressor::GetInstance() {
  if (instance_ == nullptr) {
    instance_ = new ZlibCompressor();
  }
  return instance_;
}

}  // namespace memgraph::utils
