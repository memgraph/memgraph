// Copyright 2022 Memgraph Ltd.
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

#include <filesystem>

#include "slk/streams.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/storage_replication/slk.hpp"
#include "utils/cast.hpp"
#include "utils/file.hpp"

namespace memgraph::storage::replication {

class Encoder final : public durability::BaseEncoder {
 public:
  explicit Encoder(slk::Builder *builder) : builder_(builder) {}

  void WriteMarker(durability::Marker marker) override;

  void WriteBool(bool value) override;

  void WriteUint(uint64_t value) override;

  void WriteDouble(double value) override;

  void WriteString(std::string_view value) override;

  void WritePropertyValue(const PropertyValue &value) override;

  void WriteBuffer(const uint8_t *buffer, size_t buffer_size);

  void WriteFileData(utils::InputFile *file);

  void WriteFile(const std::filesystem::path &path);

 private:
  slk::Builder *builder_;
};

class Decoder final : public durability::BaseDecoder {
 public:
  explicit Decoder(slk::Reader *reader) : reader_(reader) {}

  std::optional<durability::Marker> ReadMarker() override;

  std::optional<bool> ReadBool() override;

  std::optional<uint64_t> ReadUint() override;

  std::optional<double> ReadDouble() override;

  std::optional<std::string> ReadString() override;

  std::optional<PropertyValue> ReadPropertyValue() override;

  bool SkipString() override;

  bool SkipPropertyValue() override;

  /// Read the file and save it inside the specified directory.
  /// @param directory Directory which will contain the read file.
  /// @param suffix Suffix to be added to the received file's filename.
  /// @return If the read was successful, path to the read file.
  std::optional<std::filesystem::path> ReadFile(const std::filesystem::path &directory, const std::string &suffix = "");

 private:
  slk::Reader *reader_;
};

}  // namespace memgraph::storage::replication
