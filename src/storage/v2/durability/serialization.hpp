// Copyright 2023 Memgraph Ltd.
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

#include <cstdint>
#include <filesystem>
#include <string_view>

#include "storage/v2/config.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/file.hpp"

namespace memgraph::storage::durability {

/// Encoder interface class. Used to implement streams to different targets
/// (e.g. file and network).
class BaseEncoder {
 protected:
  ~BaseEncoder() = default;

 public:
  virtual void WriteMarker(Marker marker) = 0;
  virtual void WriteBool(bool value) = 0;
  virtual void WriteUint(uint64_t value) = 0;
  virtual void WriteDouble(double value) = 0;
  virtual void WriteString(std::string_view value) = 0;
  virtual void WritePropertyValue(const PropertyValue &value) = 0;
};

/// Encoder that is used to generate a snapshot/WAL.
class Encoder final : public BaseEncoder {
 public:
  void Initialize(const std::filesystem::path &path, std::string_view magic, uint64_t version);

  void OpenExisting(const std::filesystem::path &path);

  void Close();
  // Main write function, the only one that is allowed to write to the `file_`
  // directly.
  void Write(const uint8_t *data, uint64_t size);

  void WriteMarker(Marker marker) override;
  void WriteBool(bool value) override;
  void WriteUint(uint64_t value) override;
  void WriteDouble(double value) override;
  void WriteString(std::string_view value) override;
  void WritePropertyValue(const PropertyValue &value) override;

  uint64_t GetPosition();
  void SetPosition(uint64_t position);

  void Sync();

  void Finalize();

  // Disable flushing of the internal buffer.
  void DisableFlushing();
  // Enable flushing of the internal buffer.
  void EnableFlushing();
  // Try flushing the internal buffer.
  void TryFlushing();
  // Get the current internal buffer with its size.
  std::pair<const uint8_t *, size_t> CurrentFileBuffer() const;

  // Get the total size of the current file.
  size_t GetSize();

 private:
  utils::OutputFile file_;
};

/// Decoder interface class. Used to implement streams from different sources
/// (e.g. file and network).
class BaseDecoder {
 protected:
  ~BaseDecoder() = default;

 public:
  virtual std::optional<Marker> ReadMarker() = 0;
  virtual std::optional<bool> ReadBool() = 0;
  virtual std::optional<uint64_t> ReadUint() = 0;
  virtual std::optional<double> ReadDouble() = 0;
  virtual std::optional<std::string> ReadString() = 0;
  virtual std::optional<PropertyValue> ReadPropertyValue() = 0;

  virtual bool SkipString() = 0;
  virtual bool SkipPropertyValue() = 0;
};

/// Decoder that is used to read a generated snapshot/WAL.
class Decoder final : public BaseDecoder {
 public:
  std::optional<uint64_t> Initialize(const std::filesystem::path &path, const std::string &magic);

  // Main read functions, the only one that are allowed to read from the `file_`
  // directly.
  bool Read(uint8_t *data, size_t size);
  bool Peek(uint8_t *data, size_t size);

  std::optional<Marker> PeekMarker();

  std::optional<Marker> ReadMarker() override;
  std::optional<bool> ReadBool() override;
  std::optional<uint64_t> ReadUint() override;
  std::optional<double> ReadDouble() override;
  std::optional<std::string> ReadString() override;
  std::optional<PropertyValue> ReadPropertyValue() override;

  bool SkipString() override;
  bool SkipPropertyValue() override;

  std::optional<uint64_t> GetSize();
  std::optional<uint64_t> GetPosition();
  bool SetPosition(uint64_t position);

 private:
  utils::InputFile file_;
};

}  // namespace memgraph::storage::durability
