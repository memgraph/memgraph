#pragma once

#include <filesystem>

#include "slk/streams.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/replication/slk.hpp"
#include "utils/cast.hpp"
#include "utils/file.hpp"

namespace storage::replication {

class Encoder final : public durability::BaseEncoder {
 public:
  explicit Encoder(slk::Builder *builder) : builder_(builder) {}

  void WriteMarker(durability::Marker marker) override;

  void WriteBool(bool value) override;

  void WriteUint(uint64_t value) override;

  void WriteDouble(double value) override;

  void WriteString(const std::string_view &value) override;

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

  // Decode the file. If successfully read, return the path
  // to the file.
  std::optional<std::filesystem::path> ReadFile(
      const std::filesystem::path &directory);

 private:
  slk::Reader *reader_;
};

}  // namespace storage::replication
