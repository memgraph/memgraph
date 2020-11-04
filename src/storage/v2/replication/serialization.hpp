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

  void WriteMarker(durability::Marker marker) override {
    slk::Save(marker, builder_);
  }

  void WriteBool(bool value) override {
    WriteMarker(durability::Marker::TYPE_BOOL);
    slk::Save(value, builder_);
  }

  void WriteUint(uint64_t value) override {
    WriteMarker(durability::Marker::TYPE_INT);
    slk::Save(value, builder_);
  }

  void WriteDouble(double value) override {
    WriteMarker(durability::Marker::TYPE_DOUBLE);
    slk::Save(value, builder_);
  }

  void WriteString(const std::string_view &value) override {
    WriteMarker(durability::Marker::TYPE_STRING);
    slk::Save(value, builder_);
  }

  void WritePropertyValue(const PropertyValue &value) override {
    WriteMarker(durability::Marker::TYPE_PROPERTY_VALUE);
    slk::Save(value, builder_);
  }

  void WriteFile(const std::filesystem::path &path) {
    utils::InputFile file;
    CHECK(file.Open(path)) << "Failed to open snapshot file!";
    CHECK(path.has_filename()) << "Path does not have a filename!";
    slk::Save(path.filename(), builder_);
    auto file_size = file.GetSize();
    slk::Save(file_size, builder_);
    uint8_t buffer[utils::kFileBufferSize];
    while (file_size > 0) {
      const auto chunk_size = std::min(file_size, utils::kFileBufferSize);
      file.Read(buffer, chunk_size);
      builder_->Save(buffer, chunk_size);
      file_size -= chunk_size;
    }
    file.Close();
  }

 private:
  slk::Builder *builder_;
};

class Decoder final : public durability::BaseDecoder {
 public:
  explicit Decoder(slk::Reader *reader) : reader_(reader) {}

  std::optional<durability::Marker> ReadMarker() override {
    durability::Marker marker;
    slk::Load(&marker, reader_);
    return marker;
  }

  std::optional<bool> ReadBool() override {
    if (const auto marker = ReadMarker();
        !marker || marker != durability::Marker::TYPE_BOOL)
      return std::nullopt;
    bool value;
    slk::Load(&value, reader_);
    return value;
  }

  std::optional<uint64_t> ReadUint() override {
    if (const auto marker = ReadMarker();
        !marker || marker != durability::Marker::TYPE_INT)
      return std::nullopt;
    uint64_t value;
    slk::Load(&value, reader_);
    return value;
  }

  std::optional<double> ReadDouble() override {
    if (const auto marker = ReadMarker();
        !marker || marker != durability::Marker::TYPE_DOUBLE)
      return std::nullopt;
    double value;
    slk::Load(&value, reader_);
    return value;
  }

  std::optional<std::string> ReadString() override {
    if (const auto marker = ReadMarker();
        !marker || marker != durability::Marker::TYPE_STRING)
      return std::nullopt;
    std::string value;
    slk::Load(&value, reader_);
    return std::move(value);
  }

  std::optional<PropertyValue> ReadPropertyValue() override {
    if (const auto marker = ReadMarker();
        !marker || marker != durability::Marker::TYPE_PROPERTY_VALUE)
      return std::nullopt;
    PropertyValue value;
    slk::Load(&value, reader_);
    return std::move(value);
  }

  bool SkipString() override {
    if (const auto marker = ReadMarker();
        !marker || marker != durability::Marker::TYPE_STRING)
      return false;
    std::string value;
    slk::Load(&value, reader_);
    return true;
  }

  bool SkipPropertyValue() override {
    if (const auto marker = ReadMarker();
        !marker || marker != durability::Marker::TYPE_PROPERTY_VALUE)
      return false;
    PropertyValue value;
    slk::Load(&value, reader_);
    return true;
  }

  std::optional<std::filesystem::path> ReadFile(
      const std::filesystem::path &directory) {
    CHECK(std::filesystem::exists(directory) &&
          std::filesystem::is_directory(directory))
        << "Sent path for streamed files should be a valid directory!";
    utils::OutputFile file;
    std::string filename;
    slk::Load(&filename, reader_);
    auto path = directory / filename;
    file.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING);
    size_t file_size;
    slk::Load(&file_size, reader_);
    uint8_t buffer[utils::kFileBufferSize];
    while (file_size > 0) {
      const auto chunk_size = std::min(file_size, utils::kFileBufferSize);
      reader_->Load(buffer, chunk_size);
      file.Write(buffer, chunk_size);
      file_size -= chunk_size;
    }
    file.Close();
    return std::move(path);
  }

 private:
  slk::Reader *reader_;
};

}  // namespace storage::replication
