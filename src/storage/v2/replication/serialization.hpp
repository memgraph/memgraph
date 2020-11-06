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

  void WriteBuffer(const uint8_t *buffer, const size_t buffer_size) {
    builder_->Save(buffer, buffer_size);
  }

  void WriteFileData(utils::InputFile *file) {
    auto file_size = file->GetSize();
    uint8_t buffer[utils::kFileBufferSize];
    while (file_size > 0) {
      const auto chunk_size = std::min(file_size, utils::kFileBufferSize);
      file->Read(buffer, chunk_size);
      WriteBuffer(buffer, chunk_size);
      file_size -= chunk_size;
    }
  }

  void WriteFile(const std::filesystem::path &path) {
    utils::InputFile file;
    CHECK(file.Open(path)) << "Failed to open file " << path;
    CHECK(path.has_filename()) << "Path does not have a filename!";
    const auto &filename = path.filename().generic_string();
    WriteString(filename);
    auto file_size = file.GetSize();
    WriteUint(file_size);
    WriteFileData(&file);
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
    const auto maybe_filename = ReadString();
    CHECK(maybe_filename) << "Filename missing for the file";
    const auto &filename = *maybe_filename;
    auto path = directory / filename;

    // Check if the file already exists so we don't overwrite it
    const bool file_exists = std::filesystem::exists(path);

    // TODO (antonio2368): Maybe append filename with custom suffix so we have
    // both copies?
    if (!file_exists) {
      file.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING);
    }
    std::optional<size_t> maybe_file_size = ReadUint();
    CHECK(maybe_file_size) << "File size missing";
    auto file_size = *maybe_file_size;
    uint8_t buffer[utils::kFileBufferSize];
    while (file_size > 0) {
      const auto chunk_size = std::min(file_size, utils::kFileBufferSize);
      reader_->Load(buffer, chunk_size);
      if (!file_exists) {
        file.Write(buffer, chunk_size);
      }
      file_size -= chunk_size;
    }
    file.Close();
    return std::move(path);
  }

 private:
  slk::Reader *reader_;
};

}  // namespace storage::replication
