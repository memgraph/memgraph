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

#include "storage/v2/storage_replication/serialization.hpp"

namespace memgraph::storage::replication {
////// Encoder //////
void Encoder::WriteMarker(durability::Marker marker) { slk::Save(marker, builder_); }

void Encoder::WriteBool(bool value) {
  WriteMarker(durability::Marker::TYPE_BOOL);
  slk::Save(value, builder_);
}

void Encoder::WriteUint(uint64_t value) {
  WriteMarker(durability::Marker::TYPE_INT);
  slk::Save(value, builder_);
}

void Encoder::WriteDouble(double value) {
  WriteMarker(durability::Marker::TYPE_DOUBLE);
  slk::Save(value, builder_);
}

void Encoder::WriteString(const std::string_view value) {
  WriteMarker(durability::Marker::TYPE_STRING);
  slk::Save(value, builder_);
}

void Encoder::WritePropertyValue(const PropertyValue &value) {
  WriteMarker(durability::Marker::TYPE_PROPERTY_VALUE);
  slk::Save(value, builder_);
}

void Encoder::WriteBuffer(const uint8_t *buffer, const size_t buffer_size) { builder_->Save(buffer, buffer_size); }

void Encoder::WriteFileData(utils::InputFile *file) {
  auto file_size = file->GetSize();
  uint8_t buffer[utils::kFileBufferSize];
  while (file_size > 0) {
    const auto chunk_size = std::min(file_size, utils::kFileBufferSize);
    file->Read(buffer, chunk_size);
    WriteBuffer(buffer, chunk_size);
    file_size -= chunk_size;
  }
}

void Encoder::WriteFile(const std::filesystem::path &path) {
  utils::InputFile file;
  MG_ASSERT(file.Open(path), "Failed to open file {}", path);
  MG_ASSERT(path.has_filename(), "Path does not have a filename!");
  const auto &filename = path.filename().generic_string();
  WriteString(filename);
  auto file_size = file.GetSize();
  WriteUint(file_size);
  WriteFileData(&file);
  file.Close();
}

////// Decoder //////
std::optional<durability::Marker> Decoder::ReadMarker() {
  durability::Marker marker;
  slk::Load(&marker, reader_);
  return marker;
}

std::optional<bool> Decoder::ReadBool() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_BOOL) return std::nullopt;
  bool value;
  slk::Load(&value, reader_);
  return value;
}

std::optional<uint64_t> Decoder::ReadUint() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_INT) return std::nullopt;
  uint64_t value;
  slk::Load(&value, reader_);
  return value;
}

std::optional<double> Decoder::ReadDouble() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_DOUBLE) return std::nullopt;
  double value;
  slk::Load(&value, reader_);
  return value;
}

std::optional<std::string> Decoder::ReadString() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_STRING) return std::nullopt;
  std::string value;
  slk::Load(&value, reader_);
  return std::move(value);
}

std::optional<PropertyValue> Decoder::ReadPropertyValue() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_PROPERTY_VALUE)
    return std::nullopt;
  PropertyValue value;
  slk::Load(&value, reader_);
  return std::move(value);
}

bool Decoder::SkipString() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_STRING) return false;
  std::string value;
  slk::Load(&value, reader_);
  return true;
}

bool Decoder::SkipPropertyValue() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_PROPERTY_VALUE) return false;
  PropertyValue value;
  slk::Load(&value, reader_);
  return true;
}

std::optional<std::filesystem::path> Decoder::ReadFile(const std::filesystem::path &directory,
                                                       const std::string &suffix) {
  MG_ASSERT(std::filesystem::exists(directory) && std::filesystem::is_directory(directory),
            "Sent path for streamed files should be a valid directory!");
  utils::OutputFile file;
  const auto maybe_filename = ReadString();
  MG_ASSERT(maybe_filename, "Filename missing for the file");
  const auto filename = *maybe_filename + suffix;
  auto path = directory / filename;

  file.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING);
  std::optional<size_t> maybe_file_size = ReadUint();
  MG_ASSERT(maybe_file_size, "File size missing");
  auto file_size = *maybe_file_size;
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
}  // namespace memgraph::storage::replication
