// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/replication/serialization.hpp"

namespace memgraph::storage::replication {
////// Encoder //////
void Encoder::WriteMarker(durability::Marker marker) { slk::Save(marker, builder_); }

void Encoder::WriteBool(bool value) {
  WriteMarker(durability::Marker::TYPE_BOOL);
  slk::Save(value, builder_);
}

void Encoder::WriteUint(uint64_t value) {
  spdlog::warn("Pos before saving uint: {}", builder_->GetPos());
  WriteMarker(durability::Marker::TYPE_INT);
  slk::Save(value, builder_);
  spdlog::warn("Pos after saving uint: {}", builder_->GetPos());
}

void Encoder::WriteDouble(double value) {
  WriteMarker(durability::Marker::TYPE_DOUBLE);
  slk::Save(value, builder_);
}

void Encoder::WriteString(const std::string_view value) {
  spdlog::warn("Pos before saving string: {}", builder_->GetPos());
  WriteMarker(durability::Marker::TYPE_STRING);
  slk::Save(value, builder_);
  spdlog::warn("Pos after saving string: {}", builder_->GetPos());
}

void Encoder::WriteEnum(storage::Enum value) {
  WriteMarker(durability::Marker::TYPE_ENUM);
  slk::Save(value, builder_);
}

void Encoder::WritePoint2d(storage::Point2d value) {
  WriteMarker(durability::Marker::TYPE_POINT_2D);
  slk::Save(value, builder_);
}

void Encoder::WritePoint3d(storage::Point3d value) {
  WriteMarker(durability::Marker::TYPE_POINT_3D);
  slk::Save(value, builder_);
}

void Encoder::WriteExternalPropertyValue(const ExternalPropertyValue &value) {
  WriteMarker(durability::Marker::TYPE_PROPERTY_VALUE);
  slk::Save(value, builder_);
}

void Encoder::WriteFileBuffer(const uint8_t *buffer, const size_t buffer_size) {
  // builder_->Save(buffer, buffer_size);
  builder_->SaveFileBuffer(buffer, buffer_size);
}

void Encoder::WriteFileData(utils::InputFile *file) {
  auto file_size = file->GetSize();
  uint8_t buffer[utils::kFileBufferSize];
  while (file_size > 0) {
    const auto chunk_size = std::min(file_size, utils::kFileBufferSize);
    file->Read(buffer, chunk_size);
    WriteFileBuffer(buffer, chunk_size);
    file_size -= chunk_size;
  }
}

bool Encoder::WriteFile(const std::filesystem::path &path) {
  builder_->PrepareForFileSending();
  utils::InputFile file;
  if (!file.Open(path)) {
    spdlog::error("Failed to open file {}.", path);
    return false;
  }
  if (!path.has_filename()) {
    spdlog::error("Path {} does not have a filename.", path);
    return false;
  }
  const auto &filename = path.filename().generic_string();
  // spdlog::trace("Position before saving filename: {}", builder_->GetPos());
  WriteString(filename);
  // spdlog::trace("Position after saving filename: {}", builder_->GetPos());
  auto const file_size = file.GetSize();
  spdlog::trace("File size is: {}", file_size);
  WriteUint(file_size);
  // spdlog::trace("Position after saving file size: {}", builder_->GetPos());
  WriteFileData(&file);
  // spdlog::trace("Position after saving file data: {}", builder_->GetPos());
  return true;
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
  // spdlog::warn("Reader position before reading string: {}", reader_->GetPos());
  const auto marker = ReadMarker();
  if (!marker) {
    spdlog::warn("Marker is invalid");
    return std::nullopt;
  }

  if (marker != durability::Marker::TYPE_STRING) {
    spdlog::warn("Marker not string ,type: {}", static_cast<uint8_t>(marker.value()));
    return std::nullopt;
  }

  std::string value;
  slk::Load(&value, reader_);
  spdlog::warn("Reader position after reading string: {}", reader_->GetPos());
  return std::move(value);
}

std::optional<Enum> Decoder::ReadEnumValue() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_ENUM) return std::nullopt;
  storage::Enum value;
  slk::Load(&value, reader_);
  return value;
}

std::optional<Point2d> Decoder::ReadPoint2dValue() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_POINT_2D) return std::nullopt;
  storage::Point2d value;
  slk::Load(&value, reader_);
  return value;
}

std::optional<Point3d> Decoder::ReadPoint3dValue() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_POINT_3D) return std::nullopt;
  storage::Point3d value;
  slk::Load(&value, reader_);
  return value;
}

std::optional<ExternalPropertyValue> Decoder::ReadExternalPropertyValue() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_PROPERTY_VALUE)
    return std::nullopt;
  ExternalPropertyValue value;
  slk::Load(&value, reader_);
  return std::move(value);
}

bool Decoder::SkipString() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_STRING) return false;
  std::string value;
  slk::Load(&value, reader_);
  return true;
}

bool Decoder::SkipExternalPropertyValue() {
  if (const auto marker = ReadMarker(); !marker || marker != durability::Marker::TYPE_PROPERTY_VALUE) return false;
  ExternalPropertyValue value;
  slk::Load(&value, reader_);
  return true;
}

}  // namespace memgraph::storage::replication
