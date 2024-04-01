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

#include "storage/v2/durability/serialization.hpp"

#include "storage/v2/temporal.hpp"
#include "utils/endian.hpp"

namespace memgraph::storage::durability {

//////////////////////////
// Encoder implementation.
//////////////////////////

namespace {
void WriteSize(Encoder *encoder, uint64_t size) {
  size = utils::HostToLittleEndian(size);
  encoder->Write(reinterpret_cast<const uint8_t *>(&size), sizeof(size));
}
}  // namespace

void Encoder::Initialize(const std::filesystem::path &path, const std::string_view magic, uint64_t version) {
  file_.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING);
  Write(reinterpret_cast<const uint8_t *>(magic.data()), magic.size());
  auto version_encoded = utils::HostToLittleEndian(version);
  Write(reinterpret_cast<const uint8_t *>(&version_encoded), sizeof(version_encoded));
}

void Encoder::OpenExisting(const std::filesystem::path &path) {
  file_.Open(path, utils::OutputFile::Mode::APPEND_TO_EXISTING);
}

void Encoder::Close() {
  if (file_.IsOpen()) {
    file_.Close();
  }
}

void Encoder::Write(const uint8_t *data, uint64_t size) { file_.Write(data, size); }

void Encoder::WriteMarker(Marker marker) {
  auto value = static_cast<uint8_t>(marker);
  Write(&value, sizeof(value));
}

void Encoder::WriteBool(bool value) {
  WriteMarker(Marker::TYPE_BOOL);
  if (value) {
    WriteMarker(Marker::VALUE_TRUE);
  } else {
    WriteMarker(Marker::VALUE_FALSE);
  }
}

void Encoder::WriteUint(uint64_t value) {
  value = utils::HostToLittleEndian(value);
  WriteMarker(Marker::TYPE_INT);
  Write(reinterpret_cast<const uint8_t *>(&value), sizeof(value));
}

void Encoder::WriteDouble(double value) {
  auto value_uint = utils::MemcpyCast<uint64_t>(value);
  value_uint = utils::HostToLittleEndian(value_uint);
  WriteMarker(Marker::TYPE_DOUBLE);
  Write(reinterpret_cast<const uint8_t *>(&value_uint), sizeof(value_uint));
}

void Encoder::WriteString(const std::string_view value) {
  WriteMarker(Marker::TYPE_STRING);
  WriteSize(this, value.size());
  Write(reinterpret_cast<const uint8_t *>(value.data()), value.size());
}

void Encoder::WritePropertyValue(const PropertyValue &value) {
  WriteMarker(Marker::TYPE_PROPERTY_VALUE);
  switch (value.type()) {
    case PropertyValue::Type::Null: {
      WriteMarker(Marker::TYPE_NULL);
      break;
    }
    case PropertyValue::Type::Bool: {
      WriteBool(value.ValueBool());
      break;
    }
    case PropertyValue::Type::Int: {
      WriteUint(utils::MemcpyCast<uint64_t>(value.ValueInt()));
      break;
    }
    case PropertyValue::Type::Double: {
      WriteDouble(value.ValueDouble());
      break;
    }
    case PropertyValue::Type::String: {
      WriteString(value.ValueString());
      break;
    }
    case PropertyValue::Type::List: {
      const auto &list = value.ValueList();
      WriteMarker(Marker::TYPE_LIST);
      WriteSize(this, list.size());
      for (const auto &item : list) {
        WritePropertyValue(item);
      }
      break;
    }
    case PropertyValue::Type::Map: {
      const auto &map = value.ValueMap();
      WriteMarker(Marker::TYPE_MAP);
      WriteSize(this, map.size());
      for (const auto &item : map) {
        WriteString(item.first);
        WritePropertyValue(item.second);
      }
      break;
    }
    case PropertyValue::Type::TemporalData: {
      const auto temporal_data = value.ValueTemporalData();
      WriteMarker(Marker::TYPE_TEMPORAL_DATA);
      WriteUint(static_cast<uint64_t>(temporal_data.type));
      WriteUint(utils::MemcpyCast<uint64_t>(temporal_data.microseconds));
      break;
    }
  }
}

uint64_t Encoder::GetPosition() { return file_.GetPosition(); }

void Encoder::SetPosition(uint64_t position) { file_.SetPosition(utils::OutputFile::Position::SET, position); }

void Encoder::Sync() { file_.Sync(); }

void Encoder::Finalize() {
  file_.Sync();
  file_.Close();
}

void Encoder::DisableFlushing() { file_.DisableFlushing(); }

void Encoder::EnableFlushing() { file_.EnableFlushing(); }

void Encoder::TryFlushing() { file_.TryFlushing(); }

std::pair<const uint8_t *, size_t> Encoder::CurrentFileBuffer() const { return file_.CurrentBuffer(); }

size_t Encoder::GetSize() { return file_.GetSize(); }

//////////////////////////
// Decoder implementation.
//////////////////////////

namespace {
std::optional<Marker> CastToMarker(uint8_t value) {
  for (auto marker : kMarkersAll) {
    if (static_cast<uint8_t>(marker) == value) {
      return marker;
    }
  }
  return std::nullopt;
}

std::optional<uint64_t> ReadSize(Decoder *decoder) {
  uint64_t size;
  if (!decoder->Read(reinterpret_cast<uint8_t *>(&size), sizeof(size))) return std::nullopt;
  size = utils::LittleEndianToHost(size);
  return size;
}
}  // namespace

std::optional<uint64_t> Decoder::Initialize(const std::filesystem::path &path, const std::string &magic) {
  if (!file_.Open(path)) return std::nullopt;
  std::string file_magic(magic.size(), '\0');
  if (!Read(reinterpret_cast<uint8_t *>(file_magic.data()), file_magic.size())) return std::nullopt;
  if (file_magic != magic) return std::nullopt;
  uint64_t version_encoded;
  if (!Read(reinterpret_cast<uint8_t *>(&version_encoded), sizeof(version_encoded))) return std::nullopt;
  return utils::LittleEndianToHost(version_encoded);
}

bool Decoder::Read(uint8_t *data, size_t size) { return file_.Read(data, size); }

bool Decoder::Peek(uint8_t *data, size_t size) { return file_.Peek(data, size); }

std::optional<Marker> Decoder::PeekMarker() {
  uint8_t value;
  if (!Peek(&value, sizeof(value))) return std::nullopt;
  auto marker = CastToMarker(value);
  if (!marker) return std::nullopt;
  return *marker;
}

std::optional<Marker> Decoder::ReadMarker() {
  uint8_t value;
  if (!Read(&value, sizeof(value))) return std::nullopt;
  auto marker = CastToMarker(value);
  if (!marker) return std::nullopt;
  return *marker;
}

std::optional<bool> Decoder::ReadBool() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_BOOL) return std::nullopt;
  auto value = ReadMarker();
  if (!value || (*value != Marker::VALUE_FALSE && *value != Marker::VALUE_TRUE)) return std::nullopt;
  return *value == Marker::VALUE_TRUE;
}

std::optional<uint64_t> Decoder::ReadUint() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_INT) return std::nullopt;
  uint64_t value;
  if (!Read(reinterpret_cast<uint8_t *>(&value), sizeof(value))) return std::nullopt;
  value = utils::LittleEndianToHost(value);
  return value;
}

std::optional<double> Decoder::ReadDouble() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_DOUBLE) return std::nullopt;
  uint64_t value_int;
  if (!Read(reinterpret_cast<uint8_t *>(&value_int), sizeof(value_int))) return std::nullopt;
  value_int = utils::LittleEndianToHost(value_int);
  auto value = utils::MemcpyCast<double>(value_int);
  return value;
}

std::optional<std::string> Decoder::ReadString() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_STRING) return std::nullopt;
  auto size = ReadSize(this);
  if (!size) return std::nullopt;
  std::string value(*size, '\0');
  if (!Read(reinterpret_cast<uint8_t *>(value.data()), *size)) return std::nullopt;
  return value;
}

namespace {
std::optional<TemporalData> ReadTemporalData(Decoder &decoder) {
  const auto inner_marker = decoder.ReadMarker();
  if (!inner_marker || *inner_marker != Marker::TYPE_TEMPORAL_DATA) return std::nullopt;

  const auto type = decoder.ReadUint();
  if (!type) return std::nullopt;

  const auto microseconds = decoder.ReadUint();
  if (!microseconds) return std::nullopt;

  return TemporalData{static_cast<TemporalType>(*type), utils::MemcpyCast<int64_t>(*microseconds)};
}
}  // namespace

std::optional<PropertyValue> Decoder::ReadPropertyValue() {
  auto pv_marker = ReadMarker();
  if (!pv_marker || *pv_marker != Marker::TYPE_PROPERTY_VALUE) return std::nullopt;

  auto marker = PeekMarker();
  if (!marker) return std::nullopt;
  switch (*marker) {
    case Marker::TYPE_NULL: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_NULL) return std::nullopt;
      return PropertyValue();
    }
    case Marker::TYPE_BOOL: {
      auto value = ReadBool();
      if (!value) return std::nullopt;
      return PropertyValue(*value);
    }
    case Marker::TYPE_INT: {
      auto value = ReadUint();
      if (!value) return std::nullopt;
      return PropertyValue(utils::MemcpyCast<int64_t>(*value));
    }
    case Marker::TYPE_DOUBLE: {
      auto value = ReadDouble();
      if (!value) return std::nullopt;
      return PropertyValue(*value);
    }
    case Marker::TYPE_STRING: {
      auto value = ReadString();
      if (!value) return std::nullopt;
      return PropertyValue(std::move(*value));
    }
    case Marker::TYPE_LIST: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_LIST) return std::nullopt;
      auto size = ReadSize(this);
      if (!size) return std::nullopt;
      std::vector<PropertyValue> value;
      value.reserve(*size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto item = ReadPropertyValue();
        if (!item) return std::nullopt;
        value.emplace_back(std::move(*item));
      }
      return PropertyValue(std::move(value));
    }
    case Marker::TYPE_MAP: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_MAP) return std::nullopt;
      auto size = ReadSize(this);
      if (!size) return std::nullopt;
      std::map<std::string, PropertyValue> value;
      for (uint64_t i = 0; i < *size; ++i) {
        auto key = ReadString();
        if (!key) return std::nullopt;
        auto item = ReadPropertyValue();
        if (!item) return std::nullopt;
        value.emplace(std::move(*key), std::move(*item));
      }
      return PropertyValue(std::move(value));
    }
    case Marker::TYPE_TEMPORAL_DATA: {
      const auto maybe_temporal_data = ReadTemporalData(*this);
      if (!maybe_temporal_data) return std::nullopt;
      return PropertyValue(*maybe_temporal_data);
    }

    case Marker::TYPE_PROPERTY_VALUE:
    case Marker::SECTION_VERTEX:
    case Marker::SECTION_EDGE:
    case Marker::SECTION_MAPPER:
    case Marker::SECTION_METADATA:
    case Marker::SECTION_INDICES:
    case Marker::SECTION_CONSTRAINTS:
    case Marker::SECTION_DELTA:
    case Marker::SECTION_EPOCH_HISTORY:
    case Marker::SECTION_EDGE_INDICES:
    case Marker::SECTION_OFFSETS:
    case Marker::DELTA_VERTEX_CREATE:
    case Marker::DELTA_VERTEX_DELETE:
    case Marker::DELTA_VERTEX_ADD_LABEL:
    case Marker::DELTA_VERTEX_REMOVE_LABEL:
    case Marker::DELTA_VERTEX_SET_PROPERTY:
    case Marker::DELTA_EDGE_CREATE:
    case Marker::DELTA_EDGE_DELETE:
    case Marker::DELTA_EDGE_SET_PROPERTY:
    case Marker::DELTA_TRANSACTION_END:
    case Marker::DELTA_LABEL_INDEX_CREATE:
    case Marker::DELTA_LABEL_INDEX_DROP:
    case Marker::DELTA_LABEL_INDEX_STATS_SET:
    case Marker::DELTA_LABEL_INDEX_STATS_CLEAR:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_STATS_SET:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_STATS_CLEAR:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_CREATE:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_DROP:
    case Marker::DELTA_EDGE_TYPE_INDEX_CREATE:
    case Marker::DELTA_EDGE_TYPE_INDEX_DROP:
    case Marker::DELTA_TEXT_INDEX_CREATE:
    case Marker::DELTA_TEXT_INDEX_DROP:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_DROP:
    case Marker::DELTA_UNIQUE_CONSTRAINT_CREATE:
    case Marker::DELTA_UNIQUE_CONSTRAINT_DROP:
    case Marker::VALUE_FALSE:
    case Marker::VALUE_TRUE:
      return std::nullopt;
  }
  return std::nullopt;
}

bool Decoder::SkipString() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_STRING) return false;
  auto maybe_size = ReadSize(this);
  if (!maybe_size) return false;

  const uint64_t kBufferSize = 262144;
  uint8_t buffer[kBufferSize];
  uint64_t size = *maybe_size;
  while (size > 0) {
    uint64_t to_read = size < kBufferSize ? size : kBufferSize;
    if (!Read(reinterpret_cast<uint8_t *>(&buffer), to_read)) return false;
    size -= to_read;
  }

  return true;
}

bool Decoder::SkipPropertyValue() {
  auto pv_marker = ReadMarker();
  if (!pv_marker || *pv_marker != Marker::TYPE_PROPERTY_VALUE) return false;

  auto marker = PeekMarker();
  if (!marker) return false;
  switch (*marker) {
    case Marker::TYPE_NULL: {
      auto inner_marker = ReadMarker();
      return inner_marker && *inner_marker == Marker::TYPE_NULL;
    }
    case Marker::TYPE_BOOL: {
      return !!ReadBool();
    }
    case Marker::TYPE_INT: {
      return !!ReadUint();
    }
    case Marker::TYPE_DOUBLE: {
      return !!ReadDouble();
    }
    case Marker::TYPE_STRING: {
      return SkipString();
    }
    case Marker::TYPE_LIST: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_LIST) return false;
      auto size = ReadSize(this);
      if (!size) return false;
      for (uint64_t i = 0; i < *size; ++i) {
        if (!SkipPropertyValue()) return false;
      }
      return true;
    }
    case Marker::TYPE_MAP: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_MAP) return false;
      auto size = ReadSize(this);
      if (!size) return false;
      for (uint64_t i = 0; i < *size; ++i) {
        if (!SkipString()) return false;
        if (!SkipPropertyValue()) return false;
      }
      return true;
    }
    case Marker::TYPE_TEMPORAL_DATA: {
      return !!ReadTemporalData(*this);
    }

    case Marker::TYPE_PROPERTY_VALUE:
    case Marker::SECTION_VERTEX:
    case Marker::SECTION_EDGE:
    case Marker::SECTION_MAPPER:
    case Marker::SECTION_METADATA:
    case Marker::SECTION_INDICES:
    case Marker::SECTION_CONSTRAINTS:
    case Marker::SECTION_DELTA:
    case Marker::SECTION_EPOCH_HISTORY:
    case Marker::SECTION_EDGE_INDICES:
    case Marker::SECTION_OFFSETS:
    case Marker::DELTA_VERTEX_CREATE:
    case Marker::DELTA_VERTEX_DELETE:
    case Marker::DELTA_VERTEX_ADD_LABEL:
    case Marker::DELTA_VERTEX_REMOVE_LABEL:
    case Marker::DELTA_VERTEX_SET_PROPERTY:
    case Marker::DELTA_EDGE_CREATE:
    case Marker::DELTA_EDGE_DELETE:
    case Marker::DELTA_EDGE_SET_PROPERTY:
    case Marker::DELTA_TRANSACTION_END:
    case Marker::DELTA_LABEL_INDEX_CREATE:
    case Marker::DELTA_LABEL_INDEX_DROP:
    case Marker::DELTA_LABEL_INDEX_STATS_SET:
    case Marker::DELTA_LABEL_INDEX_STATS_CLEAR:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_STATS_SET:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_STATS_CLEAR:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_CREATE:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_DROP:
    case Marker::DELTA_EDGE_TYPE_INDEX_CREATE:
    case Marker::DELTA_EDGE_TYPE_INDEX_DROP:
    case Marker::DELTA_TEXT_INDEX_CREATE:
    case Marker::DELTA_TEXT_INDEX_DROP:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_DROP:
    case Marker::DELTA_UNIQUE_CONSTRAINT_CREATE:
    case Marker::DELTA_UNIQUE_CONSTRAINT_DROP:
    case Marker::VALUE_FALSE:
    case Marker::VALUE_TRUE:
      return false;
  }
  return false;
}

std::optional<uint64_t> Decoder::GetSize() { return file_.GetSize(); }

std::optional<uint64_t> Decoder::GetPosition() { return file_.GetPosition(); }

bool Decoder::SetPosition(uint64_t position) { return !!file_.SetPosition(utils::InputFile::Position::SET, position); }

}  // namespace memgraph::storage::durability
